package main

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"sort"
	"time"

	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/oklog/ulid"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

const numPartitions = 16
const numStoreGateways = 70
const numDaysInThePast = 7
const replicationFactor = 3
const numQueriers = 80

func main() {
	rand.Seed(time.Now().UnixNano())
	sgs, sgsQueried := createStoreGateways(numStoreGateways)
	blocks := createBlocks(numDaysInThePast, numPartitions)
	queriers, querierEntropies := createQueriers(numQueriers)

	// Randomize sgs
	rand.Shuffle(len(sgs), func(i, j int) {
		sgs[i], sgs[j] = sgs[j], sgs[i]
	})

	ownership := assignBlocks(blocks, sgs)

	now := time.Now()
	blocksToQuery := getBlocksForTimeRange(blocks, now.Add(-90*time.Hour), now.Add(-89*time.Hour))
	fmt.Printf("Found %v blocks to query\n", len(blocksToQuery))

	for i := 0; i < 1000; i++ {
		// First pick a querier to execute the query
		// Since queriers are load balanced using a queue, let's assume round robin here.
		q := queriers[i%len(queriers)]
		querierEntropy := querierEntropies[q]
		sgsToQuery := getStoreGatewaysForBlocks(querierEntropy, blocksToQuery, ownership)
		for _, sg := range sgsToQuery {
			sgsQueried[sg] += 1
		}
	}

	ownershipCount := getBlocksOwnershipCount(sgs, blocksToQuery, ownership)

	fmt.Printf("Queried store-gateways: %v\n", sgsQueried)
	fmt.Printf("Blocks owned by store-gateways: %v\n", ownershipCount)
	PlotGraph(sgsQueried, "queried.png", fmt.Sprintf("Requests per store-gateway (RF=%d)", replicationFactor))
	PlotGraph(ownershipCount, "blocksOwned.png", fmt.Sprintf("Blocks owned per store-gateway (RF=%d)", replicationFactor))
}

type Pair struct {
	Key   string
	Value float64
}

type Block struct {
	id      ulid.ULID
	minTime int64
	maxTime int64
}

func createStoreGateways(n int) (sgs []string, picked map[string]int) {
	sgs = []string{}
	picked = map[string]int{}

	for i := 0; i < n; i++ {
		sg := fmt.Sprintf("s%d", i)
		sgs = append(sgs, sg)
		picked[sg] = 0
	}
	return sgs, picked
}

func createBlocks(days int, partitions int) (blocks []Block) {
	end := time.Now()
	start := end.Add(-time.Duration(days) * 24 * time.Hour)
	fmt.Printf("Creating blocks ranging: %s, %s\n", start.String(), end.String())

	next := start
	for next.Before(end) {
		blockStart := next
		blockEnd := next.Add(24 * time.Hour).Truncate(24 * time.Hour)
		next = blockEnd
		entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := 0; i < partitions; i++ {
			id := ulid.MustNew(uint64(blockEnd.UnixMilli()), entropy)
			blocks = append(blocks, Block{
				id:      id,
				minTime: blockStart.UnixMilli(),
				maxTime: blockEnd.UnixMilli(),
			})
		}
	}

	return blocks
}

func assignBlocks(blocks []Block, sgs []string) (ownership map[string][]string) {
	ownership = map[string][]string{}

	// Assign blocks to store-gateways
	for _, block := range blocks {
		hash := cortex_tsdb.HashBlockID(block.id)

		sgsForBlock := []string{}
		for i := 0; i < replicationFactor; i++ {
			// This is an oversimplification of the algorithm
			sg := (hash%uint32(len(sgs)) + uint32(i)) % uint32(len(sgs))
			sgsForBlock = append(sgsForBlock, sgs[sg])
		}

		ownership[block.id.String()] = sgsForBlock
	}

	return ownership
}

func getStoreGatewaysForBlocks(entropy *rand.Rand, blocksToQuery []Block, ownership map[string][]string) (sgsForBlocks []string) {
	sgsForBlocks = []string{}
	for _, block := range blocksToQuery {
		owners := ownership[block.id.String()]
		entropy.Shuffle(len(owners), func(i, j int) {
			owners[i], owners[j] = owners[j], owners[i]
		})
		sgsForBlocks = append(sgsForBlocks, owners[0])
	}

	return sgsForBlocks
}

func getBlocksForTimeRange(blocks []Block, start, end time.Time) (blocksToQuery []Block) {
	blocksToQuery = []Block{}
	startMilli := start.UnixMilli()
	endMilli := end.UnixMilli()
	fmt.Printf("Finding blocks for time: %v, %v\n", startMilli, endMilli)
	for _, block := range blocks {
		if startMilli <= block.maxTime && endMilli >= block.minTime {
			blocksToQuery = append(blocksToQuery, block)
		}
	}

	return blocksToQuery
}

func getBlocksOwnershipCount(sgs []string, blocksToQuery []Block, ownership map[string][]string) map[string]int {
	ownershipCount := map[string]int{}

	for _, sg := range sgs {
		ownershipCount[sg] = 0
	}

	for _, block := range blocksToQuery {
		for _, o := range ownership[block.id.String()] {
			ownershipCount[o] += 1
		}
	}

	return ownershipCount
}

func createQueriers(n int) (queriers []string, entropies map[string]*rand.Rand) {
	queriers = []string{}
	entropies = map[string]*rand.Rand{}

	for i := 0; i < n; i++ {
		q := fmt.Sprintf("querier-%d", i)
		queriers = append(queriers, q)
		entropy := rand.New(rand.NewSource(time.Now().UnixNano() + int64(i)))
		entropies[q] = entropy
	}
	return queriers, entropies
}

func PlotGraph(picked map[string]int, file string, title string) {
	p := plot.New()
	values := plotter.Values{}

	keys := []string{}
	pairs := []Pair{}
	for k, v := range picked {
		pairs = append(pairs, Pair{
			Key:   k,
			Value: float64(v),
		})
	}

	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Value > pairs[j].Value
	})
	for _, p := range pairs {
		keys = append(keys, p.Key)
		v := float64(picked[p.Key])
		values = append(values, v)
	}

	w := vg.Points(5)
	bars, err := plotter.NewBarChart(values, w)
	bars.Offset = -w
	if err != nil {
		panic(err)
	}
	p.Add((bars))
	p.Title.Text = title
	p.NominalX(keys...)
	if err := p.Save(20*vg.Inch, 3*vg.Inch, file); err != nil {
		panic(err)
	}

}

func FNV32a(text string) int {
	algorithm := fnv.New32()
	algorithm.Write([]byte(text))
	return int(algorithm.Sum32())
}
