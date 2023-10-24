package storegateway

import (
	"context"

	"github.com/cortexproject/cortex/pkg/storegateway/storepb"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/runutil"
)

// Loads a batch of chunks
type blockChunkClient struct {
	ctx    context.Context
	logger log.Logger

	chunkr         *bucketChunkReader
	loadAggregates []storepb.Aggr
	bytesLimiter   BytesLimiter

	calculateChunkHash bool
	tenant             string
}

func newBlockChunkClient(
	ctx context.Context,
	logger log.Logger,
	b *bucketBlock,
	limiter ChunksLimiter,
	bytesLimiter BytesLimiter,
	calculateChunkHash bool,
	chunkFetchDuration *prometheus.HistogramVec,
	chunkFetchDurationSum *prometheus.HistogramVec,
) *blockChunkClient {
	chunkr := b.chunkReader()
	return &blockChunkClient{
		ctx:                ctx,
		logger:             logger,
		chunkr:             chunkr,
		bytesLimiter:       bytesLimiter,
		calculateChunkHash: calculateChunkHash,
	}
}

func (b *blockChunkClient) loadChunks(entries []seriesEntry) error {
	b.chunkr.reset()

	for i, s := range entries {
		for j := range s.chks {
			if err := b.chunkr.addLoad(s.refs[j], i, j); err != nil {
				return errors.Wrap(err, "add chunk load")
			}
		}
	}

	if err := b.chunkr.load(b.ctx, entries, b.loadAggregates, b.calculateChunkHash, b.bytesLimiter, b.tenant); err != nil {
		return errors.Wrap(err, "load chunks")
	}

	return nil
}

func (b *blockChunkClient) Close() {
	runutil.CloseWithLogOnErr(b.logger, b.chunkr, "series block")
}
