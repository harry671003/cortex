package querier

import (
	"github.com/cortexproject/cortex/pkg/storegateway/typespb"
	"github.com/go-kit/log"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

// Implementation of storage.SeriesSet, based on individual responses from store client.
type blockSeriesSet struct {
	logger   log.Logger
	series   []*typespb.SelectedSeries
	chunks   [][]*typespb.AggrChunk
	warnings annotations.Annotations

	// next response to process
	next int

	currSeries storage.Series
}

func (bqss *blockSeriesSet) Next() bool {
	bqss.currSeries = nil

	if bqss.next >= len(bqss.series) {
		return false
	}

	currLabels := labelpb.ZLabelsToPromLabels(bqss.series[bqss.next].Labels)
	currChunks := bqss.chunks[bqss.next]

	bqss.next++

	// Merge chunks for current series. Chunks may come in multiple responses, but as soon
	// as the response has chunks for a new series, we can stop searching. Series are sorted.
	// See documentation for StoreClient.Series call for details.
	for bqss.next < len(bqss.series) && labels.Compare(currLabels, labelpb.ZLabelsToPromLabels(bqss.series[bqss.next].Labels)) == 0 {
		currChunks = append(currChunks, bqss.chunks[bqss.next]...)
		bqss.next++
	}

	chunks := make([]typespb.AggrChunk, 0, len(currChunks))
	for _, c := range currChunks {
		chunks = append(chunks, *c)
	}

	// level.Info(bqss.logger).Log("msg", "Inside blockSeriesSet.Next",
	// 	"labels", currLabels,
	// 	"chunks", len(currChunks),
	// )
	bqss.currSeries = newBlockQuerierSeries(currLabels, chunks)
	return true
}

func (bqss *blockSeriesSet) At() storage.Series {
	// level.Info(bqss.logger).Log("msg", "Inside blockSeriesSet.At",
	// 	"series", bqss.currSeries.Labels(),
	// )
	return bqss.currSeries
}

func (bqss *blockSeriesSet) Err() error {
	return nil
}

func (bqss *blockSeriesSet) Warnings() annotations.Annotations {
	return bqss.warnings
}
