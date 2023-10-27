// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"strings"

	typespb "github.com/cortexproject/cortex/pkg/storegateway/typespb"
	"github.com/gogo/protobuf/types"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc/codes"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

func NewWarnSeriesResponse(err error) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_Warning{
			Warning: err.Error(),
		},
	}
}

func NewSeriesResponse(series *typespb.Series) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_Series{
			Series: series,
		},
	}
}

func NewHintsSeriesResponse(hints *types.Any) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_Hints{
			Hints: hints,
		},
	}
}

func NewWarnSelectResponse(err error) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_Warning{
			Warning: err.Error(),
		},
	}
}

func NewSelectResponse(series *typespb.SelectedSeries) *SelectResponse {
	return &SelectResponse{
		Result: &SelectResponse_Series{
			Series: series,
		},
	}
}

func NewHintsSelectResponse(hints *types.Any) *SelectResponse {
	return &SelectResponse{
		Result: &SelectResponse_Hints{
			Hints: hints,
		},
	}
}

func GRPCCodeFromWarn(warn string) codes.Code {
	if strings.Contains(warn, "rpc error: code = ResourceExhausted") {
		return codes.ResourceExhausted
	}
	if strings.Contains(warn, "rpc error: code = Code(422)") {
		return 422
	}
	return codes.Unknown
}

type emptySeriesSet struct{}

func (emptySeriesSet) Next() bool                               { return false }
func (emptySeriesSet) At() (labels.Labels, []typespb.AggrChunk) { return nil, nil }
func (emptySeriesSet) Err() error                               { return nil }

// EmptySeriesSet returns a new series set that contains no series.
func EmptySeriesSet() SeriesSet {
	return emptySeriesSet{}
}

// MergeSeriesSets takes all series sets and returns as a union single series set.
// It assumes series are sorted by labels within single SeriesSet, similar to remote read guarantees.
// However, they can be partial: in such case, if the single SeriesSet returns the same series within many iterations,
// MergeSeriesSets will merge those into one.
//
// It also assumes in a "best effort" way that chunks are sorted by min time. It's done as an optimization only, so if input
// series' chunks are NOT sorted, the only consequence is that the duplicates might be not correctly removed. This is double checked
// which on just-before PromQL level as well, so the only consequence is increased network bandwidth.
// If all chunks were sorted, MergeSeriesSet ALSO returns sorted chunks by min time.
//
// Chunks within the same series can also overlap (within all SeriesSet
// as well as single SeriesSet alone). If the chunk ranges overlap, the *exact* chunk duplicates will be removed
// (except one), and any other overlaps will be appended into on chunks slice.
func MergeSeriesSets(all ...SeriesSet) SeriesSet {
	switch len(all) {
	case 0:
		return emptySeriesSet{}
	case 1:
		return newUniqueSeriesSet(all[0])
	}
	h := len(all) / 2

	return newMergedSeriesSet(
		MergeSeriesSets(all[:h]...),
		MergeSeriesSets(all[h:]...),
	)
}

// SeriesSet is a set of series and their corresponding chunks.
// The set is sorted by the label sets. Chunks may be overlapping or expected of order.
type SeriesSet interface {
	Next() bool
	At() (labels.Labels, []typespb.AggrChunk)
	Err() error
}

// mergedSeriesSet takes two series sets as a single series set.
type mergedSeriesSet struct {
	a, b SeriesSet

	lset         labels.Labels
	chunks       []typespb.AggrChunk
	adone, bdone bool
}

func newMergedSeriesSet(a, b SeriesSet) *mergedSeriesSet {
	s := &mergedSeriesSet{a: a, b: b}
	// Initialize first elements of both sets as Next() needs
	// one element look-ahead.
	s.adone = !s.a.Next()
	s.bdone = !s.b.Next()

	return s
}

func (s *mergedSeriesSet) At() (labels.Labels, []typespb.AggrChunk) {
	return s.lset, s.chunks
}

func (s *mergedSeriesSet) Err() error {
	if s.a.Err() != nil {
		return s.a.Err()
	}
	return s.b.Err()
}

func (s *mergedSeriesSet) compare() int {
	if s.adone {
		return 1
	}
	if s.bdone {
		return -1
	}
	lsetA, _ := s.a.At()
	lsetB, _ := s.b.At()
	return labels.Compare(lsetA, lsetB)
}

func (s *mergedSeriesSet) Next() bool {
	if s.adone && s.bdone || s.Err() != nil {
		return false
	}

	d := s.compare()
	if d > 0 {
		s.lset, s.chunks = s.b.At()
		s.bdone = !s.b.Next()
		return true
	}
	if d < 0 {
		s.lset, s.chunks = s.a.At()
		s.adone = !s.a.Next()
		return true
	}

	// Both a and b contains the same series. Go through all chunks, remove duplicates and concatenate chunks from both
	// series sets. We best effortly assume chunks are sorted by min time. If not, we will not detect all deduplicate which will
	// be account on select layer anyway. We do it still for early optimization.
	lset, chksA := s.a.At()
	_, chksB := s.b.At()
	s.lset = lset

	// Slice reuse is not generally safe with nested merge iterators.
	// We err on the safe side an create a new slice.
	s.chunks = make([]typespb.AggrChunk, 0, len(chksA)+len(chksB))

	b := 0
Outer:
	for a := range chksA {
		for {
			if b >= len(chksB) {
				// No more b chunks.
				s.chunks = append(s.chunks, chksA[a:]...)
				break Outer
			}

			cmp := chksA[a].Compare(chksB[b])
			if cmp > 0 {
				s.chunks = append(s.chunks, chksA[a])
				break
			}
			if cmp < 0 {
				s.chunks = append(s.chunks, chksB[b])
				b++
				continue
			}

			// Exact duplicated chunks, discard one from b.
			b++
		}
	}

	if b < len(chksB) {
		s.chunks = append(s.chunks, chksB[b:]...)
	}

	s.adone = !s.a.Next()
	s.bdone = !s.b.Next()
	return true
}

// uniqueSeriesSet takes one series set and ensures each iteration contains single, full series.
type uniqueSeriesSet struct {
	SeriesSet
	done bool

	peek *typespb.Series

	lset   labels.Labels
	chunks []typespb.AggrChunk
}

func newUniqueSeriesSet(wrapped SeriesSet) *uniqueSeriesSet {
	return &uniqueSeriesSet{SeriesSet: wrapped}
}

func (s *uniqueSeriesSet) At() (labels.Labels, []typespb.AggrChunk) {
	return s.lset, s.chunks
}

func (s *uniqueSeriesSet) Next() bool {
	if s.Err() != nil {
		return false
	}

	for !s.done {
		if s.done = !s.SeriesSet.Next(); s.done {
			break
		}
		lset, chks := s.SeriesSet.At()
		if s.peek == nil {
			s.peek = &typespb.Series{Labels: labelpb.ZLabelsFromPromLabels(lset), Chunks: chks}
			continue
		}

		if labels.Compare(lset, s.peek.PromLabels()) != 0 {
			s.lset, s.chunks = s.peek.PromLabels(), s.peek.Chunks
			s.peek = &typespb.Series{Labels: labelpb.ZLabelsFromPromLabels(lset), Chunks: chks}
			return true
		}

		// We assume non-overlapping, sorted chunks. This is best effort only, if it's otherwise it
		// will just be duplicated, but well handled by StoreAPI consumers.
		s.peek.Chunks = append(s.peek.Chunks, chks...)
	}

	if s.peek == nil {
		return false
	}

	s.lset, s.chunks = s.peek.PromLabels(), s.peek.Chunks
	s.peek = nil
	return true
}

// Deprecated.
// TODO(bwplotka): Remove this once Cortex dep will stop using it.
type Label = labelpb.ZLabel

// Deprecated.
// TODO(bwplotka): Remove this in next PR. Done to reduce diff only.
type LabelSet = labelpb.ZLabelSet

// Deprecated.
// TODO(bwplotka): Remove this once Cortex dep will stop using it.
func CompareLabels(a, b []Label) int {
	return labels.Compare(labelpb.ZLabelsToPromLabels(a), labelpb.ZLabelsToPromLabels(b))
}

// Deprecated.
// TODO(bwplotka): Remove this once Cortex dep will stop using it.
func LabelsToPromLabelsUnsafe(lset []Label) labels.Labels {
	return labelpb.ZLabelsToPromLabels(lset)
}

type SeriesStatsCounter struct {
	lastSeriesHash uint64

	Series  int
	Chunks  int
	Samples int
}

func (c *SeriesStatsCounter) CountSeries(seriesLabels []labelpb.ZLabel) {
	seriesHash := labelpb.HashWithPrefix("", seriesLabels)
	if c.lastSeriesHash != 0 || seriesHash != c.lastSeriesHash {
		c.lastSeriesHash = seriesHash
		c.Series++
	}
}

func (c *SeriesStatsCounter) Count(series *typespb.Series) {
	c.CountSeries(series.Labels)
	for _, chk := range series.Chunks {
		if chk.Raw != nil {
			c.Chunks++
			c.Samples += chk.Raw.XORNumSamples()
		}

		if chk.Count != nil {
			c.Chunks++
			c.Samples += chk.Count.XORNumSamples()
		}

		if chk.Counter != nil {
			c.Chunks++
			c.Samples += chk.Counter.XORNumSamples()
		}

		if chk.Max != nil {
			c.Chunks++
			c.Samples += chk.Max.XORNumSamples()
		}

		if chk.Min != nil {
			c.Chunks++
			c.Samples += chk.Min.XORNumSamples()
		}

		if chk.Sum != nil {
			c.Chunks++
			c.Samples += chk.Sum.XORNumSamples()
		}
	}
}

func (m *SeriesRequest) ToPromQL() string {
	return m.QueryHints.toPromQL(m.Matchers)
}

// IsSafeToExecute returns true if the function or aggregation from the query hint
// can be safely executed by the underlying Prometheus instance without affecting the
// result of the query.
func (m *QueryHints) IsSafeToExecute() bool {
	distributiveOperations := []string{
		"max",
		"max_over_time",
		"min",
		"min_over_time",
		"group",
	}
	for _, op := range distributiveOperations {
		if m.Func.Name == op {
			return true
		}
	}

	return false
}
