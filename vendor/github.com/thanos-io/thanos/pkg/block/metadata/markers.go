// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"context"
	"encoding/json"
	"io"
	"path"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/runutil"
)

const (
	// DeletionMarkFilename is the known json filename for optional file storing details about when block is marked for deletion.
	// If such file is present in block dir, it means the block is meant to be deleted after certain delay.
	DeletionMarkFilename = "deletion-mark.json"
	// NoCompactMarkFilename is the known json filename for optional file storing details about why block has to be excluded from compaction.
	// If such file is present in block dir, it means the block has to excluded from compaction (both vertical and horizontal) or rewrite (e.g deletions).
	NoCompactMarkFilename = "no-compact-mark.json"
	// NoDownsampleMarkFilename is the known json filenanme for optional file storing details about why block has to be excluded from downsampling.
	// If such file is present in block dir, it means the block has to be excluded from downsampling.
	NoDownsampleMarkFilename = "no-downsample-mark.json"
	// DeletionMarkVersion1 is the version of deletion-mark file supported by Thanos.
	DeletionMarkVersion1 = 1
	// NoCompactMarkVersion1 is the version of no-compact-mark file supported by Thanos.
	NoCompactMarkVersion1 = 1
	// NoDownsampleVersion1 is the version of no-downsample-mark file supported by Thanos.
	NoDownsampleMarkVersion1 = 1
)

var (
	// ErrorMarkerNotFound is the error when marker file is not found.
	ErrorMarkerNotFound = errors.New("marker not found")
	// ErrorUnmarshalMarker is the error when unmarshalling marker JSON file.
	// This error can occur because marker has been partially uploaded to block storage
	// or the marker file is not a valid json file.
	ErrorUnmarshalMarker = errors.New("unmarshal marker JSON")
)

type Marker interface {
	markerFilename() string
}

// DeletionMark stores block id and when block was marked for deletion.
type DeletionMark struct {
	// ID of the tsdb block.
	ID ulid.ULID `json:"id"`
	// Version of the file.
	Version int `json:"version"`
	// Details is a human readable string giving details of reason.
	Details string `json:"details,omitempty"`

	// DeletionTime is a unix timestamp of when the block was marked to be deleted.
	DeletionTime int64 `json:"deletion_time"`
}

func (m *DeletionMark) markerFilename() string { return DeletionMarkFilename }

// NoCompactReason is a reason for a block to be excluded from compaction.
type NoCompactReason string

// NoDownsampleReason is a reason for a block to be excluded from downsample.
type NoDownsampleReason string

const (
	// ManualNoCompactReason is a custom reason of excluding from compaction that should be added when no-compact mark is added for unknown/user specified reason.
	ManualNoCompactReason NoCompactReason = "manual"
	// ManualNoDownsampleReason is a custom reason of excluding from downsample that should be added when no-downsample mark is added for unknown/user specified reason.
	ManualNoDownsampleReason NoDownsampleReason = "manual"
	// IndexSizeExceedingNoCompactReason is a reason of index being too big (for example exceeding 64GB limit: https://github.com/thanos-io/thanos/issues/1424)
	// This reason can be ignored when vertical block sharding will be implemented.
	IndexSizeExceedingNoCompactReason = "index-size-exceeding"
	// OutOfOrderChunksNoCompactReason is a reason of to no compact block with index contains out of order chunk so that the compaction is not blocked.
	OutOfOrderChunksNoCompactReason = "block-index-out-of-order-chunk"
	// DownsampleVerticalCompactionNoCompactReason is a reason to not compact overlapping downsampled blocks as it does not make sense e.g. how to vertically compact the average.
	DownsampleVerticalCompactionNoCompactReason = "downsample-vertical-compaction"
)

// NoCompactMark marker stores reason of block being excluded from compaction if needed.
type NoCompactMark struct {
	// ID of the tsdb block.
	ID ulid.ULID `json:"id"`
	// Version of the file.
	Version int `json:"version"`
	// Details is a human readable string giving details of reason.
	Details string `json:"details,omitempty"`

	// NoCompactTime is a unix timestamp of when the block was marked for no compact.
	NoCompactTime int64           `json:"no_compact_time"`
	Reason        NoCompactReason `json:"reason"`
}

func (n *NoCompactMark) markerFilename() string { return NoCompactMarkFilename }

// NoDownsampleMark marker stores reason of block being excluded from downsample if needed.
type NoDownsampleMark struct {
	// ID of the tsdb block.
	ID ulid.ULID `json:"id"`
	// Version of the file.
	Version int `json:"version"`
	// Details is a human readable string giving details of reason.
	Details string `json:"details,omitempty"`

	// NoDownsampleTime is a unix timestamp of when the block was marked for no downsample.
	NoDownsampleTime int64              `json:"no_downsample_time"`
	Reason           NoDownsampleReason `json:"reason"`
}

func (n *NoDownsampleMark) markerFilename() string { return NoDownsampleMarkFilename }

// ReadMarker reads the given mark file from <dir>/<marker filename>.json in bucket.
func ReadMarker(ctx context.Context, logger log.Logger, bkt objstore.InstrumentedBucketReader, dir string, marker Marker) error {
	markerFile := path.Join(dir, marker.markerFilename())
	r, err := bkt.ReaderWithExpectedErrs(bkt.IsObjNotFoundErr).Get(ctx, markerFile)
	if err != nil {
		if bkt.IsObjNotFoundErr(err) {
			return ErrorMarkerNotFound
		}
		return errors.Wrapf(err, "get file: %s", markerFile)
	}
	defer runutil.CloseWithLogOnErr(logger, r, "close bkt marker reader")

	metaContent, err := io.ReadAll(r)
	if err != nil {
		return errors.Wrapf(err, "read file: %s", markerFile)
	}

	if err := json.Unmarshal(metaContent, marker); err != nil {
		return errors.Wrapf(ErrorUnmarshalMarker, "file: %s; err: %v", markerFile, err.Error())
	}
	switch marker.markerFilename() {
	case NoCompactMarkFilename:
		if version := marker.(*NoCompactMark).Version; version != NoCompactMarkVersion1 {
			return errors.Errorf("unexpected no-compact-mark file version %d, expected %d", version, NoCompactMarkVersion1)
		}
	case NoDownsampleMarkFilename:
		if version := marker.(*NoDownsampleMark).Version; version != NoDownsampleMarkVersion1 {
			return errors.Errorf("unexpected no-downsample-mark file version %d, expected %d", version, NoDownsampleMarkVersion1)
		}
	case DeletionMarkFilename:
		if version := marker.(*DeletionMark).Version; version != DeletionMarkVersion1 {
			return errors.Errorf("unexpected deletion-mark file version %d, expected %d", version, DeletionMarkVersion1)
		}
	}
	return nil
}
