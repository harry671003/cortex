package storegateway

import (
	"context"
	"errors"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/pkg/ring"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
)

const (
	shardExcludedMeta = "shard-excluded"
)

var (
	errBlockNotOwned = errors.New("block not owned")
)

type ShardingStrategy interface {
	// FilterUsers whose blocks should be loaded by the store-gateway. Returns the list of user IDs
	// that should be synced by the store-gateway.
	FilterUsers(ctx context.Context, userIDs []string) []string

	// FilterBlocks filters metas in-place keeping only blocks that should be loaded by the store-gateway.
	// The provided loaded map contains blocks which have been previously returned by this function and
	// are now loaded or loading in the store-gateway.
	FilterBlocks(ctx context.Context, userID string, metas map[ulid.ULID]*metadata.Meta, loaded map[ulid.ULID]struct{}, synced block.GaugeVec) error

	// OwnBlock checks if the block is owned by the current instance.
	OwnBlock(userID string, meta metadata.Meta) (bool, error)
}

// ShardingLimits is the interface that should be implemented by the limits provider,
// limiting the scope of the limits to the ones required by sharding strategies.
type ShardingLimits interface {
	StoreGatewayTenantShardSize(userID string) float64
}

func filterDisallowedTenants(userIDs []string, logger log.Logger, allowedTenants *util.AllowedTenants) []string {
	filteredUserIDs := []string{}
	for _, userID := range userIDs {
		if !allowedTenants.IsAllowed(userID) {
			level.Debug(logger).Log("msg", "ignoring storage gateway for user, not allowed", "user", userID)
			continue
		}

		filteredUserIDs = append(filteredUserIDs, userID)
	}

	return filteredUserIDs
}

// NoShardingStrategy is a no-op strategy. When this strategy is used, no tenant/block is filtered out.
type NoShardingStrategy struct {
	logger         log.Logger
	allowedTenants *util.AllowedTenants
}

func NewNoShardingStrategy(logger log.Logger, allowedTenants *util.AllowedTenants) *NoShardingStrategy {
	return &NoShardingStrategy{
		logger:         logger,
		allowedTenants: allowedTenants,
	}
}

func (s *NoShardingStrategy) FilterUsers(_ context.Context, userIDs []string) []string {
	return filterDisallowedTenants(userIDs, s.logger, s.allowedTenants)
}

func (s *NoShardingStrategy) FilterBlocks(_ context.Context, _ string, _ map[ulid.ULID]*metadata.Meta, _ map[ulid.ULID]struct{}, _ block.GaugeVec) error {
	return nil
}

func (s *NoShardingStrategy) OwnBlock(_ string, meta metadata.Meta) (bool, error) {
	return true, nil
}

// DefaultShardingStrategy is a sharding strategy based on the hash ring formed by store-gateways.
// Not go-routine safe.
type DefaultShardingStrategy struct {
	r              *ring.Ring
	instanceAddr   string
	logger         log.Logger
	allowedTenants *util.AllowedTenants
}

// NewDefaultShardingStrategy creates DefaultShardingStrategy.
func NewDefaultShardingStrategy(r *ring.Ring, instanceAddr string, logger log.Logger, allowedTenants *util.AllowedTenants) *DefaultShardingStrategy {
	return &DefaultShardingStrategy{
		r:            r,
		instanceAddr: instanceAddr,
		logger:       logger,

		allowedTenants: allowedTenants,
	}
}

// FilterUsers implements ShardingStrategy.
func (s *DefaultShardingStrategy) FilterUsers(_ context.Context, userIDs []string) []string {
	return filterDisallowedTenants(userIDs, s.logger, s.allowedTenants)
}

// FilterBlocks implements ShardingStrategy.
func (s *DefaultShardingStrategy) FilterBlocks(_ context.Context, _ string, metas map[ulid.ULID]*metadata.Meta, loaded map[ulid.ULID]struct{}, synced block.GaugeVec) error {
	filterBlocksByRingSharding(s.r, s.instanceAddr, metas, loaded, synced, s.logger)
	return nil
}

func (s *DefaultShardingStrategy) OwnBlock(_ string, meta metadata.Meta) (bool, error) {
	key := cortex_tsdb.HashBlockID(meta.ULID)

	// Check if the block is owned by the store-gateway
	set, err := s.r.Get(key, BlocksOwnerSync, nil, nil, nil)
	if err != nil {
		return false, err
	}
	return set.Includes(s.instanceAddr), nil
}

// ShuffleShardingStrategy is a shuffle sharding strategy, based on the hash ring formed by store-gateways,
// where each tenant blocks are sharded across a subset of store-gateway instances.
type ShuffleShardingStrategy struct {
	r            *ring.Ring
	instanceID   string
	instanceAddr string
	limits       ShardingLimits
	logger       log.Logger

	zoneStableShuffleSharding bool
	allowedTenants            *util.AllowedTenants
}

// NewShuffleShardingStrategy makes a new ShuffleShardingStrategy.
func NewShuffleShardingStrategy(r *ring.Ring, instanceID, instanceAddr string, limits ShardingLimits, logger log.Logger, allowedTenants *util.AllowedTenants, zoneStableShuffleSharding bool) *ShuffleShardingStrategy {
	return &ShuffleShardingStrategy{
		r:            r,
		instanceID:   instanceID,
		instanceAddr: instanceAddr,
		limits:       limits,
		logger:       logger,

		zoneStableShuffleSharding: zoneStableShuffleSharding,
		allowedTenants:            allowedTenants,
	}
}

// FilterUsers implements ShardingStrategy.
func (s *ShuffleShardingStrategy) FilterUsers(_ context.Context, userIDs []string) []string {
	var filteredIDs []string
	for _, userID := range filterDisallowedTenants(userIDs, s.logger, s.allowedTenants) {
		subRing := GetShuffleShardingSubring(s.r, userID, s.limits, s.zoneStableShuffleSharding)

		// Include the user only if it belongs to this store-gateway shard.
		if subRing.HasInstance(s.instanceID) {
			filteredIDs = append(filteredIDs, userID)
		}
	}

	return filteredIDs
}

// FilterBlocks implements ShardingStrategy.
func (s *ShuffleShardingStrategy) FilterBlocks(_ context.Context, userID string, metas map[ulid.ULID]*metadata.Meta, loaded map[ulid.ULID]struct{}, synced block.GaugeVec) error {
	subRing := GetShuffleShardingSubring(s.r, userID, s.limits, s.zoneStableShuffleSharding)
	filterBlocksByRingSharding(subRing, s.instanceAddr, metas, loaded, synced, s.logger)
	return nil
}

func (s *ShuffleShardingStrategy) OwnBlock(userID string, meta metadata.Meta) (bool, error) {
	subRing := GetShuffleShardingSubring(s.r, userID, s.limits, s.zoneStableShuffleSharding)
	key := cortex_tsdb.HashBlockID(meta.ULID)

	// Check if the block is owned by the store-gateway
	set, err := subRing.Get(key, BlocksOwnerSync, nil, nil, nil)
	if err != nil {
		return false, err
	}
	return set.Includes(s.instanceAddr), nil
}

func filterBlocksByRingSharding(r ring.ReadRing, instanceAddr string, metas map[ulid.ULID]*metadata.Meta, loaded map[ulid.ULID]struct{}, synced block.GaugeVec, logger log.Logger) {
	bufDescs, bufHosts, bufZones := ring.MakeBuffersForGet()

	for blockID := range metas {
		key := cortex_tsdb.HashBlockID(blockID)

		// Check if the block is owned by the store-gateway
		set, err := r.Get(key, BlocksOwnerSync, bufDescs, bufHosts, bufZones)

		// If an error occurs while checking the ring, we keep the previously loaded blocks.
		if err != nil {
			if _, ok := loaded[blockID]; ok {
				level.Warn(logger).Log("msg", "failed to check block owner but block is kept because was previously loaded", "block", blockID.String(), "err", err)
			} else {
				level.Warn(logger).Log("msg", "failed to check block owner and block has been excluded because was not previously loaded", "block", blockID.String(), "err", err)

				// Skip the block.
				synced.WithLabelValues(shardExcludedMeta).Inc()
				delete(metas, blockID)
			}

			continue
		}

		// Keep the block if it is owned by the store-gateway.
		if set.Includes(instanceAddr) {
			continue
		}

		// The block is not owned by the store-gateway. However, if it's currently loaded
		// we can safely unload it only once at least 1 authoritative owner is available
		// for queries.
		if _, ok := loaded[blockID]; ok {
			// The ring Get() returns an error if there's no available instance.
			if _, err := r.Get(key, BlocksOwnerRead, bufDescs, bufHosts, bufZones); err != nil {
				// Keep the block.
				continue
			}
		}

		// The block is not owned by the store-gateway and there's at least 1 available
		// authoritative owner available for queries, so we can filter it out (and unload
		// it if it was loaded).
		synced.WithLabelValues(shardExcludedMeta).Inc()
		delete(metas, blockID)
	}
}

// GetShuffleShardingSubring returns the subring to be used for a given user. This function
// should be used both by store-gateway and querier in order to guarantee the same logic is used.
func GetShuffleShardingSubring(ring *ring.Ring, userID string, limits ShardingLimits, zoneStableShuffleSharding bool) ring.ReadRing {
	shardSize := util.DynamicShardSize(limits.StoreGatewayTenantShardSize(userID), ring.InstancesCount())

	// A shard size of 0 means shuffle sharding is disabled for this specific user,
	// so we just return the full ring so that blocks will be sharded across all store-gateways.
	if shardSize <= 0 {
		return ring
	}

	if zoneStableShuffleSharding {
		// Zone stability is required for store gateway when shuffle shard, see
		// https://github.com/cortexproject/cortex/issues/5467 for more details.
		return ring.ShuffleShardWithZoneStability(userID, shardSize)
	}
	return ring.ShuffleShard(userID, shardSize)
}

type shardingMetadataFilterAdapter struct {
	userID   string
	strategy ShardingStrategy

	// Keep track of the last blocks returned by the Filter() function.
	lastBlocks map[ulid.ULID]struct{}
}

func NewShardingMetadataFilterAdapter(userID string, strategy ShardingStrategy) block.MetadataFilter {
	return &shardingMetadataFilterAdapter{
		userID:     userID,
		strategy:   strategy,
		lastBlocks: map[ulid.ULID]struct{}{},
	}
}

// Filter implements block.MetadataFilter.
// This function is NOT safe for use by multiple goroutines concurrently.
func (a *shardingMetadataFilterAdapter) Filter(ctx context.Context, metas map[ulid.ULID]*metadata.Meta, synced block.GaugeVec, _ block.GaugeVec) error {
	if err := a.strategy.FilterBlocks(ctx, a.userID, metas, a.lastBlocks, synced); err != nil {
		return err
	}

	// Keep track of the last filtered blocks.
	a.lastBlocks = make(map[ulid.ULID]struct{}, len(metas))
	for blockID := range metas {
		a.lastBlocks[blockID] = struct{}{}
	}

	return nil
}

type shardingBucketReaderAdapter struct {
	objstore.InstrumentedBucketReader

	userID   string
	strategy ShardingStrategy
}

func NewShardingBucketReaderAdapter(userID string, strategy ShardingStrategy, wrapped objstore.InstrumentedBucketReader) objstore.InstrumentedBucketReader {
	return &shardingBucketReaderAdapter{
		InstrumentedBucketReader: wrapped,
		userID:                   userID,
		strategy:                 strategy,
	}
}

// Iter implements objstore.BucketReader.
func (a *shardingBucketReaderAdapter) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	// Skip iterating the bucket if the tenant doesn't belong to the shard. From the caller
	// perspective, this will look like the tenant has no blocks in the storage.
	if len(a.strategy.FilterUsers(ctx, []string{a.userID})) == 0 {
		return nil
	}

	return a.InstrumentedBucketReader.Iter(ctx, dir, f, options...)
}

type shardingBlockLifecycleCallbackAdapter struct {
	userID   string
	strategy ShardingStrategy
	logger   log.Logger
}

func (a *shardingBlockLifecycleCallbackAdapter) PreAdd(meta metadata.Meta) error {
	own, err := a.strategy.OwnBlock(a.userID, meta)
	// If unable to check if block is owned or not because of ring error, mark it as owned
	// and ignore the error.
	if err != nil || own {
		return nil
	}
	level.Info(a.logger).Log("msg", "block not owned from pre check", "block", meta.ULID.String())
	return errBlockNotOwned
}
