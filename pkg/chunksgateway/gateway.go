package chunksgateway

import (
	"context"
	"time"

	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storegateway"
	"github.com/cortexproject/cortex/pkg/storegateway/storegatewaypb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/weaveworks/common/logging"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
)

const (
	syncReasonInitial  = "initial"
	syncReasonPeriodic = "periodic"
)

type ChunksGateway struct {
	services.Service

	logger log.Logger

	gatewayCfg storegateway.Config
	storageCfg cortex_tsdb.BlocksStorageConfig

	stores *storegateway.BucketStores
}

func NewChunksGateway(gatewayCfg storegateway.Config, storageCfg cortex_tsdb.BlocksStorageConfig, limits *validation.Overrides, logLevel logging.Level, logger log.Logger, reg prometheus.Registerer) (*ChunksGateway, error) {
	bucketClient, err := createBucketClient(storageCfg, logger, reg)
	if err != nil {
		return nil, err
	}

	return newChunksGateway(gatewayCfg, storageCfg, bucketClient, limits, logLevel, logger, reg)
}

func newChunksGateway(gatewayCfg storegateway.Config, storageCfg cortex_tsdb.BlocksStorageConfig, bucketClient objstore.Bucket, limits *validation.Overrides, logLevel logging.Level, logger log.Logger, reg prometheus.Registerer) (*ChunksGateway, error) {
	var err error

	g := &ChunksGateway{
		gatewayCfg: gatewayCfg,
		storageCfg: storageCfg,
		logger:     logger,
	}

	// Init sharding strategy.
	shardingStrategy := storegateway.NewNoShardingStrategy()

	g.stores, err = storegateway.NewBucketStores(storageCfg, shardingStrategy, bucketClient, false, limits, logLevel, logger, extprom.WrapRegistererWith(prometheus.Labels{"component": "store-gateway"}, reg))
	if err != nil {
		return nil, errors.Wrap(err, "create bucket stores")
	}

	g.Service = services.NewBasicService(g.starting, g.running, g.stopping)

	return g, nil
}

func (g *ChunksGateway) starting(ctx context.Context) (err error) {
	if err = g.stores.InitialSync(ctx); err != nil {
		return errors.Wrap(err, "initial blocks synchronization")
	}

	return nil
}

func (g *ChunksGateway) running(ctx context.Context) error {
	// Apply a jitter to the sync frequency in order to increase the probability
	// of hitting the shared cache (if any).
	syncTicker := time.NewTicker(util.DurationWithJitter(g.storageCfg.BucketStore.SyncInterval, 0.2))
	defer syncTicker.Stop()

	for {
		select {
		case <-syncTicker.C:
			g.syncStores(ctx, syncReasonPeriodic)
		case <-ctx.Done():
			return nil
		}
	}
}

func (g *ChunksGateway) stopping(error) error {
	return nil
}

func (g *ChunksGateway) syncStores(ctx context.Context, reason string) {
	level.Info(g.logger).Log("msg", "synchronizing TSDB blocks for all users", "reason", reason)

	if err := g.stores.SyncBlocks(ctx); err != nil {
		level.Warn(g.logger).Log("msg", "failed to synchronize TSDB blocks", "reason", reason, "err", err)
	} else {
		level.Info(g.logger).Log("msg", "successfully synchronized TSDB blocks for all users", "reason", reason)
	}
}

func createBucketClient(cfg cortex_tsdb.BlocksStorageConfig, logger log.Logger, reg prometheus.Registerer) (objstore.Bucket, error) {
	bucketClient, err := bucket.NewClient(context.Background(), cfg.Bucket, "store-gateway", logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "create bucket client")
	}

	return bucketClient, nil
}

func (g *ChunksGateway) Chunks(srv storegatewaypb.ChunksGateway_ChunksServer) (err error) {
	return g.stores.Chunks(srv)
}
