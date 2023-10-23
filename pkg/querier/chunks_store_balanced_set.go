package querier

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/extprom"

	"github.com/cortexproject/cortex/pkg/ring/client"
	"github.com/cortexproject/cortex/pkg/storegateway/storegatewaypb"
	"github.com/cortexproject/cortex/pkg/util/services"
)

// BlocksStoreClient is the interface that should be implemented by any client used
// to query a backend store-gateway.
type ChunksStoreClient interface {
	storegatewaypb.ChunksGatewayClient

	// RemoteAddress returns the address of the remote store-gateway and is used to uniquely
	// identify a store-gateway backend instance.
	RemoteAddress() string
}

// BlocksStoreSet implementation used when the blocks are not sharded in the store-gateway
// and so requests are balanced across the set of store-gateway instances.
type chunksStoreBalancedSet struct {
	services.Service

	serviceAddresses []string
	clientsPool      *client.Pool
	dnsProvider      *dns.Provider

	logger log.Logger
}

func newChunksStoreBalancedSet(serviceAddresses []string, clientConfig ClientConfig, logger log.Logger, reg prometheus.Registerer) *chunksStoreBalancedSet {
	const dnsResolveInterval = 10 * time.Second

	dnsProviderReg := extprom.WrapRegistererWithPrefix("cortex_chunksgateway_client_", reg)

	reg = extprom.WrapRegistererWithPrefix("cortex_chunksgateway_client_", reg)

	s := &chunksStoreBalancedSet{
		serviceAddresses: serviceAddresses,
		dnsProvider:      dns.NewProvider(logger, dnsProviderReg, dns.GolangResolverType),
		clientsPool:      newChunksGatewayClientPool(nil, clientConfig, logger, reg),
		logger:           logger,
	}

	s.Service = services.NewTimerService(dnsResolveInterval, s.starting, s.resolve, nil)
	return s
}

func (s *chunksStoreBalancedSet) starting(ctx context.Context) error {
	// Initial DNS resolution.
	return s.resolve(ctx)
}

func (s *chunksStoreBalancedSet) resolve(ctx context.Context) error {
	if err := s.dnsProvider.Resolve(ctx, s.serviceAddresses); err != nil {
		level.Error(s.logger).Log("msg", "failed to resolve store-gateway addresses", "err", err, "addresses", s.serviceAddresses)
	}
	return nil
}

func (s *chunksStoreBalancedSet) GetClientsFor(_ string, blockIDs []ulid.ULID, exclude map[ulid.ULID][]string, _ map[ulid.ULID]map[string]int) (map[ulid.ULID]ChunksStoreClient, error) {
	addresses := s.dnsProvider.Addresses()
	if len(addresses) == 0 {
		return nil, fmt.Errorf("no address resolved for the store-gateway service addresses %s", strings.Join(s.serviceAddresses, ","))
	}

	// Randomize the list of addresses to not always query the same address.
	rand.Shuffle(len(addresses), func(i, j int) {
		addresses[i], addresses[j] = addresses[j], addresses[i]
	})

	// Pick a non excluded client for each block.
	clients := map[ulid.ULID]ChunksStoreClient{}

	for _, blockID := range blockIDs {
		// Pick the first non excluded store-gateway instance.
		addr := getFirstNonExcludedAddr(addresses, exclude[blockID])
		if addr == "" {
			return nil, fmt.Errorf("no store-gateway instance left after filtering out excluded instances for block %s", blockID.String())
		}

		c, err := s.clientsPool.GetClientFor(addr)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get store-gateway client for %s", addr)
		}

		clients[blockID] = c.(ChunksStoreClient)
	}

	return clients, nil
}
