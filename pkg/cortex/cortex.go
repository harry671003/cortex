package cortex

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/signals"
	"google.golang.org/grpc/health/grpc_health_v1"
	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/util/grpcclient"
	"github.com/cortexproject/cortex/pkg/util/resource"

	"github.com/cortexproject/cortex/pkg/alertmanager"
	"github.com/cortexproject/cortex/pkg/alertmanager/alertstore"
	"github.com/cortexproject/cortex/pkg/api"
	"github.com/cortexproject/cortex/pkg/compactor"
	"github.com/cortexproject/cortex/pkg/configs"
	configAPI "github.com/cortexproject/cortex/pkg/configs/api"
	"github.com/cortexproject/cortex/pkg/configs/db"
	_ "github.com/cortexproject/cortex/pkg/cortex/configinit"
	"github.com/cortexproject/cortex/pkg/cortex/storage"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/flusher"
	"github.com/cortexproject/cortex/pkg/frontend"
	frontendv1 "github.com/cortexproject/cortex/pkg/frontend/v1"
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/parquetconverter"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/querier/tenantfederation"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/querier/tripperware/queryrange"
	querier_worker "github.com/cortexproject/cortex/pkg/querier/worker"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/memberlist"
	"github.com/cortexproject/cortex/pkg/ruler"
	"github.com/cortexproject/cortex/pkg/ruler/rulestore"
	"github.com/cortexproject/cortex/pkg/scheduler"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storegateway"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/tracing"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/fakeauth"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/grpcutil"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/modules"
	"github.com/cortexproject/cortex/pkg/util/process"
	"github.com/cortexproject/cortex/pkg/util/runtimeconfig"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

var (
	errInvalidHTTPPrefix = errors.New("HTTP prefix should be empty or start with /")
)

// The design pattern for Cortex is a series of config objects, which are
// registered for command line flags, and then a series of components that
// are instantiated and composed.  Some rules of thumb:
// - Config types should only contain 'simple' types (ints, strings, urls etc).
// - Flag validation should be done by the flag; use a flag.Value where
//   appropriate.
// - Config types should map 1:1 with a component type.
// - Config types should define flags with a common prefix.
// - It's fine to nest configs within configs, but this should match the
//   nesting of components within components.
// - Limit as much is possible sharing of configuration between config types.
//   Where necessary, use a pointer for this - avoid repetition.
// - Where a nesting of components its not obvious, it's fine to pass
//   references to other components constructors to compose them.
// - First argument for a components constructor should be its matching config
//   object.

// Config is the root config for Cortex.
type Config struct {
	Target          flagext.StringSliceCSV  `yaml:"target"`
	AuthEnabled     bool                    `yaml:"auth_enabled"`
	PrintConfig     bool                    `yaml:"-"`
	HTTPPrefix      string                  `yaml:"http_prefix"`
	ResourceMonitor configs.ResourceMonitor `yaml:"resource_monitor"`

	ExternalQueryable prom_storage.Queryable `yaml:"-"`
	ExternalPusher    ruler.Pusher           `yaml:"-"`

	API              api.Config                      `yaml:"api"`
	Server           server.Config                   `yaml:"server"`
	Distributor      distributor.Config              `yaml:"distributor"`
	Querier          querier.Config                  `yaml:"querier"`
	IngesterClient   client.Config                   `yaml:"ingester_client"`
	Ingester         ingester.Config                 `yaml:"ingester"`
	Flusher          flusher.Config                  `yaml:"flusher"`
	Storage          storage.Config                  `yaml:"storage"`
	LimitsConfig     validation.Limits               `yaml:"limits"`
	Prealloc         cortexpb.PreallocConfig         `yaml:"prealloc" doc:"hidden"`
	Worker           querier_worker.Config           `yaml:"frontend_worker"`
	Frontend         frontend.CombinedFrontendConfig `yaml:"frontend"`
	QueryRange       queryrange.Config               `yaml:"query_range"`
	BlocksStorage    tsdb.BlocksStorageConfig        `yaml:"blocks_storage"`
	Compactor        compactor.Config                `yaml:"compactor"`
	ParquetConverter parquetconverter.Config         `yaml:"parquet_converter"`
	StoreGateway     storegateway.Config             `yaml:"store_gateway"`
	TenantFederation tenantfederation.Config         `yaml:"tenant_federation"`

	Ruler               ruler.Config                               `yaml:"ruler"`
	RulerStorage        rulestore.Config                           `yaml:"ruler_storage"`
	Configs             configs.Config                             `yaml:"configs"`
	Alertmanager        alertmanager.MultitenantAlertmanagerConfig `yaml:"alertmanager"`
	AlertmanagerStorage alertstore.Config                          `yaml:"alertmanager_storage"`
	RuntimeConfig       runtimeconfig.Config                       `yaml:"runtime_config"`
	MemberlistKV        memberlist.KVConfig                        `yaml:"memberlist"`
	QueryScheduler      scheduler.Config                           `yaml:"query_scheduler"`

	Tracing tracing.Config `yaml:"tracing"`
}

// RegisterFlags registers flag.
func (c *Config) RegisterFlags(f *flag.FlagSet) {
	c.Server.MetricsNamespace = "cortex"
	c.Server.ExcludeRequestInLog = true
	c.Server.MetricsNativeHistogramFactor = 1.1

	// Set the default module list to 'all'
	c.Target = []string{All}

	f.Var(&c.Target, "target", "Comma-separated list of Cortex modules to load. "+
		"The alias 'all' can be used in the list to load a number of core modules and will enable single-binary mode. "+
		"Use '-modules' command line flag to get a list of available modules, and to see which modules are included in 'all'.")

	f.BoolVar(&c.AuthEnabled, "auth.enabled", true, "Set to false to disable auth.")
	f.BoolVar(&c.PrintConfig, "print.config", false, "Print the config and exit.")
	f.StringVar(&c.HTTPPrefix, "http.prefix", "/api/prom", "HTTP path prefix for Cortex API.")

	c.API.RegisterFlags(f)
	c.registerServerFlagsWithChangedDefaultValues(f)
	c.Distributor.RegisterFlags(f)
	c.Querier.RegisterFlags(f)
	c.IngesterClient.RegisterFlags(f)
	c.Ingester.RegisterFlags(f)
	c.Flusher.RegisterFlags(f)
	c.Storage.RegisterFlags(f)
	c.LimitsConfig.RegisterFlags(f)
	c.Prealloc.RegisterFlags(f)
	c.Worker.RegisterFlags(f)
	c.Frontend.RegisterFlags(f)
	c.QueryRange.RegisterFlags(f)
	c.BlocksStorage.RegisterFlags(f)
	c.Compactor.RegisterFlags(f)
	c.ParquetConverter.RegisterFlags(f)
	c.StoreGateway.RegisterFlags(f)
	c.TenantFederation.RegisterFlags(f)
	c.ResourceMonitor.RegisterFlags(f)

	c.Ruler.RegisterFlags(f)
	c.RulerStorage.RegisterFlags(f)
	c.Configs.RegisterFlags(f)
	c.Alertmanager.RegisterFlags(f)
	c.AlertmanagerStorage.RegisterFlags(f)
	c.RuntimeConfig.RegisterFlags(f)
	c.MemberlistKV.RegisterFlags(f)
	c.QueryScheduler.RegisterFlags(f)
	c.Tracing.RegisterFlags(f)
}

// Validate the cortex config and returns an error if the validation
// doesn't pass
func (c *Config) Validate(log log.Logger) error {
	if err := c.validateYAMLEmptyNodes(); err != nil {
		return err
	}

	if c.HTTPPrefix != "" && !strings.HasPrefix(c.HTTPPrefix, "/") {
		return errInvalidHTTPPrefix
	}

	if err := c.API.Validate(); err != nil {
		return errors.Wrap(err, "invalid api config")
	}
	if err := c.Storage.Validate(); err != nil {
		return errors.Wrap(err, "invalid storage config")
	}
	if err := c.RulerStorage.Validate(); err != nil {
		return errors.Wrap(err, "invalid rulestore config")
	}
	if err := c.Ruler.Validate(c.LimitsConfig, log); err != nil {
		return errors.Wrap(err, "invalid ruler config")
	}
	if err := c.BlocksStorage.Validate(); err != nil {
		return errors.Wrap(err, "invalid TSDB config")
	}
	if err := c.LimitsConfig.Validate(c.Distributor.ShardByAllLabels, c.Ingester.ActiveSeriesMetricsEnabled); err != nil {
		return errors.Wrap(err, "invalid limits config")
	}
	if err := c.ResourceMonitor.Validate(); err != nil {
		return errors.Wrap(err, "invalid resource-monitor config")
	}
	if err := c.Distributor.Validate(c.LimitsConfig); err != nil {
		return errors.Wrap(err, "invalid distributor config")
	}
	if err := c.Querier.Validate(); err != nil {
		return errors.Wrap(err, "invalid querier config")
	}
	if err := c.IngesterClient.Validate(log); err != nil {
		return errors.Wrap(err, "invalid ingester_client config")
	}
	if err := c.Worker.Validate(log); err != nil {
		return errors.Wrap(err, "invalid frontend_worker config")
	}
	if err := c.QueryRange.Validate(c.Querier); err != nil {
		return errors.Wrap(err, "invalid query_range config")
	}
	if err := c.StoreGateway.Validate(c.LimitsConfig, c.ResourceMonitor.Resources); err != nil {
		return errors.Wrap(err, "invalid store-gateway config")
	}
	if err := c.Compactor.Validate(c.LimitsConfig); err != nil {
		return errors.Wrap(err, "invalid compactor config")
	}
	if err := c.AlertmanagerStorage.Validate(); err != nil {
		return errors.Wrap(err, "invalid alertmanager storage config")
	}
	if err := c.Alertmanager.Validate(c.AlertmanagerStorage); err != nil {
		return errors.Wrap(err, "invalid alertmanager config")
	}

	if err := c.Ingester.Validate(c.ResourceMonitor.Resources); err != nil {
		return errors.Wrap(err, "invalid ingester config")
	}

	if err := c.Tracing.Validate(); err != nil {
		return errors.Wrap(err, "invalid tracing config")
	}

	return nil
}

func (c *Config) isModuleEnabled(m string) bool {
	return util.StringsContain(c.Target, m)
}

// validateYAMLEmptyNodes ensure that no empty node has been specified in the YAML config file.
// When an empty node is defined in YAML, the YAML parser sets the whole struct to its zero value
// and so we loose all default values. It's very difficult to detect this case for the user, so we
// try to prevent it (on the root level) with this custom validation.
func (c *Config) validateYAMLEmptyNodes() error {
	defaults := Config{}
	flagext.DefaultValues(&defaults)

	defStruct := reflect.ValueOf(defaults)
	cfgStruct := reflect.ValueOf(*c)

	// We expect all structs are the exact same. This check should never fail.
	if cfgStruct.NumField() != defStruct.NumField() {
		return errors.New("unable to validate configuration because of mismatching internal config data structure")
	}

	for i := 0; i < cfgStruct.NumField(); i++ {
		// If the struct has been reset due to empty YAML value and the zero struct value
		// doesn't match the default one, then we should warn the user about the issue.
		if cfgStruct.Field(i).Kind() == reflect.Struct && cfgStruct.Field(i).IsZero() && !defStruct.Field(i).IsZero() {
			return fmt.Errorf("the %s configuration in YAML has been specified as an empty YAML node", cfgStruct.Type().Field(i).Name)
		}
	}

	return nil
}

func (c *Config) registerServerFlagsWithChangedDefaultValues(fs *flag.FlagSet) {
	throwaway := flag.NewFlagSet("throwaway", flag.PanicOnError)

	// Register to throwaway flags first. Default values are remembered during registration and cannot be changed,
	// but we can take values from throwaway flag set and reregister into supplied flags with new default values.
	c.Server.RegisterFlags(throwaway)

	throwaway.VisitAll(func(f *flag.Flag) {
		// Ignore errors when setting new values. We have a test to verify that it works.
		switch f.Name {
		case "server.grpc.keepalive.min-time-between-pings":
			_ = f.Value.Set("10s")

		case "server.grpc.keepalive.ping-without-stream-allowed":
			_ = f.Value.Set("true")
		}

		fs.Var(f.Value, f.Name, f.Usage)
	})
}

// Cortex is the root datastructure for Cortex.
type Cortex struct {
	Cfg Config

	// set during initialization
	ServiceMap    map[string]services.Service
	ModuleManager *modules.Manager

	API                      *api.API
	Server                   *server.Server
	Ring                     *ring.Ring
	TenantLimits             validation.TenantLimits
	Overrides                *validation.Overrides
	Distributor              *distributor.Distributor
	Ingester                 *ingester.Ingester
	Flusher                  *flusher.Flusher
	Frontend                 *frontendv1.Frontend
	RuntimeConfig            *runtimeconfig.Manager
	QuerierQueryable         prom_storage.SampleAndChunkQueryable
	ExemplarQueryable        prom_storage.ExemplarQueryable
	MetadataQuerier          querier.MetadataQuerier
	QuerierEngine            promql.QueryEngine
	QueryFrontendTripperware tripperware.Tripperware
	ResourceMonitor          *resource.Monitor

	Ruler            *ruler.Ruler
	RulerStorage     rulestore.RuleStore
	ConfigAPI        *configAPI.API
	ConfigDB         db.DB
	Alertmanager     *alertmanager.MultitenantAlertmanager
	Compactor        *compactor.Compactor
	Parquetconverter *parquetconverter.Converter
	StoreGateway     *storegateway.StoreGateway
	MemberlistKV     *memberlist.KVInitService

	// Queryables that the querier should use to query the long
	// term storage. It depends on the storage engine used.
	StoreQueryables []querier.QueryableWithFilter
}

// New makes a new Cortex.
func New(cfg Config) (*Cortex, error) {
	if cfg.PrintConfig {
		if err := yaml.NewEncoder(os.Stdout).Encode(&cfg); err != nil {
			fmt.Println("Error encoding config:", err)
		}
		os.Exit(0)
	}

	// Swap out the default resolver to support multiple tenant IDs separated by a '|'
	if cfg.TenantFederation.Enabled {
		util_log.WarnExperimentalUse("tenant-federation")
		tenant.WithDefaultResolver(tenant.NewMultiResolver())
	}

	cfg.API.HTTPAuthMiddleware = fakeauth.SetupAuthMiddleware(&cfg.Server, cfg.AuthEnabled,
		// Also don't check auth for these gRPC methods, since single call is used for multiple users (or no user like health check).
		[]string{
			"/grpc.health.v1.Health/Check",
			"/frontend.Frontend/Process",
			"/frontend.Frontend/NotifyClientShutdown",
			"/schedulerpb.SchedulerForFrontend/FrontendLoop",
			"/schedulerpb.SchedulerForQuerier/QuerierLoop",
			"/schedulerpb.SchedulerForQuerier/NotifyQuerierShutdown",
		})

	cortex := &Cortex{
		Cfg: cfg,
	}

	cortex.setupThanosTracing()
	cortex.setupGRPCHeaderForwarding()
	cortex.setupRequestSigning()

	if err := cortex.setupModuleManager(); err != nil {
		return nil, err
	}

	cortex.setupPromQLFunctions()
	return cortex, nil
}

// setupThanosTracing appends a gRPC middleware used to inject our tracer into the custom
// context used by Thanos, in order to get Thanos spans correctly attached to our traces.
func (t *Cortex) setupThanosTracing() {
	t.Cfg.Server.GRPCMiddleware = append(t.Cfg.Server.GRPCMiddleware, ThanosTracerUnaryInterceptor)
	t.Cfg.Server.GRPCStreamMiddleware = append(t.Cfg.Server.GRPCStreamMiddleware, ThanosTracerStreamInterceptor)
}

// setupGRPCHeaderForwarding appends a gRPC middleware used to enable the propagation of
// HTTP Headers through child gRPC calls
func (t *Cortex) setupGRPCHeaderForwarding() {
	if len(t.Cfg.API.HTTPRequestHeadersToLog) > 0 {
		t.Cfg.Server.GRPCMiddleware = append(t.Cfg.Server.GRPCMiddleware, grpcutil.HTTPHeaderPropagationServerInterceptor)
		t.Cfg.Server.GRPCStreamMiddleware = append(t.Cfg.Server.GRPCStreamMiddleware, grpcutil.HTTPHeaderPropagationStreamServerInterceptor)
	}
}

func (t *Cortex) setupRequestSigning() {
	if t.Cfg.Distributor.SignWriteRequestsEnabled {
		util_log.WarnExperimentalUse("Distributor SignWriteRequestsEnabled")
		t.Cfg.Server.GRPCMiddleware = append(t.Cfg.Server.GRPCMiddleware, grpcclient.UnarySigningServerInterceptor)
	}
}

// Run starts Cortex running, and blocks until a Cortex stops.
func (t *Cortex) Run() error {
	// Register custom process metrics.
	if c, err := process.NewProcessCollector(); err == nil {
		prometheus.MustRegister(c)
	} else {
		level.Warn(util_log.Logger).Log("msg", "skipped registration of custom process metrics collector", "err", err)
	}

	for _, module := range t.Cfg.Target {
		if !t.ModuleManager.IsUserVisibleModule(module) {
			level.Warn(util_log.Logger).Log("msg", "selected target is an internal module, is this intended?", "target", module)
		}
	}

	var err error
	t.ServiceMap, err = t.ModuleManager.InitModuleServices(t.Cfg.Target...)
	if err != nil {
		return err
	}

	t.API.RegisterServiceMapHandler(http.HandlerFunc(t.servicesHandler))

	// get all services, create service manager and tell it to start
	servs := []services.Service(nil)
	for _, s := range t.ServiceMap {
		servs = append(servs, s)
	}

	sm, err := services.NewManager(servs...)
	if err != nil {
		return err
	}

	// before starting servers, register /ready handler and gRPC health check service.
	// It should reflect entire Cortex.
	t.Server.HTTP.Path("/ready").Handler(t.readyHandler(sm))
	grpc_health_v1.RegisterHealthServer(t.Server.GRPC, grpcutil.NewHealthCheck(sm))

	// Let's listen for events from this manager, and log them.
	healthy := func() { level.Info(util_log.Logger).Log("msg", "Cortex started") }
	stopped := func() { level.Info(util_log.Logger).Log("msg", "Cortex stopped") }
	serviceFailed := func(service services.Service) {
		// if any service fails, stop entire Cortex
		sm.StopAsync()

		// let's find out which module failed
		for m, s := range t.ServiceMap {
			if s == service {
				if service.FailureCase() == modules.ErrStopProcess {
					level.Info(util_log.Logger).Log("msg", "received stop signal via return error", "module", m, "err", service.FailureCase())
				} else {
					level.Error(util_log.Logger).Log("msg", "module failed", "module", m, "err", service.FailureCase())
				}
				return
			}
		}

		level.Error(util_log.Logger).Log("msg", "module failed", "module", "unknown", "err", service.FailureCase())
	}

	sm.AddListener(services.NewManagerListener(healthy, stopped, serviceFailed))

	// Setup signal handler. If signal arrives, we stop the manager, which stops all the services.
	handler := signals.NewHandler(t.Server.Log)
	go func() {
		handler.Loop()
		sm.StopAsync()
	}()

	// Start all services. This can really only fail if some service is already
	// in other state than New, which should not be the case.
	err = sm.StartAsync(context.Background())
	if err == nil {
		// Wait until service manager stops. It can stop in two ways:
		// 1) Signal is received and manager is stopped.
		// 2) Any service fails.
		err = sm.AwaitStopped(context.Background())
	}

	// If there is no error yet (= service manager started and then stopped without problems),
	// but any service failed, report that failure as an error to caller.
	if err == nil {
		if failed := sm.ServicesByState()[services.Failed]; len(failed) > 0 {
			for _, f := range failed {
				if f.FailureCase() != modules.ErrStopProcess {
					// Details were reported via failure listener before
					err = errors.New("failed services")
					break
				}
			}
		}
	}
	return err
}

func (t *Cortex) readyHandler(sm *services.Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !sm.IsHealthy() {
			msg := bytes.Buffer{}
			msg.WriteString("Some services are not Running:\n")

			byState := sm.ServicesByState()
			for st, ls := range byState {
				msg.WriteString(fmt.Sprintf("%v: %d\n", st, len(ls)))
			}

			http.Error(w, msg.String(), http.StatusServiceUnavailable)
			return
		}

		// Ingester has a special check that makes sure that it was able to register into the ring,
		// and that all other ring entries are OK too.
		if t.Ingester != nil {
			if err := t.Ingester.CheckReady(r.Context()); err != nil {
				http.Error(w, "Ingester not ready: "+err.Error(), http.StatusServiceUnavailable)
				return
			}
		}

		// Query Frontend has a special check that makes sure that a querier is attached before it signals
		// itself as ready
		if t.Frontend != nil {
			if err := t.Frontend.CheckReady(r.Context()); err != nil {
				http.Error(w, "Query Frontend not ready: "+err.Error(), http.StatusServiceUnavailable)
				return
			}
		}

		util.WriteTextResponse(w, "ready")
	}
}

func (t *Cortex) setupPromQLFunctions() {
	// The holt_winters function is renamed to double_exponential_smoothing and has been experimental since Prometheus v3. (https://github.com/prometheus/prometheus/pull/14930)
	// The cortex supports holt_winters for users using this function.
	querier.EnableExperimentalPromQLFunctions(t.Cfg.Querier.EnablePromQLExperimentalFunctions, true)
}
