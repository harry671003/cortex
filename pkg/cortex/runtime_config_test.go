package cortex

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

// Given limits are usually loaded via a config file, and that
// a configmap is limited to 1MB, we need to minimise the limits file.
// One way to do it is via YAML anchors.
func TestLoadRuntimeConfig_ShouldLoadAnchoredYAML(t *testing.T) {
	yamlFile := strings.NewReader(`
overrides:
  '1234': &id001
    ingestion_burst_size: 15000
    ingestion_rate: 1500
    max_global_series_per_metric: 7000
    max_global_series_per_user: 15000
    max_series_per_metric: 0
    max_series_per_user: 0
    ruler_max_rule_groups_per_tenant: 20
    ruler_max_rules_per_rule_group: 20
  '1235': *id001
  '1236': *id001
`)
	loader := runtimeConfigLoader{cfg: Config{Distributor: distributor.Config{ShardByAllLabels: true}}}
	runtimeCfg, err := loader.load(yamlFile)
	require.NoError(t, err)

	limits := validation.Limits{
		IngestionRate:               1500,
		IngestionBurstSize:          15000,
		MaxGlobalSeriesPerUser:      15000,
		MaxGlobalSeriesPerMetric:    7000,
		RulerMaxRulesPerRuleGroup:   20,
		RulerMaxRuleGroupsPerTenant: 20,
	}

	loadedLimits := runtimeCfg.(*RuntimeConfigValues).TenantLimits
	require.Equal(t, 3, len(loadedLimits))
	require.Equal(t, limits, *loadedLimits["1234"])
	require.Equal(t, limits, *loadedLimits["1235"])
	require.Equal(t, limits, *loadedLimits["1236"])
}

func TestLoadRuntimeConfig_ShouldLoadEmptyFile(t *testing.T) {
	yamlFile := strings.NewReader(`
# This is an empty YAML.
`)
	actual, err := runtimeConfigLoader{}.load(yamlFile)
	require.NoError(t, err)
	assert.Equal(t, &RuntimeConfigValues{}, actual)
}

func TestLoadRuntimeConfig_MissingPointerFieldsAreNil(t *testing.T) {
	yamlFile := strings.NewReader(`
# This is an empty YAML.
`)
	actual, err := runtimeConfigLoader{}.load(yamlFile)
	require.NoError(t, err)

	actualCfg, ok := actual.(*RuntimeConfigValues)
	require.Truef(t, ok, "expected to be able to cast %+v to RuntimeConfigValues", actual)

	// Ensure that when settings are omitted, the pointers are nil. See #4228
	assert.Nil(t, actualCfg.IngesterLimits)
}

func TestLoadRuntimeConfig_ShouldReturnErrorOnMultipleDocumentsInTheConfig(t *testing.T) {
	cases := []string{
		`
---
---
`, `
---
overrides:
  '1234':
    ingestion_burst_size: 123
---
overrides:
  '1234':
    ingestion_burst_size: 123
`, `
---
# This is an empty YAML.
---
overrides:
  '1234':
    ingestion_burst_size: 123
`, `
---
overrides:
  '1234':
    ingestion_burst_size: 123
---
# This is an empty YAML.
`,
	}

	for _, tc := range cases {
		actual, err := runtimeConfigLoader{}.load(strings.NewReader(tc))
		assert.Equal(t, errMultipleDocuments, err)
		assert.Nil(t, actual)
	}
}

func TestLoad_ShouldNotErrorWithCertainTarget(t *testing.T) {

	tests := []struct {
		desc             string
		target           []string
		shardByAllLabels bool
		isErr            bool
	}{
		{
			desc:             "all",
			target:           []string{All},
			shardByAllLabels: true,
		},
		{
			desc:             "all, shardByAllLabels:false",
			target:           []string{All},
			shardByAllLabels: false,
			isErr:            true,
		},
		{
			desc:             "distributor",
			target:           []string{Distributor},
			shardByAllLabels: true,
		},
		{
			desc:             "distributor, shardByAllLabels:false",
			target:           []string{Distributor},
			shardByAllLabels: false,
			isErr:            true,
		},
		{
			desc:             "querier",
			target:           []string{Querier},
			shardByAllLabels: true,
		},
		{
			desc:             "querier, shardByAllLabels:false",
			target:           []string{Querier},
			shardByAllLabels: false,
			isErr:            true,
		},
		{
			desc:             "ruler",
			target:           []string{Ruler},
			shardByAllLabels: true,
		},
		{
			desc:             "ruler, shardByAllLabels:false",
			target:           []string{Ruler},
			shardByAllLabels: false,
			isErr:            true,
		},
		{
			desc:   "ingester",
			target: []string{Ingester},
		},
		{
			desc:   "query frontend",
			target: []string{QueryFrontend},
		},
		{
			desc:   "alertmanager",
			target: []string{AlertManager},
		},
		{
			desc:   "store gateway",
			target: []string{StoreGateway},
		},
		{
			desc:   "compactor",
			target: []string{Compactor},
		},
		{
			desc:   "overrides exporter",
			target: []string{OverridesExporter},
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			yamlFile := strings.NewReader(`
overrides:
  'user-1':
    max_global_series_per_user: 15000
`)

			loader := runtimeConfigLoader{}
			loader.cfg = Config{
				Target:      tc.target,
				Distributor: distributor.Config{ShardByAllLabels: tc.shardByAllLabels},
				Ingester:    ingester.Config{ActiveSeriesMetricsEnabled: true},
			}

			_, err := loader.load(yamlFile)
			if tc.isErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
