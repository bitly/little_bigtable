package bttest

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCMVConfigFromSQL(t *testing.T) {
	tests := []struct {
		query     string
		expectErr string
		expectCfg *CMVConfig
	}{
		{
			query: `SELECT
  SPLIT(_key, '#')[SAFE_OFFSET(2)] AS region,
  SPLIT(_key, '#')[SAFE_OFFSET(3)] AS user_id,
  SPLIT(_key, '#')[SAFE_OFFSET(1)] AS ts,
  SPLIT(_key, '#')[SAFE_OFFSET(0)] AS device_id,
  _key AS src_key,
  info AS info,
  stats AS stats
FROM ` + "`sensor_readings`" + `
ORDER BY region, user_id, ts, device_id, src_key`,
			expectCfg: &CMVConfig{
				SourceTable:     "sensor_readings",
				KeySeparator:    "#",
				KeyMapping:      []int{2, 3, 1, 0},
				IncludeFamilies: []string{"info", "stats"},
				AppendSourceKey: true,
			},
		},
		{
			query: `SELECT
  SPLIT(_key, '#')[SAFE_OFFSET(2)] AS c,
  SPLIT(_key, '#')[SAFE_OFFSET(0)] AS a,
  SPLIT(_key, '#')[SAFE_OFFSET(1)] AS b,
  cf1 AS cf1
FROM ` + "`my_table`" + `
ORDER BY c, a, b`,
			expectCfg: &CMVConfig{
				SourceTable:     "my_table",
				KeySeparator:    "#",
				KeyMapping:      []int{2, 0, 1},
				IncludeFamilies: []string{"cf1"},
			},
		},
		{
			query: `SELECT
  SPLIT(_key, '#')[SAFE_OFFSET(1)] AS b,
  SPLIT(_key, '#')[SAFE_OFFSET(0)] AS a,
  meta AS meta,
  logs AS logs,
  tags AS tags,
  raw AS raw
FROM ` + "`source`" + `
ORDER BY b, a`,
			expectCfg: &CMVConfig{
				SourceTable:     "source",
				KeySeparator:    "#",
				KeyMapping:      []int{1, 0},
				IncludeFamilies: []string{"meta", "logs", "tags", "raw"},
			},
		},
		{
			query:     `SELECT SPLIT(_key, '#')[SAFE_OFFSET(0)] AS a ORDER BY a`,
			expectErr: "FROM",
		},
		{
			query:     "SELECT a FROM `t` ORDER BY a",
			expectErr: "SPLIT",
		},
		{
			query:     "SELECT SPLIT(_key, '#')[SAFE_OFFSET(0)] AS a FROM `t`",
			expectErr: "ORDER BY",
		},
		{
			query: `SELECT
  SPLIT(_key, '#')[SAFE_OFFSET(0)] AS a
FROM ` + "`t`" + `
ORDER BY a, unknown_col`,
			expectErr: "unknown_col",
		},
		{
			query: `SELECT
  SPLIT(_key, '|')[SAFE_OFFSET(1)] AS b,
  SPLIT(_key, '|')[SAFE_OFFSET(0)] AS a,
  cf AS cf
FROM ` + "`t`" + `
ORDER BY b, a`,
			expectCfg: &CMVConfig{
				SourceTable:     "t",
				KeySeparator:    "|",
				KeyMapping:      []int{1, 0},
				IncludeFamilies: []string{"cf"},
			},
		},
		{
			// GROUP BY (aggregation) queries are not supported — the emulator only handles
			// ORDER BY (key re-mapping) CMVs. Aggregation requires maintaining running state
			// across writes, which is a fundamentally different execution model.
			query:     "SELECT _key, count(fam1['col1']) as count FROM `t1` GROUP BY _key",
			expectErr: "GROUP BY",
		},
	}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			cfg, err := ParseCMVConfigFromSQL("", tc.query)

			if tc.expectErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectErr)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, tc.expectCfg)
			require.NotNil(t, cfg)
			assert.Equal(t, *tc.expectCfg, *cfg)
		})
	}
}
