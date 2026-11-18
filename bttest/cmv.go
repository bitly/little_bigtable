package bttest

import (
	"log"
	"strings"
	"sync"
)

// CMVConfig defines a Continuous Materialized View for the emulator.
// CMVs are created via the CreateMaterializedView gRPC method.
type CMVConfig struct {
	// SourceTable is the Bigtable table ID that feeds this CMV.
	SourceTable string `json:"source_table"`
	// ViewID is the materialized view ID (used as the table name for reads).
	ViewID string `json:"view_id"`
	// KeySeparator is the delimiter used in the source table's composite row key
	// (used only to split the source key).
	KeySeparator string `json:"key_separator"`
	// KeyMapping defines how source key components map to CMV key components.
	// Each entry is the 0-based index into the SPLIT result of the source key.
	// The CMV row key is built from the mapped components using prod Bigtable's
	// multi-column encoding (see transformKey), not by joining with KeySeparator.
	// Example: [3,4,1,2,0] means CMV key = source[3] \x00\x01 source[4] \x00\x01 source[1] ...
	KeyMapping []int `json:"key_mapping"`
	// IncludeFamilies lists the column families to carry from source to CMV.
	// An empty list means all families are included.
	IncludeFamilies []string `json:"include_families,omitempty"`
	// AppendSourceKey appends the original source row key as the final component.
	AppendSourceKey bool `json:"append_source_key,omitempty"`
}

// cmvRegistry maps plain source table IDs to CMV definitions.
// Lookups match by table ID suffix against fully-qualified table names.
// Its own mu protects concurrent reads/writes to configs independently of s.mu.
type cmvRegistry struct {
	mu      sync.RWMutex
	configs map[string][]CMVConfig
}

func newCMVRegistry() *cmvRegistry {
	return &cmvRegistry{
		configs: make(map[string][]CMVConfig),
	}
}

func (r *cmvRegistry) register(cfg CMVConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.configs[cfg.SourceTable] = append(r.configs[cfg.SourceTable], cfg)
}

func (r *cmvRegistry) deregister(viewID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for src, cfgs := range r.configs {
		filtered := cfgs[:0]
		for _, c := range cfgs {
			if c.ViewID != viewID {
				filtered = append(filtered, c)
			}
		}
		if len(filtered) == 0 {
			delete(r.configs, src)
		} else {
			r.configs[src] = filtered
		}
	}
}

// deregisterBySource removes all CMV configs for a given source table and
// returns the view IDs that were registered against it.
func (r *cmvRegistry) deregisterBySource(sourceTable string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	cfgs := r.configs[sourceTable]
	if len(cfgs) == 0 {
		return nil
	}
	viewIDs := make([]string, len(cfgs))
	for i, c := range cfgs {
		viewIDs[i] = c.ViewID
	}
	delete(r.configs, sourceTable)
	return viewIDs
}

func (r *cmvRegistry) cmvsForTable(fqTable string) []*cmvInstance {
	parent, tableID := splitFQTable(fqTable)
	r.mu.RLock()
	cfgs, ok := r.configs[tableID]
	if !ok {
		r.mu.RUnlock()
		return nil
	}
	result := make([]*cmvInstance, len(cfgs))
	for i := range cfgs {
		result[i] = &cmvInstance{config: cfgs[i], parent: parent}
	}
	r.mu.RUnlock()
	return result
}

// splitFQTable splits "projects/p/instances/i/tables/t" into parent and tableID.
func splitFQTable(fqTable string) (parent, tableID string) {
	idx := strings.LastIndex(fqTable, "/tables/")
	if idx < 0 {
		return "", fqTable
	}
	return fqTable[:idx], fqTable[idx+len("/tables/"):]
}

type cmvInstance struct {
	config CMVConfig
	parent string // e.g., projects/p/instances/i
}

// Row key encoding used by prod Bigtable for materialized views with several
// key columns: every string column except the last is terminated with
// cmvKeyTerminator, and a 0x00 byte inside a value is escaped as cmvKeyEscape.
const (
	cmvKeyTerminator = "\x00\x01"
	cmvKeyEscape     = "\x00\xff"
)

// transformKey builds the view row key for a source key. Key columns appear in
// ORDER BY order, encoded as described above; the source key (if selected as a
// key column) is the final, unterminated column.
func (c *cmvInstance) transformKey(sourceKey string) string {
	parts := strings.Split(sourceKey, c.config.KeySeparator)
	cols := make([]string, 0, len(c.config.KeyMapping)+1)
	for _, idx := range c.config.KeyMapping {
		if idx < len(parts) {
			cols = append(cols, parts[idx])
		} else {
			log.Printf("CMV %q: key_mapping index %d out of bounds for source key %q (%d parts) — check your config",
				c.config.ViewID, idx, sourceKey, len(parts))
			cols = append(cols, "")
		}
	}
	if c.config.AppendSourceKey {
		cols = append(cols, sourceKey)
	}
	for i, col := range cols {
		cols[i] = strings.ReplaceAll(col, "\x00", cmvKeyEscape)
	}
	return strings.Join(cols, cmvKeyTerminator)
}

// shouldIncludeFamily returns true for all families when IncludeFamilies is empty.
func (c *cmvInstance) shouldIncludeFamily(famName string) bool {
	if len(c.config.IncludeFamilies) == 0 {
		return true
	}
	for _, f := range c.config.IncludeFamilies {
		if f == famName {
			return true
		}
	}
	return false
}

// buildCMVRow builds a re-keyed CMV row by copying all included families from the source row.
func (c *cmvInstance) buildCMVRow(sourceRow *row) *row {
	newKey := c.transformKey(sourceRow.key)
	cmvRow := newRow(newKey)
	for famName, fam := range sourceRow.families {
		if !c.shouldIncludeFamily(famName) {
			continue
		}
		newFam := &family{
			Name:  famName,
			Order: fam.Order,
			Cells: make(map[string][]cell),
		}
		newFam.ColNames = make([]string, len(fam.ColNames))
		copy(newFam.ColNames, fam.ColNames)
		for col, cells := range fam.Cells {
			newCells := make([]cell, len(cells))
			copy(newCells, cells)
			newFam.Cells[col] = newCells
		}
		cmvRow.families[famName] = newFam
	}
	return cmvRow
}

// deriveCMVKey returns the CMV row key for a given source key.
func (c *cmvInstance) deriveCMVKey(sourceKey string) string {
	return c.transformKey(sourceKey)
}
