package bttest

import (
	"log"
	"sort"
	"strings"
)

// cmvMigrationBatchSize bounds how many rows are held in memory at once while
// scanning or rewriting a table. Batches are required because the SQLite
// connection pool has a single connection: a write cannot be issued from inside
// a row iterator.
const cmvMigrationBatchSize = 1000

// keyColumns returns how many columns make up the view row key.
func (c CMVConfig) keyColumns() int {
	n := len(c.KeyMapping)
	if c.AppendSourceKey {
		n++
	}
	return n
}

// isLegacyKey reports whether a view row key was written by an emulator version
// that joined key columns with the source key separator (for example '#')
// instead of using prod's \x00\x01 terminators.
//
// Only views with two or more key columns can be told apart: every key such a
// view writes today contains at least one terminator. A single-column key has
// no terminator in either format, so it never needs migration.
func (c CMVConfig) isLegacyKey(viewKey string) bool {
	return c.keyColumns() >= 2 && !strings.Contains(viewKey, cmvKeyTerminator)
}

// scanRowBatches calls fn with consecutive batches of rows in key order until
// the table is exhausted or fn returns false. Rows are fully read before fn is
// called, so fn may write to the database.
func scanRowBatches(rows *SqlRows, fn func(batch []*row) bool) {
	start := ""
	for {
		batch := make([]*row, 0, cmvMigrationBatchSize)
		rows.AscendGreaterOrEqual(btreeKey(start), func(i Item) bool {
			batch = append(batch, i.(*row))
			return len(batch) < cmvMigrationBatchSize
		})
		if len(batch) == 0 {
			return
		}
		if !fn(batch) {
			return
		}
		if len(batch) < cmvMigrationBatchSize {
			return
		}
		start = batch[len(batch)-1].key + "\x00"
	}
}

// MigrateLegacyCMVKeys is a one-time startup migration. Earlier emulator
// versions keyed view rows as '#'-joined columns. For every registered view
// whose shadow table still holds such rows, it rebuilds the view from the
// source table using the current key encoding and then removes the old rows.
//
// It needs no version marker: once a view is rebuilt it has no legacy keys, so
// later startups find nothing to do. It is also safe to interrupt, because new
// rows are written before old rows are removed; the next startup detects the
// remaining old rows and rebuilds again.
//
// Call after LoadTables and LoadMaterializedViews, before serving requests.
func (s *server) MigrateLegacyCMVKeys() {
	s.mu.Lock()
	defer s.mu.Unlock()

	names := make([]string, 0, len(s.materializedViews))
	for name := range s.materializedViews {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		cfg, ok := s.cmvs.get(viewIDFromName(name))
		if !ok {
			continue
		}
		parent := strings.TrimSuffix(name, "/materializedViews/"+cfg.ViewID)
		s.migrateLegacyCMVKeys(&cmvInstance{config: cfg, parent: parent})
	}
}

// migrateLegacyCMVKeys migrates a single view. Must be called with s.mu held.
func (s *server) migrateLegacyCMVKeys(cmv *cmvInstance) {
	viewTbl := s.tables[cmv.parent+"/tables/"+cmv.config.ViewID]
	if viewTbl == nil {
		return // never written to, so nothing to migrate
	}

	var legacy []string
	scanRowBatches(viewTbl.rows, func(batch []*row) bool {
		for _, r := range batch {
			if cmv.config.isLegacyKey(r.key) {
				legacy = append(legacy, r.key)
			}
		}
		return true
	})
	if len(legacy) == 0 {
		return
	}

	sourceTbl := s.tables[cmv.parent+"/tables/"+cmv.config.SourceTable]
	if sourceTbl == nil {
		log.Printf("WARNING: CMV %q has %d legacy row keys but source table %q does not exist; cannot rebuild, leaving view unchanged",
			cmv.config.ViewID, len(legacy), cmv.config.SourceTable)
		return
	}

	log.Printf("migrating CMV %q: found %d legacy '#'-joined row keys, rebuilding from source table %q",
		cmv.config.ViewID, len(legacy), cmv.config.SourceTable)

	rebuilt := 0
	scanRowBatches(sourceTbl.rows, func(batch []*row) bool {
		for _, src := range batch {
			viewTbl.rows.ReplaceOrInsert(cmv.buildCMVRow(src))
			rebuilt++
		}
		return true
	})
	for _, key := range legacy {
		viewTbl.rows.Delete(btreeKey(key))
	}
	log.Printf("migrated CMV %q: wrote %d rows, removed %d legacy rows", cmv.config.ViewID, rebuilt, len(legacy))
}
