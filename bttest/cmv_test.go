package bttest

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	btapb "cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func newTestServerWithCMV(t *testing.T, configs []CMVConfig) *server {
	dbFilename := newDBFile(t)
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?cache=shared", dbFilename))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	CreateTables(context.Background(), db)

	s := &server{
		tables:            make(map[string]*table),
		materializedViews: make(map[string]*btapb.MaterializedView),
		db:                db,
		tableBackend:      NewSqlTables(db),
		mvBackend:         NewSqlMaterializedViews(db),
		cmvs:              newCMVRegistry(),
	}
	for _, cfg := range configs {
		s.cmvs.register(cfg)
	}
	return s
}

func TestCMVTransformKey(t *testing.T) {
	// key_mapping [2,3,1,0] + append_source_key:
	// CMV key = parts[2]#parts[3]#parts[1]#parts[0]#<full_source_key>
	inst := &cmvInstance{
		config: CMVConfig{
			SourceTable:     "sensor_readings",
			ViewID:          "readings_by_region",
			KeySeparator:    "#",
			KeyMapping:      []int{2, 3, 1, 0},
			AppendSourceKey: true,
		},
	}

	sourceKey := "device-1#ts-100#us-east#user-42"
	got := inst.transformKey(sourceKey)
	// parts: [0]device-1 [1]ts-100 [2]us-east [3]user-42
	// mapped: parts[2]#parts[3]#parts[1]#parts[0] + full source key
	want := "us-east\x00\x01user-42\x00\x01ts-100\x00\x01device-1\x00\x01device-1#ts-100#us-east#user-42"
	assert.Equal(t, want, got)
}

func TestCMVTransformKeyEscapesNUL(t *testing.T) {
	inst := &cmvInstance{
		config: CMVConfig{
			SourceTable:  "t",
			ViewID:       "v",
			KeySeparator: "#",
			KeyMapping:   []int{1, 0},
		},
	}
	got := inst.transformKey("a\x00b#c\x00")
	assert.Equal(t, "c\x00\xff\x00\x01a\x00\xffb", got)
}

func TestCMVTransformKeyNoAppend(t *testing.T) {
	inst := &cmvInstance{
		config: CMVConfig{
			SourceTable:     "test_table",
			ViewID:          "test_cmv",
			KeySeparator:    "#",
			KeyMapping:      []int{2, 0, 1},
			AppendSourceKey: false,
		},
	}

	sourceKey := "a#b#c"
	got := inst.transformKey(sourceKey)
	assert.Equal(t, "c\x00\x01a\x00\x01b", got)
}

func TestCMVWriteSync(t *testing.T) {
	ctx := context.Background()
	configs := []CMVConfig{{
		SourceTable:     "src_table",
		ViewID:          "src_table_by_other",
		KeySeparator:    "#",
		KeyMapping:      []int{1, 0},
		IncludeFamilies: []string{"cf1"},
		AppendSourceKey: false,
	}}
	s := newTestServerWithCMV(t, configs)

	parent := "projects/test/instances/test"

	// Create source table with column family cf1.
	_, err := s.CreateTable(ctx, &btapb.CreateTableRequest{
		Parent:  parent,
		TableId: "src_table",
		Table: &btapb.Table{
			ColumnFamilies: map[string]*btapb.ColumnFamily{
				"cf1": {},
			},
		},
	})
	require.NoError(t, err)

	fqSrc := parent + "/tables/src_table"
	fqCMV := parent + "/tables/src_table_by_other"

	// Write a row to the source table.
	_, err = s.MutateRow(ctx, &btpb.MutateRowRequest{
		TableName: fqSrc,
		RowKey:    []byte("alpha#beta"),
		Mutations: []*btpb.Mutation{{
			Mutation: &btpb.Mutation_SetCell_{
				SetCell: &btpb.Mutation_SetCell{
					FamilyName:      "cf1",
					ColumnQualifier: []byte("col"),
					TimestampMicros: 1000,
					Value:           []byte("hello"),
				},
			},
		}},
	})
	require.NoError(t, err)

	// Verify CMV shadow table was created and contains the re-keyed row.
	s.mu.Lock()
	cmvTbl, ok := s.tables[fqCMV]
	s.mu.Unlock()
	require.True(t, ok, "CMV shadow table should be created")

	// The CMV key should be beta,alpha (mapping [1, 0]) in prod's encoding.
	wantKey := "beta\x00\x01alpha"
	cmvRow := cmvTbl.rows.Get(btreeKey(wantKey))
	require.NotNil(t, cmvRow)
	r := cmvRow.(*row)
	assert.Equal(t, wantKey, r.key)
	assert.Contains(t, r.families, "cf1")
	assert.Contains(t, r.families["cf1"].Cells, "col")
	assert.Equal(t, []byte("hello"), r.families["cf1"].Cells["col"][0].Value)
}

// TestCMVMultiColumnKeyEncoding creates a view from SQL with several key
// columns, writes a source row, and reads the view row back over ReadRows to
// check the exact key bytes match prod Bigtable's encoding.
func TestCMVMultiColumnKeyEncoding(t *testing.T) {
	ctx := context.Background()
	cfg, err := ParseCMVConfigFromSQL("src_by_foo", "SELECT\n"+
		"  SPLIT(_key, '#')[SAFE_OFFSET(1)] AS foo,\n"+
		"  SPLIT(_key, '#')[SAFE_OFFSET(2)] AS bar,\n"+
		"  SPLIT(_key, '#')[SAFE_OFFSET(0)] AS baz,\n"+
		"  _key AS src_key,\n"+
		"  cf1 AS cf1\n"+
		"FROM `src_table`\n"+
		"ORDER BY foo, bar, baz, src_key")
	require.NoError(t, err)
	s := newTestServerWithCMV(t, []CMVConfig{*cfg})

	parent := "projects/test/instances/test"
	fqSrc := parent + "/tables/src_table"
	fqView := parent + "/tables/src_by_foo"

	_, err = s.CreateTable(ctx, &btapb.CreateTableRequest{
		Parent:  parent,
		TableId: "src_table",
		Table: &btapb.Table{
			ColumnFamilies: map[string]*btapb.ColumnFamily{"cf1": {}},
		},
	})
	require.NoError(t, err)

	_, err = s.MutateRow(ctx, &btpb.MutateRowRequest{
		TableName: fqSrc,
		RowKey:    []byte("ccc#aaa#bbb"),
		Mutations: []*btpb.Mutation{{
			Mutation: &btpb.Mutation_SetCell_{
				SetCell: &btpb.Mutation_SetCell{
					FamilyName:      "cf1",
					ColumnQualifier: []byte("c"),
					TimestampMicros: 1000,
					Value:           []byte("v"),
				},
			},
		}},
	})
	require.NoError(t, err)

	mock := &MockReadRowsServer{}
	require.NoError(t, s.ReadRows(&btpb.ReadRowsRequest{TableName: fqView}, mock))

	var keys [][]byte
	for _, resp := range mock.responses {
		for _, c := range resp.Chunks {
			if len(c.RowKey) > 0 {
				keys = append(keys, c.RowKey)
			}
		}
	}
	require.Len(t, keys, 1)
	assert.Equal(t, []byte("aaa\x00\x01bbb\x00\x01ccc\x00\x01ccc#aaa#bbb"), keys[0])
}

func TestCMVDeleteSync(t *testing.T) {
	ctx := context.Background()
	configs := []CMVConfig{{
		SourceTable:  "src_table",
		ViewID:       "src_cmv",
		KeySeparator: "#",
		KeyMapping:   []int{1, 0},
	}}
	s := newTestServerWithCMV(t, configs)

	parent := "projects/test/instances/test"
	fqSrc := parent + "/tables/src_table"
	fqCMV := parent + "/tables/src_cmv"

	_, err := s.CreateTable(ctx, &btapb.CreateTableRequest{
		Parent:  parent,
		TableId: "src_table",
		Table: &btapb.Table{
			ColumnFamilies: map[string]*btapb.ColumnFamily{
				"cf1": {},
			},
		},
	})
	require.NoError(t, err)

	// Write a row.
	_, err = s.MutateRow(ctx, &btpb.MutateRowRequest{
		TableName: fqSrc,
		RowKey:    []byte("x#y"),
		Mutations: []*btpb.Mutation{{
			Mutation: &btpb.Mutation_SetCell_{
				SetCell: &btpb.Mutation_SetCell{
					FamilyName:      "cf1",
					ColumnQualifier: []byte("c"),
					TimestampMicros: 1000,
					Value:           []byte("v"),
				},
			},
		}},
	})
	require.NoError(t, err)

	// Confirm CMV has the row.
	s.mu.Lock()
	cmvTbl := s.tables[fqCMV]
	s.mu.Unlock()
	require.NotNil(t, cmvTbl)
	assert.Equal(t, 1, cmvTbl.rows.Len())

	// Delete the row from source.
	_, err = s.MutateRow(ctx, &btpb.MutateRowRequest{
		TableName: fqSrc,
		RowKey:    []byte("x#y"),
		Mutations: []*btpb.Mutation{{
			Mutation: &btpb.Mutation_DeleteFromRow_{
				DeleteFromRow: &btpb.Mutation_DeleteFromRow{},
			},
		}},
	})
	require.NoError(t, err)

	// CMV row should be gone.
	assert.Equal(t, 0, cmvTbl.rows.Len())
}

func TestCMVDropRowRangeAll(t *testing.T) {
	ctx := context.Background()
	configs := []CMVConfig{{
		SourceTable:  "src_table",
		ViewID:       "src_cmv",
		KeySeparator: "#",
		KeyMapping:   []int{1, 0},
	}}
	s := newTestServerWithCMV(t, configs)

	parent := "projects/test/instances/test"
	fqSrc := parent + "/tables/src_table"
	fqCMV := parent + "/tables/src_cmv"

	_, err := s.CreateTable(ctx, &btapb.CreateTableRequest{
		Parent:  parent,
		TableId: "src_table",
		Table: &btapb.Table{
			ColumnFamilies: map[string]*btapb.ColumnFamily{
				"cf1": {},
			},
		},
	})
	require.NoError(t, err)

	// Write multiple rows.
	for _, key := range []string{"a#b", "c#d", "e#f"} {
		_, err = s.MutateRow(ctx, &btpb.MutateRowRequest{
			TableName: fqSrc,
			RowKey:    []byte(key),
			Mutations: []*btpb.Mutation{{
				Mutation: &btpb.Mutation_SetCell_{
					SetCell: &btpb.Mutation_SetCell{
						FamilyName:      "cf1",
						ColumnQualifier: []byte("c"),
						TimestampMicros: 1000,
						Value:           []byte("v"),
					},
				},
			}},
		})
		require.NoError(t, err)
	}

	s.mu.Lock()
	cmvTbl := s.tables[fqCMV]
	s.mu.Unlock()
	require.NotNil(t, cmvTbl)
	assert.Equal(t, 3, cmvTbl.rows.Len())

	// Drop all rows from source.
	_, err = s.DropRowRange(ctx, &btapb.DropRowRangeRequest{
		Name:   fqSrc,
		Target: &btapb.DropRowRangeRequest_DeleteAllDataFromTable{DeleteAllDataFromTable: true},
	})
	require.NoError(t, err)

	assert.Equal(t, 0, cmvTbl.rows.Len())
}

func TestCMVTransformKeyOutOfBounds(t *testing.T) {
	inst := &cmvInstance{
		config: CMVConfig{
			SourceTable:  "my_table",
			ViewID:       "my_view",
			KeySeparator: "#",
			KeyMapping:   []int{0, 99}, // index 99 is out of bounds
		},
	}
	// Should not panic; out-of-bounds index produces an empty component.
	got := inst.transformKey("only#two#parts")
	assert.Equal(t, "only\x00\x01", got)
}

func TestCreateMaterializedViewRPC(t *testing.T) {
	ctx := context.Background()
	dbFilename := newDBFile(t)
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?cache=shared", dbFilename))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	CreateTables(ctx, db)

	s := &server{
		tables:            make(map[string]*table),
		materializedViews: make(map[string]*btapb.MaterializedView),
		db:                db,
		tableBackend:      NewSqlTables(db),
		mvBackend:         NewSqlMaterializedViews(db),
		cmvs:              newCMVRegistry(),
	}

	parent := "projects/test/instances/test"
	mvSQL := "SELECT\n" +
		"  SPLIT(_key, '#')[SAFE_OFFSET(2)] AS region,\n" +
		"  SPLIT(_key, '#')[SAFE_OFFSET(3)] AS user_id,\n" +
		"  SPLIT(_key, '#')[SAFE_OFFSET(1)] AS ts,\n" +
		"  SPLIT(_key, '#')[SAFE_OFFSET(0)] AS device_id,\n" +
		"  _key AS src_key,\n" +
		"  data AS data\n" +
		"FROM `sensor_readings`\n" +
		"ORDER BY region, user_id, ts, device_id, src_key"

	op, err := s.CreateMaterializedView(ctx, &btapb.CreateMaterializedViewRequest{
		Parent:             parent,
		MaterializedViewId: "readings_by_region",
		MaterializedView:   &btapb.MaterializedView{Query: mvSQL},
	})
	require.NoError(t, err)
	assert.True(t, op.Done)

	mv, err := s.GetMaterializedView(ctx, &btapb.GetMaterializedViewRequest{
		Name: parent + "/materializedViews/readings_by_region",
	})
	require.NoError(t, err)
	assert.Equal(t, "readings_by_region", mv.Name[strings.LastIndex(mv.Name, "/")+1:])
	assert.Equal(t, mvSQL, mv.Query)

	list, err := s.ListMaterializedViews(ctx, &btapb.ListMaterializedViewsRequest{Parent: parent})
	require.NoError(t, err)
	assert.Len(t, list.MaterializedViews, 1)

	// CMV should fire on writes to the source table.
	_, err = s.CreateTable(ctx, &btapb.CreateTableRequest{
		Parent:  parent,
		TableId: "sensor_readings",
		Table:   &btapb.Table{ColumnFamilies: map[string]*btapb.ColumnFamily{"data": {}}},
	})
	require.NoError(t, err)

	_, err = s.MutateRow(ctx, &btpb.MutateRowRequest{
		TableName: parent + "/tables/sensor_readings",
		RowKey:    []byte("device-1#ts-100#us-east#user-42"),
		Mutations: []*btpb.Mutation{{
			Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
				FamilyName: "data", ColumnQualifier: []byte("temp"), Value: []byte("72"),
			}},
		}},
	})
	require.NoError(t, err)

	fqCMV := parent + "/tables/readings_by_region"
	cmvTbl := s.tables[fqCMV]
	require.NotNil(t, cmvTbl, "CMV shadow table should have been auto-created")

	// DeleteMaterializedView should remove the view.
	_, err = s.DeleteMaterializedView(ctx, &btapb.DeleteMaterializedViewRequest{
		Name: parent + "/materializedViews/readings_by_region",
	})
	require.NoError(t, err)
	list, err = s.ListMaterializedViews(ctx, &btapb.ListMaterializedViewsRequest{Parent: parent})
	require.NoError(t, err)
	assert.Len(t, list.MaterializedViews, 0)
}

func TestCMVDropRowRangePrefix(t *testing.T) {
	ctx := context.Background()
	// Key mapping [1,0]: CMV key = source[1]#source[0] (swap two components).
	configs := []CMVConfig{{
		SourceTable:  "src_table",
		ViewID:       "src_cmv",
		KeySeparator: "#",
		KeyMapping:   []int{1, 0},
	}}
	s := newTestServerWithCMV(t, configs)

	parent := "projects/test/instances/test"
	fqSrc := parent + "/tables/src_table"
	fqCMV := parent + "/tables/src_cmv"

	_, err := s.CreateTable(ctx, &btapb.CreateTableRequest{
		Parent:  parent,
		TableId: "src_table",
		Table: &btapb.Table{
			ColumnFamilies: map[string]*btapb.ColumnFamily{
				"cf1": {},
			},
		},
	})
	require.NoError(t, err)

	// Write three rows: two share the prefix "alpha#" and one does not.
	for _, key := range []string{"alpha#one", "alpha#two", "beta#three"} {
		_, err = s.MutateRow(ctx, &btpb.MutateRowRequest{
			TableName: fqSrc,
			RowKey:    []byte(key),
			Mutations: []*btpb.Mutation{{
				Mutation: &btpb.Mutation_SetCell_{
					SetCell: &btpb.Mutation_SetCell{
						FamilyName:      "cf1",
						ColumnQualifier: []byte("c"),
						TimestampMicros: 1000,
						Value:           []byte("v"),
					},
				},
			}},
		})
		require.NoError(t, err)
	}

	s.mu.Lock()
	cmvTbl := s.tables[fqCMV]
	s.mu.Unlock()
	require.NotNil(t, cmvTbl)
	assert.Equal(t, 3, cmvTbl.rows.Len())

	// Drop source rows with prefix "alpha#".
	_, err = s.DropRowRange(ctx, &btapb.DropRowRangeRequest{
		Name:   fqSrc,
		Target: &btapb.DropRowRangeRequest_RowKeyPrefix{RowKeyPrefix: []byte("alpha#")},
	})
	require.NoError(t, err)

	// CMV should now have only 1 row: "three\x00\x01beta" (from "beta#three").
	assert.Equal(t, 1, cmvTbl.rows.Len())
	// The remaining CMV row should be the transformed "beta#three" → "three\x00\x01beta".
	cmvRow := cmvTbl.rows.Get(btreeKey("three\x00\x01beta"))
	assert.NotNil(t, cmvRow, "CMV row for non-deleted source should still exist")
}

func newTestInstanceServer(t *testing.T) (*server, string) {
	t.Helper()
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	CreateTables(ctx, db)
	s := &server{
		tables:            make(map[string]*table),
		materializedViews: make(map[string]*btapb.MaterializedView),
		db:                db,
		tableBackend:      NewSqlTables(db),
		mvBackend:         NewSqlMaterializedViews(db),
		cmvs:              newCMVRegistry(),
	}
	return s, "projects/test/instances/test"
}

func TestDeletionProtection_BlocksDelete(t *testing.T) {
	ctx := context.Background()
	s, parent := newTestInstanceServer(t)

	mvSQL := "SELECT SPLIT(_key, '#')[SAFE_OFFSET(0)] AS a FROM `src` ORDER BY a"
	_, err := s.CreateMaterializedView(ctx, &btapb.CreateMaterializedViewRequest{
		Parent:             parent,
		MaterializedViewId: "protected_view",
		MaterializedView:   &btapb.MaterializedView{Query: mvSQL, DeletionProtection: true},
	})
	require.NoError(t, err)

	// Delete should fail while DeletionProtection is enabled.
	_, err = s.DeleteMaterializedView(ctx, &btapb.DeleteMaterializedViewRequest{
		Name: parent + "/materializedViews/protected_view",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "protected against deletion")

	// Verify the view still exists.
	mv, err := s.GetMaterializedView(ctx, &btapb.GetMaterializedViewRequest{
		Name: parent + "/materializedViews/protected_view",
	})
	require.NoError(t, err)
	assert.True(t, mv.DeletionProtection)
}

func TestDeletionProtection_UpdateThenDelete(t *testing.T) {
	ctx := context.Background()
	s, parent := newTestInstanceServer(t)

	mvSQL := "SELECT SPLIT(_key, '#')[SAFE_OFFSET(0)] AS a FROM `src` ORDER BY a"
	_, err := s.CreateMaterializedView(ctx, &btapb.CreateMaterializedViewRequest{
		Parent:             parent,
		MaterializedViewId: "protected_view",
		MaterializedView:   &btapb.MaterializedView{Query: mvSQL, DeletionProtection: true},
	})
	require.NoError(t, err)

	// Unprotect via UpdateMaterializedView.
	_, err = s.UpdateMaterializedView(ctx, &btapb.UpdateMaterializedViewRequest{
		MaterializedView: &btapb.MaterializedView{
			Name:               parent + "/materializedViews/protected_view",
			DeletionProtection: false,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"deletion_protection"}},
	})
	require.NoError(t, err)

	// Confirm the flag was cleared.
	mv, err := s.GetMaterializedView(ctx, &btapb.GetMaterializedViewRequest{
		Name: parent + "/materializedViews/protected_view",
	})
	require.NoError(t, err)
	assert.False(t, mv.DeletionProtection)

	// Delete should now succeed.
	_, err = s.DeleteMaterializedView(ctx, &btapb.DeleteMaterializedViewRequest{
		Name: parent + "/materializedViews/protected_view",
	})
	require.NoError(t, err)
}

func cmvViewKeys(t *testing.T, tbl *table) []string {
	t.Helper()
	var keys []string
	tbl.rows.Ascend(func(i Item) bool {
		keys = append(keys, i.(*row).key)
		return true
	})
	return keys
}

func putTestCell(t *testing.T, s *server, fqTable, key string) {
	t.Helper()
	_, err := s.MutateRow(context.Background(), &btpb.MutateRowRequest{
		TableName: fqTable,
		RowKey:    []byte(key),
		Mutations: []*btpb.Mutation{{
			Mutation: &btpb.Mutation_SetCell_{
				SetCell: &btpb.Mutation_SetCell{
					FamilyName:      "cf1",
					ColumnQualifier: []byte("c"),
					TimestampMicros: 1000,
					Value:           []byte("v:" + key),
				},
			},
		}},
	})
	require.NoError(t, err)
}

func TestCMVMigrateLegacyKeys(t *testing.T) {
	ctx := context.Background()
	cfg := CMVConfig{
		SourceTable:     "src_table",
		ViewID:          "src_by_b",
		KeySeparator:    "#",
		KeyMapping:      []int{1, 0},
		AppendSourceKey: true,
	}
	s := newTestServerWithCMV(t, []CMVConfig{cfg})
	parent := "projects/test/instances/test"
	fqSrc := parent + "/tables/src_table"
	fqView := parent + "/tables/src_by_b"
	s.materializedViews[parent+"/materializedViews/src_by_b"] = &btapb.MaterializedView{}

	_, err := s.CreateTable(ctx, &btapb.CreateTableRequest{
		Parent:  parent,
		TableId: "src_table",
		Table:   &btapb.Table{ColumnFamilies: map[string]*btapb.ColumnFamily{"cf1": {}}},
	})
	require.NoError(t, err)

	// Rows written under the current encoding (also creates the view table).
	putTestCell(t, s, fqSrc, "a1#b1")
	putTestCell(t, s, fqSrc, "a2#b2")
	viewTbl := s.tables[fqView]
	require.NotNil(t, viewTbl)

	// Simulate a database written by an older version: drop the new rows and
	// store '#'-joined keys, including one for a source row that no longer exists.
	viewTbl.rows.DeleteAll()
	for _, k := range []string{"b1#a1#a1#b1", "b2#a2#a2#b2", "bX#aX#aX#bX"} {
		viewTbl.rows.ReplaceOrInsert(&row{key: k, families: map[string]*family{}})
	}

	s.MigrateLegacyCMVKeys()

	assert.Equal(t, []string{
		"b1\x00\x01a1\x00\x01a1#b1",
		"b2\x00\x01a2\x00\x01a2#b2",
	}, cmvViewKeys(t, viewTbl))

	// Rebuilt rows carry the source data.
	got := viewTbl.rows.Get(btreeKey("b2\x00\x01a2\x00\x01a2#b2")).(*row)
	require.Contains(t, got.families, "cf1")
	assert.Equal(t, []byte("v:a2#b2"), got.families["cf1"].Cells["c"][0].Value)

	// A second run finds nothing to migrate.
	s.MigrateLegacyCMVKeys()
	assert.Len(t, cmvViewKeys(t, viewTbl), 2)
}

func TestCMVMigrateLegacyKeysBatches(t *testing.T) {
	ctx := context.Background()
	cfg := CMVConfig{SourceTable: "src_table", ViewID: "v", KeySeparator: "#", KeyMapping: []int{1, 0}}
	s := newTestServerWithCMV(t, []CMVConfig{cfg})
	parent := "projects/test/instances/test"
	s.materializedViews[parent+"/materializedViews/v"] = &btapb.MaterializedView{}

	_, err := s.CreateTable(ctx, &btapb.CreateTableRequest{
		Parent:  parent,
		TableId: "src_table",
		Table:   &btapb.Table{ColumnFamilies: map[string]*btapb.ColumnFamily{"cf1": {}}},
	})
	require.NoError(t, err)

	n := cmvMigrationBatchSize*2 + 7
	putTestCell(t, s, parent+"/tables/src_table", "seed#seed")
	viewTbl := s.tables[parent+"/tables/v"]
	viewTbl.rows.DeleteAll()
	src := s.tables[parent+"/tables/src_table"]
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("a%05d#b%05d", i, i)
		src.rows.ReplaceOrInsert(&row{key: k, families: map[string]*family{}})
		viewTbl.rows.ReplaceOrInsert(&row{key: fmt.Sprintf("b%05d#a%05d", i, i), families: map[string]*family{}})
	}

	s.MigrateLegacyCMVKeys()

	keys := cmvViewKeys(t, viewTbl)
	// n generated rows plus the seed row.
	require.Len(t, keys, n+1)
	for _, k := range keys {
		assert.Contains(t, k, "\x00\x01")
	}
}

func TestCMVMigrateSkipsSingleColumnViews(t *testing.T) {
	ctx := context.Background()
	cfg := CMVConfig{SourceTable: "src_table", ViewID: "v", KeySeparator: "#", KeyMapping: []int{1}}
	s := newTestServerWithCMV(t, []CMVConfig{cfg})
	parent := "projects/test/instances/test"
	s.materializedViews[parent+"/materializedViews/v"] = &btapb.MaterializedView{}

	_, err := s.CreateTable(ctx, &btapb.CreateTableRequest{
		Parent:  parent,
		TableId: "src_table",
		Table:   &btapb.Table{ColumnFamilies: map[string]*btapb.ColumnFamily{"cf1": {}}},
	})
	require.NoError(t, err)
	putTestCell(t, s, parent+"/tables/src_table", "a#b")
	putTestCell(t, s, parent+"/tables/src_table", "c#d")
	viewTbl := s.tables[parent+"/tables/v"]
	viewTbl.rows.Delete(btreeKey("d"))

	s.MigrateLegacyCMVKeys()

	// Single-column keys are identical in both formats; nothing is rebuilt.
	assert.Equal(t, []string{"b"}, cmvViewKeys(t, viewTbl))
}
