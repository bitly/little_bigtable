// Copyright 2016 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bttest

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	btapb "cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/protobuf/field_mask"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestMain(m *testing.M) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	os.Exit(m.Run())
}

// newDBFile returns an unused uniqute temp filename
func newDBFile(t *testing.T) string {
	f, err := os.CreateTemp("", "little_bigtable*.db")
	if err != nil {
		t.Fatal(err)
	}
	fn := f.Name()
	f.Close()
	err = os.Remove(fn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(fn) })
	return fn
}

func newTestServer(t *testing.T) *server {
	dbFilename := newDBFile(t)
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared", dbFilename))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	CreateTables(context.Background(), db)

	s := &server{
		tables:       make(map[string]*table),
		db:           db,
		tableBackend: NewSqlTables(db),
	}
	return s
}

func TestConcurrentMutationsReadModifyAndGC(t *testing.T) {
	s := newTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := s.CreateTable(
		ctx,
		&btapb.CreateTableRequest{Parent: "cluster", TableId: "t"}); err != nil {
		t.Fatal(err)
	}
	const name = `cluster/tables/t`
	req := &btapb.ModifyColumnFamiliesRequest{
		Name: name,
		Modifications: []*btapb.ModifyColumnFamiliesRequest_Modification{{
			Id:  "cf",
			Mod: &btapb.ModifyColumnFamiliesRequest_Modification_Create{Create: &btapb.ColumnFamily{}},
		}},
	}
	_, err := s.ModifyColumnFamilies(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	req = &btapb.ModifyColumnFamiliesRequest{
		Name: name,
		Modifications: []*btapb.ModifyColumnFamiliesRequest_Modification{{
			Id: "cf",
			Mod: &btapb.ModifyColumnFamiliesRequest_Modification_Update{Update: &btapb.ColumnFamily{
				GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}},
			}},
		}},
	}
	if _, err := s.ModifyColumnFamilies(ctx, req); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var ts int64
	ms := func() []*btpb.Mutation {
		return []*btpb.Mutation{{
			Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
				FamilyName:      "cf",
				ColumnQualifier: []byte(`col`),
				TimestampMicros: atomic.AddInt64(&ts, 1000),
			}},
		}}
	}

	rmw := func() *btpb.ReadModifyWriteRowRequest {
		return &btpb.ReadModifyWriteRowRequest{
			TableName: name,
			RowKey:    []byte(fmt.Sprint(rand.Intn(100))),
			Rules: []*btpb.ReadModifyWriteRule{{
				FamilyName:      "cf",
				ColumnQualifier: []byte("col"),
				Rule:            &btpb.ReadModifyWriteRule_IncrementAmount{IncrementAmount: 1},
			}},
		}
	}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				req := &btpb.MutateRowRequest{
					TableName: name,
					RowKey:    []byte(fmt.Sprint(rand.Intn(100))),
					Mutations: ms(),
				}
				if _, err := s.MutateRow(ctx, req); err != nil {
					panic(err) // can't use t.Fatal in goroutine
				}
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				_, _ = s.ReadModifyWriteRow(ctx, rmw())
			}
		}()

	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Error("Concurrent mutations and GCs haven't completed after 1s")
	}
}

func TestConcurrentMutations(t *testing.T) {
	// 50 concurrent mutations of different cells on the same row
	// expect all 50 values after
	s := newTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if _, err := s.CreateTable(
		ctx,
		&btapb.CreateTableRequest{Parent: "c", TableId: "t"}); err != nil {
		t.Fatal(err)
	}
	const name = `c/tables/t`
	req := &btapb.ModifyColumnFamiliesRequest{
		Name: name,
		Modifications: []*btapb.ModifyColumnFamiliesRequest_Modification{{
			Id:  "cf",
			Mod: &btapb.ModifyColumnFamiliesRequest_Modification_Create{Create: &btapb.ColumnFamily{}},
		}},
	}
	_, err := s.ModifyColumnFamilies(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	ms := func(i int) []*btpb.Mutation {
		return []*btpb.Mutation{{
			Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
				FamilyName:      "cf",
				ColumnQualifier: []byte(fmt.Sprintf("%d", i)),
				Value:           []byte(fmt.Sprintf("%d", i)),
				TimestampMicros: 1000,
			}},
		}}
	}

	rowKey := []byte("rowkey")
	start := make(chan bool)
	for i := 0; i < 50; i++ {
		i := i
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			for ctx.Err() == nil {
				req := &btpb.MutateRowRequest{
					TableName: name,
					RowKey:    rowKey,
					Mutations: ms(i),
				}
				if _, err := s.MutateRow(ctx, req); err != nil {
					panic(err) // can't use t.Fatal in goroutine
				}
			}
		}(i)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	close(start)
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Error("Concurrent mutations haven't completed after 1s")
	}

	// verify
	mock := &MockReadRowsServer{}
	rreq := &btpb.ReadRowsRequest{TableName: name}
	if err = s.ReadRows(rreq, mock); err != nil {
		t.Fatalf("ReadRows error: %v", err)
	}
	if len(mock.responses) != 1 {
		t.Fatal("Response count: got 0, want 1")
	}
	if len(mock.responses[0].Chunks) != 50 {
		t.Errorf("Chunk count: got %d, want 50", len(mock.responses[0].Chunks))
	}

	var gotChunks []*btpb.ReadRowsResponse_CellChunk
	for _, res := range mock.responses {
		gotChunks = append(gotChunks, res.Chunks...)
	}
	var seen []string
	for i, c := range gotChunks {
		if !bytes.Equal(c.RowKey, rowKey) {
			t.Fatalf("expected row %q got %q", c.RowKey, rowKey)
		}
		if !bytes.Equal(c.Qualifier.Value, c.Value) {
			t.Fatalf("[%d] expected equal got %q %q", i, c.Qualifier.Value, c.Value)
		}
		seen = append(seen, string(c.Qualifier.Value))
	}
	sort.Strings(seen)
	t.Logf("seen %#v", seen)
}

func TestCreateTableResponse(t *testing.T) {
	// We need to ensure that invoking CreateTable returns
	// the  ColumnFamilies as well as Granularity.
	// See issue https://github.com/googleapis/google-cloud-go/issues/1512.
	s := newTestServer(t)
	ctx := context.Background()
	got, err := s.CreateTable(ctx, &btapb.CreateTableRequest{
		Parent:  "projects/issue-1512/instances/instance",
		TableId: "table",
		Table: &btapb.Table{
			ColumnFamilies: map[string]*btapb.ColumnFamily{
				"cf1": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 123}}},
				"cf2": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 456}}},
			},
			AutomatedBackupConfig: getAutomatedBackupConfig(&btapb.Table_AutomatedBackupPolicy{
				Frequency:       durationpb.New(24 * time.Hour),
				RetentionPeriod: durationpb.New(72 * time.Hour),
			}),
		},
	})
	if err != nil {
		t.Fatalf("Creating table: %v", err)
	}

	want := &btapb.Table{
		Name: "projects/issue-1512/instances/instance/tables/table",
		// If no Granularity was specified, we should get back "MILLIS".
		Granularity: btapb.Table_MILLIS,
		ColumnFamilies: map[string]*btapb.ColumnFamily{
			"cf1": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 123}}},
			"cf2": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 456}}},
		},
		AutomatedBackupConfig: &btapb.Table_AutomatedBackupPolicy_{
			AutomatedBackupPolicy: &btapb.Table_AutomatedBackupPolicy{
				Frequency:       durationpb.New(24 * time.Hour),
				RetentionPeriod: durationpb.New(72 * time.Hour),
			},
		},
	}
	require.Equal(t, want, got)
}

func TestCreateTableWithFamily(t *testing.T) {
	// The Go client currently doesn't support creating a table with column families
	// in one operation but it is allowed by the API. This must still be supported by the
	// fake server so this test lives here instead of in the main bigtable
	// integration test.
	s := newTestServer(t)
	ctx := context.Background()
	newTbl := btapb.Table{
		ColumnFamilies: map[string]*btapb.ColumnFamily{
			"cf1": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 123}}},
			"cf2": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 456}}},
		},
	}
	cTbl, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: "cluster", TableId: "t", Table: &newTbl})
	if err != nil {
		t.Fatalf("Creating table: %v", err)
	}
	tbl, err := s.GetTable(ctx, &btapb.GetTableRequest{Name: cTbl.Name})
	if err != nil {
		t.Fatalf("Getting table: %v", err)
	}
	cf := tbl.ColumnFamilies["cf1"]
	if cf == nil {
		t.Fatalf("Missing col family cf1")
	}
	if got, want := cf.GcRule.GetMaxNumVersions(), int32(123); got != want {
		t.Errorf("Invalid MaxNumVersions: wanted:%d, got:%d", want, got)
	}
	cf = tbl.ColumnFamilies["cf2"]
	if cf == nil {
		t.Fatalf("Missing col family cf2")
	}
	if got, want := cf.GcRule.GetMaxNumVersions(), int32(456); got != want {
		t.Errorf("Invalid MaxNumVersions: wanted:%d, got:%d", want, got)
	}
}

func TestUpdateTable(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	createdTbl, err := s.CreateTable(ctx, &btapb.CreateTableRequest{
		Parent:  "projects/issue-1512/instances/instance",
		TableId: "t",
		Table: &btapb.Table{
			ColumnFamilies: map[string]*btapb.ColumnFamily{
				"cf1": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}}},
			},
		},
	})
	if err != nil {
		t.Fatalf("Creating table: %v", err)
	}

	createdBackupPolicy := getAutomatedBackupPolicy(createdTbl)
	require.Nil(t, createdBackupPolicy)

	_, err = s.UpdateTable(ctx, &btapb.UpdateTableRequest{
		Table: &btapb.Table{
			Name: createdTbl.Name,
			ColumnFamilies: map[string]*btapb.ColumnFamily{
				"cf1": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}}},
			},
			AutomatedBackupConfig: getAutomatedBackupConfig(&btapb.Table_AutomatedBackupPolicy{
				Frequency:       durationpb.New(24 * time.Hour),
				RetentionPeriod: durationpb.New(72 * time.Hour),
			}),
		},
		UpdateMask: &field_mask.FieldMask{
			Paths: []string{"automated_backup_policy.retention_period", "automated_backup_policy.frequency"},
		},
	})
	if err != nil {
		t.Fatalf("Updating table: %v", err)
	}

	updatedTbl, err := s.GetTable(ctx, &btapb.GetTableRequest{Name: createdTbl.Name})
	if err != nil {
		t.Fatalf("Getting table: %v", err)
	}
	updatedBackupPolicy := getAutomatedBackupPolicy(updatedTbl)
	require.Equal(t, updatedBackupPolicy.GetRetentionPeriod().Seconds, durationpb.New(72*time.Hour).Seconds)
	require.Equal(t, updatedBackupPolicy.GetFrequency().Seconds, durationpb.New(24*time.Hour).Seconds)
}

type MockSampleRowKeysServer struct {
	responses []*btpb.SampleRowKeysResponse
	grpc.ServerStream
}

func (s *MockSampleRowKeysServer) Send(resp *btpb.SampleRowKeysResponse) error {
	s.responses = append(s.responses, resp)
	return nil
}

func TestSampleRowKeys(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	newTbl := btapb.Table{
		ColumnFamilies: map[string]*btapb.ColumnFamily{
			"cf": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}}},
		},
	}
	tbl, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: "cluster", TableId: "t", Table: &newTbl})
	if err != nil {
		t.Fatalf("Creating table: %v", err)
	}

	// Populate the table
	val := []byte("value")
	rowCount := 1000
	for i := 0; i < rowCount; i++ {
		req := &btpb.MutateRowRequest{
			TableName: tbl.Name,
			RowKey:    []byte("row-" + strconv.Itoa(i)),
			Mutations: []*btpb.Mutation{{
				Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
					FamilyName:      "cf",
					ColumnQualifier: []byte("col"),
					TimestampMicros: 1000,
					Value:           val,
				}},
			}},
		}
		if _, err := s.MutateRow(ctx, req); err != nil {
			t.Fatalf("Populating table: %v", err)
		}
	}
	t.Logf("before SampleRowKeys")

	mock := &MockSampleRowKeysServer{}
	if err := s.SampleRowKeys(&btpb.SampleRowKeysRequest{TableName: tbl.Name}, mock); err != nil {
		t.Errorf("SampleRowKeys error: %v", err)
	}
	if len(mock.responses) == 0 {
		t.Fatal("Response count: got 0, want > 0")
	}
	// Make sure the offset of the final response is the offset of the final row
	got := mock.responses[len(mock.responses)-1].OffsetBytes
	want := int64((rowCount - 1) * len(val))
	if got != want {
		t.Errorf("Invalid offset: got %d, want %d", got, want)
	}
}

type AntagonistFunction func(s *server, attempts int, tblName string, finished chan (bool))

func SampleRowKeysConcurrentTest(t *testing.T, antagonist AntagonistFunction) {
	s := newTestServer(t)
	ctx := context.Background()
	newTbl := btapb.Table{
		ColumnFamilies: map[string]*btapb.ColumnFamily{
			"cf": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}}},
		},
	}
	tbl, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: "cluster", TableId: "t", Table: &newTbl})
	if err != nil {
		t.Fatalf("Creating table: %v", err)
	}

	// Populate the table
	populate := func() {
		rowCount := 100
		for i := 0; i < rowCount; i++ {
			req := &btpb.MutateRowRequest{
				TableName: tbl.Name,
				RowKey:    []byte("row-" + strconv.Itoa(i)),
				Mutations: []*btpb.Mutation{{
					Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
						FamilyName:      "cf",
						ColumnQualifier: []byte("col"),
						TimestampMicros: 1000,
						Value:           []byte("value"),
					}},
				}},
			}
			if _, err := s.MutateRow(ctx, req); err != nil {
				t.Fatalf("Populating table: %v", err)
			}
		}
	}

	attempts := 100
	finished := make(chan bool)
	go func() {
		populate()
		mock := &MockSampleRowKeysServer{}
		for i := 0; i < attempts; i++ {
			if err := s.SampleRowKeys(&btpb.SampleRowKeysRequest{TableName: tbl.Name}, mock); err != nil {
				t.Errorf("SampleRowKeys error: %v", err)
			}
		}
		finished <- true
	}()
	go antagonist(s, attempts, tbl.Name, finished)
	timeout := time.After(15 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case <-finished:
		case <-timeout:
			t.Fatalf("Timeout waiting for task %d\n", i)
		}
	}
}

func TestSampleRowKeysVsDropRowRange(t *testing.T) {
	SampleRowKeysConcurrentTest(t, func(s *server, attempts int, tblName string, finished chan (bool)) {
		ctx := context.Background()
		for i := 0; i < attempts; i++ {
			req := &btapb.DropRowRangeRequest{
				Name:   tblName,
				Target: &btapb.DropRowRangeRequest_DeleteAllDataFromTable{DeleteAllDataFromTable: true},
			}
			if _, err := s.DropRowRange(ctx, req); err != nil {
				t.Fatalf("Dropping all rows: %v", err)
			}
		}
		finished <- true
	})
}

func TestSampleRowKeysVsModifyColumnFamilies(t *testing.T) {
	SampleRowKeysConcurrentTest(t, func(s *server, attempts int, tblName string, finished chan (bool)) {
		ctx := context.Background()
		for i := 0; i < attempts; i++ {
			req := &btapb.ModifyColumnFamiliesRequest{
				Name: tblName,
				Modifications: []*btapb.ModifyColumnFamiliesRequest_Modification{{
					Id:  "cf2",
					Mod: &btapb.ModifyColumnFamiliesRequest_Modification_Create{Create: &btapb.ColumnFamily{}},
				}},
			}
			if _, err := s.ModifyColumnFamilies(ctx, req); err != nil {
				t.Fatalf("Creating column family cf2: %v", err)
			}
			rowCount := 100
			for i := 0; i < rowCount; i++ {
				req := &btpb.MutateRowRequest{
					TableName: tblName,
					RowKey:    []byte("row-" + strconv.Itoa(i)),
					Mutations: []*btpb.Mutation{{
						Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
							FamilyName:      "cf2",
							ColumnQualifier: []byte("col"),
							TimestampMicros: 1000,
							Value:           []byte("value"),
						}},
					}},
				}
				if _, err := s.MutateRow(ctx, req); err != nil {
					t.Fatalf("Populating table: %v", err)
				}
			}
			req = &btapb.ModifyColumnFamiliesRequest{
				Name: tblName,
				Modifications: []*btapb.ModifyColumnFamiliesRequest_Modification{{
					Id:  "cf2",
					Mod: &btapb.ModifyColumnFamiliesRequest_Modification_Drop{Drop: true},
				}},
			}
			if _, err := s.ModifyColumnFamilies(ctx, req); err != nil {
				t.Fatalf("Dropping column family cf2: %v", err)
			}
		}
		finished <- true
	})
}

func TestModifyColumnFamilies(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	tblInfo, err := populateTable(ctx, s)
	if err != nil {
		t.Fatal(err)
	}

	readRows := func(expectChunks, expectCols, expectFams int) {
		t.Helper()
		mock := &MockReadRowsServer{}
		req := &btpb.ReadRowsRequest{TableName: tblInfo.Name}
		if err := s.ReadRows(req, mock); err != nil {
			t.Fatalf("ReadRows error: %v", err)
		}
		cols := map[string]bool{}
		fams := map[string]bool{}
		chunks := 0
		for _, r := range mock.responses {
			for _, c := range r.Chunks {
				chunks++
				colName := c.FamilyName.Value + "." + string(c.Qualifier.Value)
				cols[colName] = true
				fams[c.FamilyName.Value] = true
			}
		}
		if got, want := len(fams), expectFams; got != want {
			t.Errorf("col count: got %d, want %d", got, want)
		}
		if got, want := len(cols), expectCols; got != want {
			t.Errorf("col count: got %d, want %d", got, want)
		}
		if got, want := chunks, expectChunks; got != want {
			t.Errorf("chunk count: got %d, want %d", got, want)
		}
	}

	// 	readRows(27, 9, 3)
	// but because our GC is lazy and exact we expect 21; cf0 has a verison limit
	readRows(21, 9, 3)

	// Now drop the middle column.
	if _, err := s.ModifyColumnFamilies(ctx, &btapb.ModifyColumnFamiliesRequest{
		Name: tblInfo.Name,
		Modifications: []*btapb.ModifyColumnFamiliesRequest_Modification{{
			Id:  "cf1",
			Mod: &btapb.ModifyColumnFamiliesRequest_Modification_Drop{Drop: true},
		}},
	}); err != nil {
		t.Fatalf("ModifyColumnFamilies error: %v", err)
	}

	readRows(12, 6, 2)

	// adding the column back should not re-create the data.
	if _, err := s.ModifyColumnFamilies(ctx, &btapb.ModifyColumnFamiliesRequest{
		Name: tblInfo.Name,
		Modifications: []*btapb.ModifyColumnFamiliesRequest_Modification{{
			Id:  "cf1",
			Mod: &btapb.ModifyColumnFamiliesRequest_Modification_Create{Create: &btapb.ColumnFamily{}},
		}},
	}); err != nil {
		t.Fatalf("ModifyColumnFamilies error: %v", err)
	}

	readRows(12, 6, 2)
}

func TestDropRowRange(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	newTbl := btapb.Table{
		ColumnFamilies: map[string]*btapb.ColumnFamily{
			"cf": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}}},
		},
	}
	tblInfo, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: "cluster", TableId: "t", Table: &newTbl})
	if err != nil {
		t.Fatalf("Creating table: %v", err)
	}

	tbl := s.tables[tblInfo.Name]

	// Populate the table
	prefixes := []string{"AAA", "BBB", "CCC", "DDD"}
	count := 3
	doWrite := func() {
		for _, prefix := range prefixes {
			for i := 0; i < count; i++ {
				req := &btpb.MutateRowRequest{
					TableName: tblInfo.Name,
					RowKey:    []byte(prefix + strconv.Itoa(i)),
					Mutations: []*btpb.Mutation{{
						Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
							FamilyName:      "cf",
							ColumnQualifier: []byte("col"),
							TimestampMicros: 1000,
							Value:           []byte{},
						}},
					}},
				}
				if _, err := s.MutateRow(ctx, req); err != nil {
					t.Fatalf("Populating table: %v", err)
				}
			}
		}
	}

	doWrite()
	tblSize := tbl.rows.Len()
	req := &btapb.DropRowRangeRequest{
		Name:   tblInfo.Name,
		Target: &btapb.DropRowRangeRequest_RowKeyPrefix{RowKeyPrefix: []byte("AAA")},
	}
	if _, err = s.DropRowRange(ctx, req); err != nil {
		t.Fatalf("Dropping first range: %v", err)
	}
	got, want := tbl.rows.Len(), tblSize-count
	if got != want {
		t.Errorf("Row count after first drop: got %d (%v), want %d", got, tbl.rows, want)
	}

	req = &btapb.DropRowRangeRequest{
		Name:   tblInfo.Name,
		Target: &btapb.DropRowRangeRequest_RowKeyPrefix{RowKeyPrefix: []byte("DDD")},
	}
	if _, err = s.DropRowRange(ctx, req); err != nil {
		t.Fatalf("Dropping second range: %v", err)
	}
	got, want = tbl.rows.Len(), tblSize-(2*count)
	if got != want {
		t.Errorf("Row count after second drop: got %d (%v), want %d", got, tbl.rows, want)
	}

	req = &btapb.DropRowRangeRequest{
		Name:   tblInfo.Name,
		Target: &btapb.DropRowRangeRequest_RowKeyPrefix{RowKeyPrefix: []byte("XXX")},
	}
	if _, err = s.DropRowRange(ctx, req); err != nil {
		t.Fatalf("Dropping invalid range: %v", err)
	}
	got, want = tbl.rows.Len(), tblSize-(2*count)
	if got != want {
		t.Errorf("Row count after invalid drop: got %d (%v), want %d", got, tbl.rows, want)
	}

	req = &btapb.DropRowRangeRequest{
		Name:   tblInfo.Name,
		Target: &btapb.DropRowRangeRequest_DeleteAllDataFromTable{DeleteAllDataFromTable: true},
	}
	if _, err = s.DropRowRange(ctx, req); err != nil {
		t.Fatalf("Dropping all data: %v", err)
	}
	got, want = tbl.rows.Len(), 0
	if got != want {
		t.Errorf("Row count after drop all: got %d, want %d", got, want)
	}

	// Test that we can write rows, delete some and then write them again.
	count = 1
	doWrite()

	req = &btapb.DropRowRangeRequest{
		Name:   tblInfo.Name,
		Target: &btapb.DropRowRangeRequest_DeleteAllDataFromTable{DeleteAllDataFromTable: true},
	}
	if _, err = s.DropRowRange(ctx, req); err != nil {
		t.Fatalf("Dropping all data: %v", err)
	}
	got, want = tbl.rows.Len(), 0
	if got != want {
		t.Errorf("Row count after drop all: got %d, want %d", got, want)
	}

	doWrite()
	got, want = tbl.rows.Len(), len(prefixes)
	if got != want {
		t.Errorf("Row count after rewrite: got %d, want %d", got, want)
	}

	req = &btapb.DropRowRangeRequest{
		Name:   tblInfo.Name,
		Target: &btapb.DropRowRangeRequest_RowKeyPrefix{RowKeyPrefix: []byte("BBB")},
	}
	if _, err = s.DropRowRange(ctx, req); err != nil {
		t.Fatalf("Dropping range: %v", err)
	}
	doWrite()
	got, want = tbl.rows.Len(), len(prefixes)
	if got != want {
		t.Errorf("Row count after drop range: got %d, want %d", got, want)
	}
}

type MockReadRowsServer struct {
	responses []*btpb.ReadRowsResponse
	grpc.ServerStream
}

func (s *MockReadRowsServer) Send(resp *btpb.ReadRowsResponse) error {
	s.responses = append(s.responses, resp)
	return nil
}

func TestCheckTimestampMaxValue(t *testing.T) {
	// Test that max Timestamp value can be passed in TimestampMicros without error
	// and that max Timestamp is the largest valid value in Millis.
	// See issue https://github.com/googleapis/google-cloud-go/issues/1790
	ctx := context.Background()
	s := newTestServer(t)
	newTbl := btapb.Table{
		ColumnFamilies: map[string]*btapb.ColumnFamily{
			"cf0": {},
		},
	}
	tblInfo, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: "issue-1790", TableId: "t", Table: &newTbl})
	if err != nil {
		t.Fatalf("Creating table: %v", err)
	}
	var maxTimestamp int64 = math.MaxInt64 - math.MaxInt64%1000
	mreq1 := &btpb.MutateRowRequest{
		TableName: tblInfo.Name,
		RowKey:    []byte("row"),
		Mutations: []*btpb.Mutation{{
			Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
				FamilyName:      "cf0",
				ColumnQualifier: []byte("col"),
				TimestampMicros: maxTimestamp,
				Value:           []byte{},
			}},
		}},
	}
	if _, err := s.MutateRow(ctx, mreq1); err != nil {
		t.Fatalf("TimestampMicros wasn't set: %v", err)
	}

	mreq2 := &btpb.MutateRowRequest{
		TableName: tblInfo.Name,
		RowKey:    []byte("row"),
		Mutations: []*btpb.Mutation{{
			Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
				FamilyName:      "cf0",
				ColumnQualifier: []byte("col"),
				TimestampMicros: maxTimestamp + 1000,
				Value:           []byte{},
			}},
		}},
	}
	if _, err := s.MutateRow(ctx, mreq2); err == nil {
		t.Fatalf("want TimestampMicros rejection, got acceptance: %v", err)
	}
}

func TestReadRows(t *testing.T) {
	ctx := context.Background()
	s := newTestServer(t)
	newTbl := btapb.Table{
		ColumnFamilies: map[string]*btapb.ColumnFamily{
			"cf0": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}}},
		},
	}
	tblInfo, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: "cluster", TableId: "t", Table: &newTbl})
	if err != nil {
		t.Fatalf("Creating table: %v", err)
	}
	mreq := &btpb.MutateRowRequest{
		TableName: tblInfo.Name,
		RowKey:    []byte("row"),
		Mutations: []*btpb.Mutation{{
			Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
				FamilyName:      "cf0",
				ColumnQualifier: []byte("col"),
				TimestampMicros: 1000,
				Value:           []byte{},
			}},
		}},
	}
	if _, err := s.MutateRow(ctx, mreq); err != nil {
		t.Fatalf("Populating table: %v", err)
	}

	for _, rowset := range []*btpb.RowSet{
		{RowKeys: [][]byte{[]byte("row")}},
		{RowRanges: []*btpb.RowRange{{StartKey: &btpb.RowRange_StartKeyClosed{StartKeyClosed: []byte("")}}}},
		{RowRanges: []*btpb.RowRange{{StartKey: &btpb.RowRange_StartKeyClosed{StartKeyClosed: []byte("r")}}}},
		{RowRanges: []*btpb.RowRange{{
			StartKey: &btpb.RowRange_StartKeyClosed{StartKeyClosed: []byte("")},
			EndKey:   &btpb.RowRange_EndKeyOpen{EndKeyOpen: []byte("s")},
		}}},
	} {
		mock := &MockReadRowsServer{}
		req := &btpb.ReadRowsRequest{TableName: tblInfo.Name, Rows: rowset}
		if err = s.ReadRows(req, mock); err != nil {
			t.Fatalf("ReadRows error: %v", err)
		}
		if got, want := len(mock.responses), 1; got != want {
			t.Errorf("%+v: response count: got %d, want %d", rowset, got, want)
		}
	}
}

func TestReadRowsError(t *testing.T) {
	ctx := context.Background()
	s := newTestServer(t)
	newTbl := btapb.Table{
		ColumnFamilies: map[string]*btapb.ColumnFamily{
			"cf0": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}}},
		},
	}
	tblInfo, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: "cluster", TableId: "t", Table: &newTbl})
	if err != nil {
		t.Fatalf("Creating table: %v", err)
	}
	mreq := &btpb.MutateRowRequest{
		TableName: tblInfo.Name,
		RowKey:    []byte("row"),
		Mutations: []*btpb.Mutation{{
			Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
				FamilyName:      "cf0",
				ColumnQualifier: []byte("col"),
				TimestampMicros: 1000,
				Value:           []byte{},
			}},
		}},
	}
	if _, err := s.MutateRow(ctx, mreq); err != nil {
		t.Fatalf("Populating table: %v", err)
	}

	mock := &MockReadRowsServer{}
	req := &btpb.ReadRowsRequest{
		TableName: tblInfo.Name, Filter: &btpb.RowFilter{
			Filter: &btpb.RowFilter_RowKeyRegexFilter{RowKeyRegexFilter: []byte("[")},
		}, // Invalid regex.
	}
	if err = s.ReadRows(req, mock); err == nil {
		t.Fatal("ReadRows got no error, want error")
	}
}

func TestReadRowsAfterDeletion(t *testing.T) {
	ctx := context.Background()
	s := newTestServer(t)
	newTbl := btapb.Table{
		ColumnFamilies: map[string]*btapb.ColumnFamily{
			"cf0": {},
		},
	}
	tblInfo, err := s.CreateTable(ctx, &btapb.CreateTableRequest{
		Parent: "cluster", TableId: "t", Table: &newTbl,
	})
	if err != nil {
		t.Fatalf("Creating table: %v", err)
	}
	populateTable(ctx, s)
	dreq := &btpb.MutateRowRequest{
		TableName: tblInfo.Name,
		RowKey:    []byte("row"),
		Mutations: []*btpb.Mutation{{
			Mutation: &btpb.Mutation_DeleteFromRow_{
				DeleteFromRow: &btpb.Mutation_DeleteFromRow{},
			},
		}},
	}
	if _, err := s.MutateRow(ctx, dreq); err != nil {
		t.Fatalf("Deleting from table: %v", err)
	}

	mock := &MockReadRowsServer{}
	req := &btpb.ReadRowsRequest{TableName: tblInfo.Name}
	if err = s.ReadRows(req, mock); err != nil {
		t.Fatalf("ReadRows error: %v", err)
	}
	if got, want := len(mock.responses), 0; got != want {
		t.Errorf("response count: got %d, want %d", got, want)
	}
}

func TestReadRowsOrder(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	newTbl := btapb.Table{
		ColumnFamilies: map[string]*btapb.ColumnFamily{
			"cf0": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}}},
		},
	}
	tblInfo, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: "cluster", TableId: "t", Table: &newTbl})
	if err != nil {
		t.Fatalf("Creating table: %v", err)
	}
	count := 3
	mcf := func(i int) *btapb.ModifyColumnFamiliesRequest {
		return &btapb.ModifyColumnFamiliesRequest{
			Name: tblInfo.Name,
			Modifications: []*btapb.ModifyColumnFamiliesRequest_Modification{{
				Id:  "cf" + strconv.Itoa(i),
				Mod: &btapb.ModifyColumnFamiliesRequest_Modification_Create{Create: &btapb.ColumnFamily{}},
			}},
		}
	}
	for i := 1; i <= count; i++ {
		_, err = s.ModifyColumnFamilies(ctx, mcf(i))
		if err != nil {
			t.Fatal(err)
		}
	}
	// Populate the table
	for fc := 0; fc < count; fc++ {
		for cc := count; cc > 0; cc-- {
			for tc := 0; tc < count; tc++ {
				req := &btpb.MutateRowRequest{
					TableName: tblInfo.Name,
					RowKey:    []byte("row"),
					Mutations: []*btpb.Mutation{{
						Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
							FamilyName:      "cf" + strconv.Itoa(fc),
							ColumnQualifier: []byte("col" + strconv.Itoa(cc)),
							TimestampMicros: int64((tc + 1) * 1000),
							Value:           []byte{},
						}},
					}},
				}
				if _, err := s.MutateRow(ctx, req); err != nil {
					t.Fatalf("Populating table: %v", err)
				}
			}
		}
	}
	req := &btpb.ReadRowsRequest{
		TableName: tblInfo.Name,
		Rows:      &btpb.RowSet{RowKeys: [][]byte{[]byte("row")}},
	}
	mock := &MockReadRowsServer{}
	if err = s.ReadRows(req, mock); err != nil {
		t.Errorf("ReadRows error: %v", err)
	}
	if len(mock.responses) == 0 {
		t.Fatal("Response count: got 0, want > 0")
	}
	if len(mock.responses[0].Chunks) != 21 {
		t.Fatalf("Chunk count: got %d, want 21", len(mock.responses[0].Chunks))
	}
	testOrder := func(ms *MockReadRowsServer) {
		var prevFam, prevCol string
		var prevTime int64
		for _, cc := range ms.responses[0].Chunks {
			if prevFam == "" {
				prevFam = cc.FamilyName.Value
				prevCol = string(cc.Qualifier.Value)
				prevTime = cc.TimestampMicros
				continue
			}
			if cc.FamilyName.Value < prevFam {
				t.Errorf("Family order is not correct: got %s < %s", cc.FamilyName.Value, prevFam)
			} else if cc.FamilyName.Value == prevFam {
				if string(cc.Qualifier.Value) < prevCol {
					t.Errorf("Column order is not correct: got %s < %s", string(cc.Qualifier.Value), prevCol)
				} else if string(cc.Qualifier.Value) == prevCol {
					if cc.TimestampMicros > prevTime {
						t.Errorf("cell order is not correct: got %d > %d", cc.TimestampMicros, prevTime)
					}
				}
			}
			prevFam = cc.FamilyName.Value
			prevCol = string(cc.Qualifier.Value)
			prevTime = cc.TimestampMicros
		}
	}
	testOrder(mock)

	// Read with interleave filter
	inter := &btpb.RowFilter_Interleave{}
	fnr := &btpb.RowFilter{Filter: &btpb.RowFilter_FamilyNameRegexFilter{FamilyNameRegexFilter: "cf1"}}
	cqr := &btpb.RowFilter{Filter: &btpb.RowFilter_ColumnQualifierRegexFilter{ColumnQualifierRegexFilter: []byte("col2")}}
	inter.Filters = append(inter.Filters, fnr, cqr)
	req = &btpb.ReadRowsRequest{
		TableName: tblInfo.Name,
		Rows:      &btpb.RowSet{RowKeys: [][]byte{[]byte("row")}},
		Filter: &btpb.RowFilter{
			Filter: &btpb.RowFilter_Interleave_{Interleave: inter},
		},
	}

	mock = &MockReadRowsServer{}
	if err = s.ReadRows(req, mock); err != nil {
		t.Errorf("ReadRows error: %v", err)
	}
	if len(mock.responses) == 0 {
		t.Fatal("Response count: got 0, want > 0")
	}
	if len(mock.responses[0].Chunks) != 16 {
		t.Fatalf("Chunk count: got %d, want 16", len(mock.responses[0].Chunks))
	}
	testOrder(mock)

	// Check order after ReadModifyWriteRow
	rmw := func(i int) *btpb.ReadModifyWriteRowRequest {
		return &btpb.ReadModifyWriteRowRequest{
			TableName: tblInfo.Name,
			RowKey:    []byte("row"),
			Rules: []*btpb.ReadModifyWriteRule{{
				FamilyName:      "cf3",
				ColumnQualifier: []byte("col" + strconv.Itoa(i)),
				Rule:            &btpb.ReadModifyWriteRule_IncrementAmount{IncrementAmount: 1},
			}},
		}
	}
	for i := count; i > 0; i-- {
		if _, err := s.ReadModifyWriteRow(ctx, rmw(i)); err != nil {
			t.Fatal(err)
		}
	}
	req = &btpb.ReadRowsRequest{
		TableName: tblInfo.Name,
		Rows:      &btpb.RowSet{RowKeys: [][]byte{[]byte("row")}},
	}
	mock = &MockReadRowsServer{}
	if err = s.ReadRows(req, mock); err != nil {
		t.Errorf("ReadRows error: %v", err)
	}
	if len(mock.responses) == 0 {
		t.Fatal("Response count: got 0, want > 0")
	}
	if len(mock.responses[0].Chunks) != 24 {
		t.Fatalf("Chunk count: got %d, want 24", len(mock.responses[0].Chunks))
	}
	testOrder(mock)
}

func TestReadRowsWithlabelTransformer(t *testing.T) {
	ctx := context.Background()
	s := newTestServer(t)
	newTbl := btapb.Table{
		ColumnFamilies: map[string]*btapb.ColumnFamily{
			"cf0": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}}},
		},
	}
	tblInfo, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: "cluster", TableId: "t", Table: &newTbl})
	if err != nil {
		t.Fatalf("Creating table: %v", err)
	}
	mreq := &btpb.MutateRowRequest{
		TableName: tblInfo.Name,
		RowKey:    []byte("row"),
		Mutations: []*btpb.Mutation{{
			Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
				FamilyName:      "cf0",
				ColumnQualifier: []byte("col"),
				TimestampMicros: 1000,
				Value:           []byte{},
			}},
		}},
	}
	if _, err := s.MutateRow(ctx, mreq); err != nil {
		t.Fatalf("Populating table: %v", err)
	}

	mock := &MockReadRowsServer{}
	req := &btpb.ReadRowsRequest{
		TableName: tblInfo.Name,
		Filter: &btpb.RowFilter{
			Filter: &btpb.RowFilter_ApplyLabelTransformer{
				ApplyLabelTransformer: "label",
			},
		},
	}
	if err = s.ReadRows(req, mock); err != nil {
		t.Fatalf("ReadRows error: %v", err)
	}

	if got, want := len(mock.responses), 1; got != want {
		t.Fatalf("response count: got %d, want %d", got, want)
	}
	resp := mock.responses[0]
	if got, want := len(resp.Chunks), 1; got != want {
		t.Fatalf("chunks count: got %d, want %d", got, want)
	}
	chunk := resp.Chunks[0]
	if got, want := len(chunk.Labels), 1; got != want {
		t.Fatalf("labels count: got %d, want %d", got, want)
	}
	if got, want := chunk.Labels[0], "label"; got != want {
		t.Fatalf("label: got %s, want %s", got, want)
	}

	mock = &MockReadRowsServer{}
	req = &btpb.ReadRowsRequest{
		TableName: tblInfo.Name,
		Filter: &btpb.RowFilter{
			Filter: &btpb.RowFilter_ApplyLabelTransformer{
				ApplyLabelTransformer: "", // invalid label
			},
		},
	}
	if err = s.ReadRows(req, mock); err == nil {
		t.Fatal("ReadRows want invalid label error, got none")
	}
}

func TestReadRowsReversed(t *testing.T) {
	ctx := context.Background()
	srv := newTestServer(t)
	newTbl := btapb.Table{
		ColumnFamilies: map[string]*btapb.ColumnFamily{
			"cf": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}}},
		},
	}
	tbl, err := srv.CreateTable(ctx, &btapb.CreateTableRequest{Parent: "cluster", TableId: "t", Table: &newTbl})
	if err != nil {
		t.Fatalf("Creating table: %v", err)
	}
	entries := []struct {
		row   string
		value []byte
	}{
		{"row1", []byte("a")},
		{"row2", []byte("b")},
	}

	for _, entry := range entries {
		req := &btpb.MutateRowRequest{
			TableName: tbl.Name,
			RowKey:    []byte(entry.row),
			Mutations: []*btpb.Mutation{{
				Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
					FamilyName:      "cf",
					ColumnQualifier: []byte("cq"),
					TimestampMicros: 1000,
					Value:           entry.value,
				}},
			}},
		}
		if _, err := srv.MutateRow(ctx, req); err != nil {
			t.Fatalf("Failed to insert entry %v into server: %v", entry, err)
		}
	}

	rrss := new(MockReadRowsServer)
	rreq := &btpb.ReadRowsRequest{TableName: tbl.Name, Reversed: true}
	if err := srv.ReadRows(rreq, rrss); err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}

	var gotChunks []*btpb.ReadRowsResponse_CellChunk
	for _, res := range rrss.responses {
		gotChunks = append(gotChunks, res.Chunks...)
	}

	wantChunks := []*btpb.ReadRowsResponse_CellChunk{
		{
			RowKey:          []byte("row2"),
			FamilyName:      &wrappers.StringValue{Value: "cf"},
			Qualifier:       &wrappers.BytesValue{Value: []byte("cq")},
			TimestampMicros: 1000,
			Value:           []byte("b"),
			RowStatus: &btpb.ReadRowsResponse_CellChunk_CommitRow{
				CommitRow: true,
			},
		},
		{
			RowKey:          []byte("row1"),
			FamilyName:      &wrappers.StringValue{Value: "cf"},
			Qualifier:       &wrappers.BytesValue{Value: []byte("cq")},
			TimestampMicros: 1000,
			Value:           []byte("a"),
			RowStatus: &btpb.ReadRowsResponse_CellChunk_CommitRow{
				CommitRow: true,
			},
		},
	}
	if diff := cmp.Diff(gotChunks, wantChunks, cmp.Comparer(proto.Equal)); diff != "" {
		t.Fatalf("Response chunks mismatch: got: + want -\n%s", diff)
	}
}

func TestCheckAndMutateRowWithoutPredicate(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	newTbl := btapb.Table{
		ColumnFamilies: map[string]*btapb.ColumnFamily{
			"cf": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}}},
		},
	}
	tbl, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: "cluster", TableId: "t", Table: &newTbl})
	if err != nil {
		t.Fatalf("Creating table: %v", err)
	}

	val := []byte("value")
	muts := []*btpb.Mutation{{
		Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
			FamilyName:      "cf",
			ColumnQualifier: []byte("col"),
			TimestampMicros: 1000,
			Value:           val,
		}},
	}}

	mrreq := &btpb.MutateRowRequest{
		TableName: tbl.Name,
		RowKey:    []byte("row-present"),
		Mutations: muts,
	}
	if _, err := s.MutateRow(ctx, mrreq); err != nil {
		t.Fatalf("Populating table: %v", err)
	}

	req := &btpb.CheckAndMutateRowRequest{
		TableName:      tbl.Name,
		RowKey:         []byte("row-not-present"),
		FalseMutations: muts,
	}
	if res, err := s.CheckAndMutateRow(ctx, req); err != nil {
		t.Errorf("CheckAndMutateRow error: %v", err)
	} else if got, want := res.PredicateMatched, false; got != want {
		t.Errorf("Invalid PredicateMatched value: got %t, want %t", got, want)
	}

	req = &btpb.CheckAndMutateRowRequest{
		TableName:      tbl.Name,
		RowKey:         []byte("row-present"),
		FalseMutations: muts,
	}
	if res, err := s.CheckAndMutateRow(ctx, req); err != nil {
		t.Errorf("CheckAndMutateRow error: %v", err)
	} else if got, want := res.PredicateMatched, true; got != want {
		t.Errorf("Invalid PredicateMatched value: got %t, want %t", got, want)
	}
}

func TestCheckAndMutateRowWithPredicate(t *testing.T) {
	ctx := context.Background()
	srv := newTestServer(t)

	tblReq := &btapb.CreateTableRequest{
		Parent:  "issue-1435",
		TableId: "table_id",
		Table: &btapb.Table{
			ColumnFamilies: map[string]*btapb.ColumnFamily{
				"cf": {},
				"df": {},
				"ef": {},
				"ff": {},
				"zf": {},
			},
		},
	}
	tbl, err := srv.CreateTable(ctx, tblReq)
	if err != nil {
		t.Fatalf("Failed to create the table: %v", err)
	}

	entries := []struct {
		row                         string
		value                       []byte
		familyName, columnQualifier string
	}{
		{"row1", []byte{0x11}, "cf", "cq"},
		{"row2", []byte{0x1a}, "df", "dq"},
		{"row3", []byte{'a'}, "ef", "eq"},
		{"row4", []byte{'b'}, "ff", "fq"},
	}

	for _, entry := range entries {
		req := &btpb.MutateRowRequest{
			TableName: tbl.Name,
			RowKey:    []byte(entry.row),
			Mutations: []*btpb.Mutation{{
				Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
					FamilyName:      entry.familyName,
					ColumnQualifier: []byte(entry.columnQualifier),
					TimestampMicros: 1000,
					Value:           entry.value,
				}},
			}},
		}
		if _, err := srv.MutateRow(ctx, req); err != nil {
			t.Fatalf("Failed to insert entry %v into server: %v", entry, err)
		}
	}

	var bogusMutations = []*btpb.Mutation{{
		Mutation: &btpb.Mutation_DeleteFromFamily_{
			DeleteFromFamily: &btpb.Mutation_DeleteFromFamily{
				FamilyName: "bogus_family",
			},
		},
	}}

	tests := []struct {
		req       *btpb.CheckAndMutateRowRequest
		wantMatch bool
		name      string

		// if wantState is nil, that means we don't care to check
		// what the state of the world is.
		wantState []*btpb.ReadRowsResponse_CellChunk
	}{
		{
			req: &btpb.CheckAndMutateRowRequest{
				TableName: tbl.Name,
				RowKey:    []byte("row1"),
				PredicateFilter: &btpb.RowFilter{
					Filter: &btpb.RowFilter_RowKeyRegexFilter{
						RowKeyRegexFilter: []byte("not-one"),
					},
				},
				TrueMutations: bogusMutations,
			},
			name: "no match",
		},
		{
			req: &btpb.CheckAndMutateRowRequest{
				TableName: tbl.Name,
				RowKey:    []byte("row1"),
				PredicateFilter: &btpb.RowFilter{
					Filter: &btpb.RowFilter_RowKeyRegexFilter{
						RowKeyRegexFilter: []byte("ro.+"),
					},
				},
				FalseMutations: bogusMutations,
			},
			wantMatch: true,
			name:      "rowkey regex",
		},
		{
			req: &btpb.CheckAndMutateRowRequest{
				TableName: tbl.Name,
				RowKey:    []byte("row1"),
				PredicateFilter: &btpb.RowFilter{
					Filter: &btpb.RowFilter_PassAllFilter{
						PassAllFilter: true,
					},
				},
				FalseMutations: bogusMutations,
			},
			wantMatch: true,
			name:      "pass all",
		},
		{
			req: &btpb.CheckAndMutateRowRequest{
				TableName: tbl.Name,
				RowKey:    []byte("row1"),
				PredicateFilter: &btpb.RowFilter{
					Filter: &btpb.RowFilter_BlockAllFilter{
						BlockAllFilter: true,
					},
				},
				FalseMutations: []*btpb.Mutation{
					{
						Mutation: &btpb.Mutation_SetCell_{
							SetCell: &btpb.Mutation_SetCell{
								FamilyName:      "zf",
								Value:           []byte("foo"),
								TimestampMicros: 2000,
								ColumnQualifier: []byte("et"),
							},
						},
					},
				},
			},
			name:      "BlockAll for row1",
			wantMatch: false,
			wantState: []*btpb.ReadRowsResponse_CellChunk{
				{
					RowKey: []byte("row1"),
					FamilyName: &wrappers.StringValue{
						Value: "cf",
					},
					Qualifier: &wrappers.BytesValue{
						Value: []byte("cq"),
					},
					TimestampMicros: 1000,
					Value:           []byte{0x11},
				},
				{
					RowKey: []byte("row1"),
					FamilyName: &wrappers.StringValue{
						Value: "zf",
					},
					Qualifier: &wrappers.BytesValue{
						Value: []byte("et"),
					},
					TimestampMicros: 2000,
					Value:           []byte("foo"),
					RowStatus: &btpb.ReadRowsResponse_CellChunk_CommitRow{
						CommitRow: true,
					},
				},
				{
					RowKey: []byte("row2"),
					FamilyName: &wrappers.StringValue{
						Value: "df",
					},
					Qualifier: &wrappers.BytesValue{
						Value: []byte("dq"),
					},
					TimestampMicros: 1000,
					Value:           []byte{0x1a},
					RowStatus: &btpb.ReadRowsResponse_CellChunk_CommitRow{
						CommitRow: true,
					},
				},
				{
					RowKey: []byte("row3"),
					FamilyName: &wrappers.StringValue{
						Value: "ef",
					},
					Qualifier: &wrappers.BytesValue{
						Value: []byte("eq"),
					},
					TimestampMicros: 1000,
					Value:           []byte("a"),
					RowStatus: &btpb.ReadRowsResponse_CellChunk_CommitRow{
						CommitRow: true,
					},
				},
				{
					RowKey: []byte("row4"),
					FamilyName: &wrappers.StringValue{
						Value: "ff",
					},
					Qualifier: &wrappers.BytesValue{
						Value: []byte("fq"),
					},
					TimestampMicros: 1000,
					Value:           []byte("b"),
					RowStatus: &btpb.ReadRowsResponse_CellChunk_CommitRow{
						CommitRow: true,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := srv.CheckAndMutateRow(ctx, tt.req)
			if err != nil {
				t.Fatalf("CheckAndMutateRow error: %v", err)
			}
			got, want := res.PredicateMatched, tt.wantMatch
			if got != want {
				t.Fatalf("Invalid PredicateMatched value: got %t, want %t\nRequest: %+v", got, want, tt.req)
			}

			if tt.wantState == nil {
				return
			}

			rreq := &btpb.ReadRowsRequest{TableName: tbl.Name}
			mock := &MockReadRowsServer{}
			if err = srv.ReadRows(rreq, mock); err != nil {
				t.Fatalf("ReadRows error: %v", err)
			}

			// Collect all the cellChunks
			var gotCellChunks []*btpb.ReadRowsResponse_CellChunk
			for _, res := range mock.responses {
				gotCellChunks = append(gotCellChunks, res.Chunks...)
			}
			sort.Slice(gotCellChunks, func(i, j int) bool {
				ci, cj := gotCellChunks[i], gotCellChunks[j]
				return compareCellChunks(ci, cj)
			})
			wantCellChunks := tt.wantState[0:]
			sort.Slice(wantCellChunks, func(i, j int) bool {
				return compareCellChunks(wantCellChunks[i], wantCellChunks[j])
			})

			// bttest for some reason undeterministically returns:
			//      RowStatus: &bigtable.ReadRowsResponse_CellChunk_CommitRow{CommitRow: true},
			// so we'll ignore that field during comparison.
			scrubRowStatus := func(cs []*btpb.ReadRowsResponse_CellChunk) []*btpb.ReadRowsResponse_CellChunk {
				for _, c := range cs {
					c.RowStatus = nil
				}
				return cs
			}
			diff := cmp.Diff(scrubRowStatus(gotCellChunks), scrubRowStatus(wantCellChunks), cmp.Comparer(proto.Equal))
			if diff != "" {
				t.Fatalf("unexpected response: %s", diff)
			}
		})
	}
}

// compareCellChunks is a comparator that is passed
// into sort.Slice to stably sort cell chunks.
func compareCellChunks(ci, cj *btpb.ReadRowsResponse_CellChunk) bool {
	if bytes.Compare(ci.RowKey, cj.RowKey) > 0 {
		return false
	}
	if bytes.Compare(ci.Value, cj.Value) > 0 {
		return false
	}
	return ci.FamilyName.GetValue() < cj.FamilyName.GetValue()
}

func TestServer_ReadModifyWriteRow(t *testing.T) {
	s := newTestServer(t)

	ctx := context.Background()
	newTbl := btapb.Table{
		ColumnFamilies: map[string]*btapb.ColumnFamily{
			"cf": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}}},
		},
	}
	tbl, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: "cluster", TableId: "t", Table: &newTbl})
	if err != nil {
		t.Fatalf("Creating table: %v", err)
	}

	req := &btpb.ReadModifyWriteRowRequest{
		TableName: tbl.Name,
		RowKey:    []byte("row-key"),
		Rules: []*btpb.ReadModifyWriteRule{
			{
				FamilyName:      "cf",
				ColumnQualifier: []byte("q1"),
				Rule: &btpb.ReadModifyWriteRule_AppendValue{
					AppendValue: []byte("a"),
				},
			},
			// multiple ops for same cell
			{
				FamilyName:      "cf",
				ColumnQualifier: []byte("q1"),
				Rule: &btpb.ReadModifyWriteRule_AppendValue{
					AppendValue: []byte("b"),
				},
			},
			// different cell whose qualifier should sort before the prior rules
			{
				FamilyName:      "cf",
				ColumnQualifier: []byte("q0"),
				Rule: &btpb.ReadModifyWriteRule_IncrementAmount{
					IncrementAmount: 1,
				},
			},
		},
	}

	got, err := s.ReadModifyWriteRow(ctx, req)
	if err != nil {
		t.Fatalf("ReadModifyWriteRow error: %v", err)
	}

	want := &btpb.ReadModifyWriteRowResponse{
		Row: &btpb.Row{
			Key: []byte("row-key"),
			Families: []*btpb.Family{{
				Name: "cf",
				Columns: []*btpb.Column{
					{
						Qualifier: []byte("q0"),
						Cells: []*btpb.Cell{{
							Value: []byte{0, 0, 0, 0, 0, 0, 0, 1},
						}},
					},
					{
						Qualifier: []byte("q1"),
						Cells: []*btpb.Cell{{
							Value: []byte("ab"),
						}},
					},
				},
			}},
		},
	}

	scrubTimestamp := func(resp *btpb.ReadModifyWriteRowResponse) *btpb.ReadModifyWriteRowResponse {
		for _, fam := range resp.GetRow().GetFamilies() {
			for _, col := range fam.GetColumns() {
				for _, cell := range col.GetCells() {
					cell.TimestampMicros = 0
				}
			}
		}
		return resp
	}
	diff := cmp.Diff(scrubTimestamp(got), scrubTimestamp(want), cmp.Comparer(proto.Equal))
	if diff != "" {
		t.Errorf("unexpected response: %s", diff)
	}
}

// helper function to populate table data
func populateTable(ctx context.Context, s *server) (*btapb.Table, error) {
	newTbl := btapb.Table{
		ColumnFamilies: map[string]*btapb.ColumnFamily{
			"cf0": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{1}}},
		},
	}
	tblInfo, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: "cluster", TableId: "t", Table: &newTbl})
	if err != nil {
		return nil, err
	}
	count := 3
	mcf := func(i int) *btapb.ModifyColumnFamiliesRequest {
		return &btapb.ModifyColumnFamiliesRequest{
			Name: tblInfo.Name,
			Modifications: []*btapb.ModifyColumnFamiliesRequest_Modification{{
				Id:  "cf" + strconv.Itoa(i),
				Mod: &btapb.ModifyColumnFamiliesRequest_Modification_Create{&btapb.ColumnFamily{}},
			}},
		}
	}
	for i := 1; i <= count; i++ {
		_, err = s.ModifyColumnFamilies(ctx, mcf(i))
		if err != nil {
			return nil, err
		}
	}
	// Populate the table
	for fc := 0; fc < count; fc++ {
		for cc := count; cc > 0; cc-- {
			for tc := 0; tc < count; tc++ {
				req := &btpb.MutateRowRequest{
					TableName: tblInfo.Name,
					RowKey:    []byte("row"),
					Mutations: []*btpb.Mutation{{
						Mutation: &btpb.Mutation_SetCell_{&btpb.Mutation_SetCell{
							FamilyName:      "cf" + strconv.Itoa(fc),
							ColumnQualifier: []byte("col" + strconv.Itoa(cc)),
							TimestampMicros: int64((tc + 1) * 1000),
							Value:           []byte{},
						}},
					}},
				}
				if _, err := s.MutateRow(ctx, req); err != nil {
					return nil, err
				}
			}
		}
	}

	return tblInfo, nil
}

func TestFilters(t *testing.T) {
	tests := []struct {
		in   *btpb.RowFilter
		code codes.Code
		out  int
	}{
		{in: &btpb.RowFilter{Filter: &btpb.RowFilter_BlockAllFilter{true}}, out: 0},
		{in: &btpb.RowFilter{Filter: &btpb.RowFilter_BlockAllFilter{false}}, code: codes.InvalidArgument},
		{in: &btpb.RowFilter{Filter: &btpb.RowFilter_PassAllFilter{true}}, out: 1},
		{in: &btpb.RowFilter{Filter: &btpb.RowFilter_PassAllFilter{false}}, code: codes.InvalidArgument},
	}

	ctx := context.Background()

	s := newTestServer(t)

	tblInfo, err := populateTable(ctx, s)
	if err != nil {
		t.Fatal(err)
	}

	req := &btpb.ReadRowsRequest{
		TableName: tblInfo.Name,
		Rows:      &btpb.RowSet{RowKeys: [][]byte{[]byte("row")}},
	}

	for _, tc := range tests {
		req.Filter = tc.in

		mock := &MockReadRowsServer{}
		err := s.ReadRows(req, mock)
		if tc.code != codes.OK {
			s, _ := status.FromError(err)
			if s.Code() != tc.code {
				t.Errorf("error code: got %d, want %d", s.Code(), tc.code)
			}
			continue
		}

		if err != nil {
			t.Errorf("ReadRows error: %v", err)
			continue
		}

		if len(mock.responses) != tc.out {
			t.Errorf("Response count: got %d, want %d", len(mock.responses), tc.out)
			continue
		}
	}
}

func Test_Mutation_DeleteFromColumn(t *testing.T) {
	ctx := context.Background()

	s := newTestServer(t)

	tblInfo, err := populateTable(ctx, s)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		in   *btpb.MutateRowRequest
		fail bool
	}{
		{
			in: &btpb.MutateRowRequest{
				TableName: tblInfo.Name,
				RowKey:    []byte("row"),
				Mutations: []*btpb.Mutation{{
					Mutation: &btpb.Mutation_DeleteFromColumn_{DeleteFromColumn: &btpb.Mutation_DeleteFromColumn{
						FamilyName:      "cf1",
						ColumnQualifier: []byte("col1"),
						TimeRange: &btpb.TimestampRange{
							StartTimestampMicros: 2000,
							EndTimestampMicros:   1000,
						},
					}},
				}},
			},
			fail: true,
		},
		{
			in: &btpb.MutateRowRequest{
				TableName: tblInfo.Name,
				RowKey:    []byte("row"),
				Mutations: []*btpb.Mutation{{
					Mutation: &btpb.Mutation_DeleteFromColumn_{DeleteFromColumn: &btpb.Mutation_DeleteFromColumn{
						FamilyName:      "cf2",
						ColumnQualifier: []byte("col2"),
						TimeRange: &btpb.TimestampRange{
							StartTimestampMicros: 1000,
							EndTimestampMicros:   2000,
						},
					}},
				}},
			},
			fail: false,
		},
		{
			in: &btpb.MutateRowRequest{
				TableName: tblInfo.Name,
				RowKey:    []byte("row"),
				Mutations: []*btpb.Mutation{{
					Mutation: &btpb.Mutation_DeleteFromColumn_{DeleteFromColumn: &btpb.Mutation_DeleteFromColumn{
						FamilyName:      "cf3",
						ColumnQualifier: []byte("col3"),
						TimeRange: &btpb.TimestampRange{
							StartTimestampMicros: 1000,
							EndTimestampMicros:   0,
						},
					}},
				}},
			},
			fail: false,
		},
		{
			in: &btpb.MutateRowRequest{
				TableName: tblInfo.Name,
				RowKey:    []byte("row"),
				Mutations: []*btpb.Mutation{{
					Mutation: &btpb.Mutation_DeleteFromColumn_{DeleteFromColumn: &btpb.Mutation_DeleteFromColumn{
						FamilyName:      "cf4",
						ColumnQualifier: []byte("col4"),
						TimeRange: &btpb.TimestampRange{
							StartTimestampMicros: 0,
							EndTimestampMicros:   1000,
						},
					}},
				}},
			},
			fail: true,
		},
	}
	for _, test := range tests {
		_, err = s.MutateRow(ctx, test.in)

		if err != nil && !test.fail {
			t.Errorf("expected passed got failure for : %v \n with err: %v", test.in, err)
		}

		if err == nil && test.fail {
			t.Errorf("expected failure got passed for : %v", test)
		}
	}
}

func TestFilterRow(t *testing.T) {
	row := &row{
		key: "row",
		families: map[string]*family{
			"fam": {
				Name: "fam",
				Cells: map[string][]cell{
					"col": {{Ts: 1000, Value: []byte("val")}},
				},
			},
		},
	}
	for _, test := range []struct {
		filter *btpb.RowFilter
		want   bool
	}{
		// The regexp-based filters perform whole-string, case-sensitive matches.
		{&btpb.RowFilter{Filter: &btpb.RowFilter_RowKeyRegexFilter{[]byte("row")}}, true},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_RowKeyRegexFilter{[]byte("ro")}}, false},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_RowKeyRegexFilter{[]byte("ROW")}}, false},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_RowKeyRegexFilter{[]byte("moo")}}, false},

		{&btpb.RowFilter{Filter: &btpb.RowFilter_FamilyNameRegexFilter{"fam"}}, true},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_FamilyNameRegexFilter{"f.*"}}, true},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_FamilyNameRegexFilter{"[fam]+"}}, true},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_FamilyNameRegexFilter{"fa"}}, false},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_FamilyNameRegexFilter{"FAM"}}, false},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_FamilyNameRegexFilter{"moo"}}, false},

		{&btpb.RowFilter{Filter: &btpb.RowFilter_ColumnQualifierRegexFilter{[]byte("col")}}, true},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_ColumnQualifierRegexFilter{[]byte("co")}}, false},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_ColumnQualifierRegexFilter{[]byte("COL")}}, false},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_ColumnQualifierRegexFilter{[]byte("moo")}}, false},

		{&btpb.RowFilter{Filter: &btpb.RowFilter_ValueRegexFilter{[]byte("val")}}, true},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_ValueRegexFilter{[]byte("va")}}, false},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_ValueRegexFilter{[]byte("VAL")}}, false},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_ValueRegexFilter{[]byte("moo")}}, false},

		{&btpb.RowFilter{Filter: &btpb.RowFilter_TimestampRangeFilter{&btpb.TimestampRange{StartTimestampMicros: int64(0), EndTimestampMicros: int64(1000)}}}, false},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_TimestampRangeFilter{&btpb.TimestampRange{StartTimestampMicros: int64(1000), EndTimestampMicros: int64(2000)}}}, true},
	} {
		got, err := filterRow(test.filter, row.copy())
		if err != nil {
			t.Errorf("%s: got unexpected error: %v", proto.CompactTextString(test.filter), err)
		}
		if got != test.want {
			t.Errorf("%s: got %t, want %t", proto.CompactTextString(test.filter), got, test.want)
		}
	}
}

func TestFilterRowWithErrors(t *testing.T) {
	row := &row{
		key: "row",
		families: map[string]*family{
			"fam": {
				Name: "fam",
				Cells: map[string][]cell{
					"col": {{Ts: 1000, Value: []byte("val")}},
				},
			},
		},
	}
	for _, test := range []struct {
		badRegex *btpb.RowFilter
	}{
		{&btpb.RowFilter{Filter: &btpb.RowFilter_RowKeyRegexFilter{[]byte("[")}}},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_FamilyNameRegexFilter{"["}}},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_ColumnQualifierRegexFilter{[]byte("[")}}},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_ValueRegexFilter{[]byte("[")}}},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_Chain_{
			Chain: &btpb.RowFilter_Chain{
				Filters: []*btpb.RowFilter{
					{Filter: &btpb.RowFilter_ValueRegexFilter{[]byte("[")}},
				},
			},
		}}},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_Condition_{
			Condition: &btpb.RowFilter_Condition{
				PredicateFilter: &btpb.RowFilter{Filter: &btpb.RowFilter_ValueRegexFilter{[]byte("[")}},
			},
		}}},

		{&btpb.RowFilter{Filter: &btpb.RowFilter_RowSampleFilter{0.0}}},                                                                                        // 0.0 is invalid.
		{&btpb.RowFilter{Filter: &btpb.RowFilter_RowSampleFilter{1.0}}},                                                                                        // 1.0 is invalid.
		{&btpb.RowFilter{Filter: &btpb.RowFilter_TimestampRangeFilter{&btpb.TimestampRange{StartTimestampMicros: int64(1), EndTimestampMicros: int64(1000)}}}}, // Server only supports millisecond precision.
		{&btpb.RowFilter{Filter: &btpb.RowFilter_TimestampRangeFilter{&btpb.TimestampRange{StartTimestampMicros: int64(1000), EndTimestampMicros: int64(1)}}}}, // Server only supports millisecond precision.
	} {
		got, err := filterRow(test.badRegex, row.copy())
		if got != false {
			t.Errorf("%s: got true, want false", proto.CompactTextString(test.badRegex))
		}
		if err == nil {
			t.Errorf("%s: got no error, want error", proto.CompactTextString(test.badRegex))
		}
	}
}

func TestFilterRowWithRowSampleFilter(t *testing.T) {
	prev := randFloat
	randFloat = func() float64 { return 0.5 }
	defer func() { randFloat = prev }()
	for _, test := range []struct {
		p    float64
		want bool
	}{
		{0.1, false}, // Less than random float. Return no rows.
		{0.5, false}, // Equal to random float. Return no rows.
		{0.9, true},  // Greater than random float. Return all rows.
	} {
		got, err := filterRow(&btpb.RowFilter{Filter: &btpb.RowFilter_RowSampleFilter{test.p}}, &row{})
		if err != nil {
			t.Fatalf("%f: %v", test.p, err)
		}
		if got != test.want {
			t.Errorf("%v: got %t, want %t", test.p, got, test.want)
		}
	}
}

func TestFilterRowWithBinaryColumnQualifier(t *testing.T) {
	rs := []byte{128, 128}
	row := &row{
		key: string(rs),
		families: map[string]*family{
			"fam": {
				Name: "fam",
				Cells: map[string][]cell{
					string(rs): {{Ts: 1000, Value: []byte("val")}},
				},
			},
		},
	}
	for _, test := range []struct {
		filter string
		want   bool
	}{
		{`\x80\x80`, true},      // succeeds, exact match
		{`\x80\x81`, false},     // fails
		{`\x80`, false},         // fails, because the regexp must match the entire input
		{`\x80*`, true},         // succeeds: 0 or more 128s
		{`[\x7f\x80]{2}`, true}, // succeeds: exactly two of either 127 or 128
		{`\C{2}`, true},         // succeeds: two bytes
	} {
		got, _ := filterRow(&btpb.RowFilter{Filter: &btpb.RowFilter_ColumnQualifierRegexFilter{[]byte(test.filter)}}, row.copy())
		if got != test.want {
			t.Errorf("%v: got %t, want %t", test.filter, got, test.want)
		}
	}
}

func TestFilterRowWithUnicodeColumnQualifier(t *testing.T) {
	rs := []byte("a§b")
	row := &row{
		key: string(rs),
		families: map[string]*family{
			"fam": {
				Name: "fam",
				Cells: map[string][]cell{
					string(rs): {{Ts: 1000, Value: []byte("val")}},
				},
			},
		},
	}
	for _, test := range []struct {
		filter string
		want   bool
	}{
		{`a§b`, true},        // succeeds, exact match
		{`a\xC2\xA7b`, true}, // succeeds, exact match
		{`a\xC2.+`, true},    // succeeds, prefix match
		{`a\xC2\C{2}`, true}, // succeeds, prefix match
		{`a\xC.+`, false},    // fails, prefix match, bad escape
		{`a§.+`, true},       // succeeds, prefix match
		{`.+§b`, true},       // succeeds, suffix match
		{`.§b`, true},        // succeeds
		{`a§c`, false},       // fails
		{`§b`, false},        // fails, because the regexp must match the entire input
		{`.*§.*`, true},      // succeeds: anything with a §
		{`.+§.+`, true},      // succeeds: anything with a § in the middle
		{`a\C{2}b`, true},    // succeeds: § is two bytes
		{`\C{4}`, true},      // succeeds: four bytes
	} {
		got, _ := filterRow(&btpb.RowFilter{Filter: &btpb.RowFilter_ColumnQualifierRegexFilter{[]byte(test.filter)}}, row.copy())
		if got != test.want {
			t.Errorf("%v: got %t, want %t", test.filter, got, test.want)
		}
	}
}

// Test that a single column qualifier with the interleave filter returns
// the correct result and not return every single row.
// See Issue https://github.com/googleapis/google-cloud-go/issues/1399
func TestFilterRowWithSingleColumnQualifier(t *testing.T) {
	ctx := context.Background()
	srv := newTestServer(t)

	tblReq := &btapb.CreateTableRequest{
		Parent:  "issue-1399",
		TableId: "table_id",
		Table: &btapb.Table{
			ColumnFamilies: map[string]*btapb.ColumnFamily{
				"cf": {},
			},
		},
	}
	tbl, err := srv.CreateTable(ctx, tblReq)
	if err != nil {
		t.Fatalf("Failed to create the table: %v", err)
	}

	entries := []struct {
		row   string
		value []byte
	}{
		{"row1", []byte{0x11}},
		{"row2", []byte{0x1a}},
		{"row3", []byte{'a'}},
		{"row4", []byte{'b'}},
	}

	for _, entry := range entries {
		req := &btpb.MutateRowRequest{
			TableName: tbl.Name,
			RowKey:    []byte(entry.row),
			Mutations: []*btpb.Mutation{{
				Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
					FamilyName:      "cf",
					ColumnQualifier: []byte("cq"),
					TimestampMicros: 1000,
					Value:           entry.value,
				}},
			}},
		}
		if _, err := srv.MutateRow(ctx, req); err != nil {
			t.Fatalf("Failed to insert entry %v into server: %v", entry, err)
		}
	}

	// After insertion now it is time for querying.
	req := &btpb.ReadRowsRequest{
		TableName: tbl.Name,
		Filter: &btpb.RowFilter{Filter: &btpb.RowFilter_Chain_{
			Chain: &btpb.RowFilter_Chain{Filters: []*btpb.RowFilter{{
				Filter: &btpb.RowFilter_Interleave_{
					Interleave: &btpb.RowFilter_Interleave{
						Filters: []*btpb.RowFilter{{Filter: &btpb.RowFilter_Condition_{
							Condition: &btpb.RowFilter_Condition{
								PredicateFilter: &btpb.RowFilter{Filter: &btpb.RowFilter_Chain_{
									Chain: &btpb.RowFilter_Chain{Filters: []*btpb.RowFilter{
										{
											Filter: &btpb.RowFilter_ValueRangeFilter{ValueRangeFilter: &btpb.ValueRange{
												StartValue: &btpb.ValueRange_StartValueClosed{
													StartValueClosed: []byte("a"),
												},
												EndValue: &btpb.ValueRange_EndValueClosed{EndValueClosed: []byte("a")},
											}},
										},
										{Filter: &btpb.RowFilter_PassAllFilter{PassAllFilter: true}},
									}},
								}},
								TrueFilter: &btpb.RowFilter{Filter: &btpb.RowFilter_PassAllFilter{PassAllFilter: true}},
							},
						}},
							{Filter: &btpb.RowFilter_BlockAllFilter{BlockAllFilter: true}},
						},
					},
				},
			},
				{Filter: &btpb.RowFilter_PassAllFilter{PassAllFilter: true}},
			}},
		}},
	}

	rrss := new(MockReadRowsServer)
	if err := srv.ReadRows(req, rrss); err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}

	if g, w := len(rrss.responses), 1; g != w {
		t.Fatalf("Results/Streamed chunks mismatch:: got %d want %d", g, w)
	}

	got := rrss.responses[0]
	// Only row3 should be matched.
	want := &btpb.ReadRowsResponse{
		Chunks: []*btpb.ReadRowsResponse_CellChunk{
			{
				RowKey:          []byte("row3"),
				FamilyName:      &wrappers.StringValue{Value: "cf"},
				Qualifier:       &wrappers.BytesValue{Value: []byte("cq")},
				TimestampMicros: 1000,
				Value:           []byte("a"),
				RowStatus: &btpb.ReadRowsResponse_CellChunk_CommitRow{
					CommitRow: true,
				},
			},
		},
	}
	if diff := cmp.Diff(got, want, cmp.Comparer(proto.Equal)); diff != "" {
		t.Fatalf("Response mismatch: got: + want -\n%s", diff)
	}
}

func TestValueFilterRowWithAlternationInRegex(t *testing.T) {
	// Test that regex alternation is applied properly.
	// See Issue https://github.com/googleapis/google-cloud-go/issues/1499
	ctx := context.Background()
	srv := newTestServer(t)

	tblReq := &btapb.CreateTableRequest{
		Parent:  "issue-1499",
		TableId: "table_id",
		Table: &btapb.Table{
			ColumnFamilies: map[string]*btapb.ColumnFamily{
				"cf": {},
			},
		},
	}
	tbl, err := srv.CreateTable(ctx, tblReq)
	if err != nil {
		t.Fatalf("Failed to create the table: %v", err)
	}

	entries := []struct {
		row   string
		value []byte
	}{
		{"row1", []byte("")},
		{"row2", []byte{'x'}},
		{"row3", []byte{'a'}},
		{"row4", []byte{'m'}},
	}

	for _, entry := range entries {
		req := &btpb.MutateRowRequest{
			TableName: tbl.Name,
			RowKey:    []byte(entry.row),
			Mutations: []*btpb.Mutation{{
				Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
					FamilyName:      "cf",
					ColumnQualifier: []byte("cq"),
					TimestampMicros: 1000,
					Value:           entry.value,
				}},
			}},
		}
		if _, err := srv.MutateRow(ctx, req); err != nil {
			t.Fatalf("Failed to insert entry %v into server: %v", entry, err)
		}
	}

	// After insertion now it is time for querying.
	req := &btpb.ReadRowsRequest{
		TableName: tbl.Name,
		Rows:      &btpb.RowSet{},
		Filter: &btpb.RowFilter{
			Filter: &btpb.RowFilter_ValueRegexFilter{
				ValueRegexFilter: []byte("|a"),
			},
		},
	}

	rrss := new(MockReadRowsServer)
	if err := srv.ReadRows(req, rrss); err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}

	var gotChunks []*btpb.ReadRowsResponse_CellChunk
	for _, res := range rrss.responses {
		gotChunks = append(gotChunks, res.Chunks...)
	}

	// Only row1 "" and row3 "a" should be matched.
	wantChunks := []*btpb.ReadRowsResponse_CellChunk{
		{
			RowKey:          []byte("row1"),
			FamilyName:      &wrappers.StringValue{Value: "cf"},
			Qualifier:       &wrappers.BytesValue{Value: []byte("cq")},
			TimestampMicros: 1000,
			Value:           []byte(""),
			RowStatus: &btpb.ReadRowsResponse_CellChunk_CommitRow{
				CommitRow: true,
			},
		},
		{
			RowKey:          []byte("row3"),
			FamilyName:      &wrappers.StringValue{Value: "cf"},
			Qualifier:       &wrappers.BytesValue{Value: []byte("cq")},
			TimestampMicros: 1000,
			Value:           []byte("a"),
			RowStatus: &btpb.ReadRowsResponse_CellChunk_CommitRow{
				CommitRow: true,
			},
		},
	}
	if diff := cmp.Diff(gotChunks, wantChunks, cmp.Comparer(proto.Equal)); diff != "" {
		t.Fatalf("Response chunks mismatch: got: + want -\n%s", diff)
	}
}

func TestMutateRowEmptyMutationErrors(t *testing.T) {
	srv := newTestServer(t)
	ctx := context.Background()
	req := &btpb.MutateRowRequest{
		TableName: "mytable",
		RowKey:    []byte("r"),
		Mutations: []*btpb.Mutation{},
	}

	resp, err := srv.MutateRow(ctx, req)
	if resp != nil ||
		fmt.Sprint(err) !=
			"rpc error: code = InvalidArgument"+
				" desc = No mutations provided" {
		t.Fatalf("Failed to error %s", err)
	}
}

type bigtableTestingMutateRowsServer struct {
	grpc.ServerStream
}

func (x *bigtableTestingMutateRowsServer) Send(m *btpb.MutateRowsResponse) error {
	return nil
}

func TestMutateRowsEmptyMutationErrors(t *testing.T) {
	srv := newTestServer(t)
	req := &btpb.MutateRowsRequest{
		TableName: "mytable",
		Entries: []*btpb.MutateRowsRequest_Entry{
			{Mutations: []*btpb.Mutation{}},
			{Mutations: []*btpb.Mutation{}},
		},
	}

	err := srv.MutateRows(req, &bigtableTestingMutateRowsServer{})
	if fmt.Sprint(err) !=
		"rpc error: code = InvalidArgument "+
			"desc = No mutations provided" {
		t.Fatalf("Failed to error %s", err)
	}
}

func TestFilterRowCellsPerRowLimitFilterTruthiness(t *testing.T) {
	row := &row{
		key: "row",
		families: map[string]*family{
			"fam": {
				Name: "fam",
				Cells: map[string][]cell{
					"col1": {{Ts: 1000, Value: []byte("val2")}},
					"col2": {
						{Ts: 1000, Value: []byte("val2")},
						{Ts: 1000, Value: []byte("val3")},
					},
				},
				ColNames: []string{"col1", "col2"},
			},
		},
	}
	for _, test := range []struct {
		filter *btpb.RowFilter
		want   bool
	}{
		// The regexp-based filters perform whole-string, case-sensitive matches.
		{&btpb.RowFilter{Filter: &btpb.RowFilter_CellsPerRowOffsetFilter{1}}, true},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_CellsPerRowOffsetFilter{2}}, true},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_CellsPerRowOffsetFilter{3}}, false},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_CellsPerRowOffsetFilter{4}}, false},
	} {
		got, err := filterRow(test.filter, row.copy())
		if err != nil {
			t.Errorf("%s: got unexpected error: %v", proto.CompactTextString(test.filter), err)
		}
		if got != test.want {
			t.Errorf("%s: got %t, want %t", proto.CompactTextString(test.filter), got, test.want)
		}
	}
}
