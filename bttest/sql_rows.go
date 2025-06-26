package bttest

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"log"
	"sync"

	"github.com/google/btree"
	"github.com/mattn/go-sqlite3"
)

// SqlRows is a backend modeled on the github.com/google/btree interface
// all errors are considered fatal
//
// rows are persisted in rows_t
type SqlRows struct {
	parent  string // Values are of the form `projects/{project}/instances/{instance}`.
	tableId string // The name by which the new table should be referred to within the parent instance

	mu sync.RWMutex
	db *sql.DB
}

func NewSqlRows(db *sql.DB, parent, tableId string) *SqlRows {
	return &SqlRows{
		parent:  parent,
		tableId: tableId,
		db:      db,
	}
}

func (r *row) Scan(src interface{}) error {
	switch src := src.(type) {
	case nil:
		return nil
	case []byte:
	default:
		return fmt.Errorf("unknown type %T", src)
	}

	b := bytes.NewBuffer(src.([]byte))
	return gob.NewDecoder(b).Decode(&r.families)
}
func (r *row) Bytes() ([]byte, error) {
	if r == nil {
		return nil, nil
	}
	b := new(bytes.Buffer)
	err := gob.NewEncoder(b).Encode(r.families)
	return b.Bytes(), err
}

type ItemIterator = btree.ItemIterator
type Item = btree.Item

func (db *SqlRows) query(iterator ItemIterator, query string, args ...interface{}) {
	// db.mu.RLock()
	// defer db.mu.RUnlock()
	rows, err := db.db.Query(query, args...)
	if err == sql.ErrNoRows {
		return
	}
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.key, &r); err != nil {
			log.Fatal(err)
		}
		if !iterator(&r) {
			break
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}

func (db *SqlRows) Ascend(iterator ItemIterator) {
	db.query(iterator, "SELECT row_key, families FROM rows_t WHERE parent = ? and table_id = ? ORDER BY row_key ASC", db.parent, db.tableId)
}

func (db *SqlRows) AscendGreaterOrEqual(pivot Item, iterator ItemIterator) {
	row := pivot.(*row)
	db.query(iterator, "SELECT row_key, families FROM rows_t WHERE parent = ? and table_id = ? and row_key >= ? ORDER BY row_key ASC", db.parent, db.tableId, row.key)
}

func (db *SqlRows) AscendLessThan(pivot Item, iterator ItemIterator) {
	row := pivot.(*row)
	db.query(iterator, "SELECT row_key, families FROM rows_t WHERE parent = ? and table_id = ? and row_key < ? ORDER BY row_key ASC", db.parent, db.tableId, row.key)
}

func (db *SqlRows) AscendRange(greaterOrEqual, lessThan Item, iterator ItemIterator) {
	ge := greaterOrEqual.(*row)
	lt := lessThan.(*row)
	db.query(iterator, "SELECT row_key, families FROM rows_t WHERE parent = ? and table_id = ? and row_key >= ? and row_key < ? ORDER BY row_key ASC", db.parent, db.tableId, ge.key, lt.key)
}

// Descending order methods for reverse scans
func (db *SqlRows) Descend(iterator ItemIterator) {
	db.query(iterator, "SELECT row_key, families FROM rows_t WHERE parent = ? and table_id = ? ORDER BY row_key DESC", db.parent, db.tableId)
}

func (db *SqlRows) DescendGreaterOrEqual(pivot Item, iterator ItemIterator) {
	row := pivot.(*row)
	db.query(iterator, "SELECT row_key, families FROM rows_t WHERE parent = ? and table_id = ? and row_key >= ? ORDER BY row_key DESC", db.parent, db.tableId, row.key)
}

func (db *SqlRows) DescendLessThan(pivot Item, iterator ItemIterator) {
	row := pivot.(*row)
	db.query(iterator, "SELECT row_key, families FROM rows_t WHERE parent = ? and table_id = ? and row_key < ? ORDER BY row_key DESC", db.parent, db.tableId, row.key)
}

func (db *SqlRows) DescendRange(greaterOrEqual, lessThan Item, iterator ItemIterator) {
	ge := greaterOrEqual.(*row)
	lt := lessThan.(*row)
	db.query(iterator, "SELECT row_key, families FROM rows_t WHERE parent = ? and table_id = ? and row_key >= ? and row_key < ? ORDER BY row_key DESC", db.parent, db.tableId, ge.key, lt.key)
}

func (db *SqlRows) DeleteAll() {
	db.mu.Lock()
	defer db.mu.Unlock()
	_, err := db.db.Exec("DELETE FROM rows_t WHERE parent = ? and table_id = ?", db.parent, db.tableId)
	if err != nil {
		log.Fatal(err)
	}

}

func (db *SqlRows) Delete(item Item) {
	db.mu.Lock()
	defer db.mu.Unlock()
	row := item.(*row)
	_, err := db.db.Exec("DELETE FROM rows_t WHERE parent = ? and table_id = ? and row_key = ?", db.parent, db.tableId, row.key)
	if err != nil {
		log.Fatal(err)
	}
}

func (db *SqlRows) Get(key Item) Item {
	row := key.(*row)
	if row.families == nil {
		row.families = make(map[string]*family)
	}
	// db.mu.RLock()
	// defer db.mu.RUnlock()
	err := db.db.QueryRow("SELECT families FROM rows_t WHERE parent = ? and table_id = ? and row_key = ?", db.parent, db.tableId, row.key).Scan(row)
	if err == sql.ErrNoRows {
		return row
	}
	if err != nil {
		log.Fatal(err)
	}
	return row
}

func (db *SqlRows) Len() int {
	var count int
	// db.mu.RLock()
	// defer db.mu.RUnlock()
	err := db.db.QueryRow("SELECT count(*) FROM rows_t WHERE parent = ? and table_id = ?", db.parent, db.tableId).Scan(&count)
	if err != nil {
		log.Fatal(err)
	}
	return count
}

func (db *SqlRows) ReplaceOrInsert(item Item) Item {
	row := item.(*row)
	families, err := row.Bytes()
	if err != nil {
		log.Fatal(err)
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	_, err = db.db.Exec("INSERT INTO rows_t (parent, table_id, row_key, families) values (?, ?, ?, ?)", db.parent, db.tableId, row.key, families)
	if e, ok := err.(sqlite3.Error); ok && e.Code == 19 {
		_, err = db.db.Exec("UPDATE rows_t SET families = ? WHERE parent = ? AND table_id = ? AND row_key = ?", families, db.parent, db.tableId, row.key)
	}
	if err != nil {
		log.Fatalf("row:%s err %s", row.key, err)
	}
	return row
}
