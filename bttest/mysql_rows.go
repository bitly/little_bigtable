package bttest

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"log"

	"github.com/google/btree"
)

// MysqlRows is a backend modeled on the github.com/google/btree interface
type MysqlRows struct {
	parent  string // Values are of the form `projects/{project}/instances/{instance}`.
	tableId string // The name by which the new table should be referred to within the parent instance

	db *sql.DB
}

func NewMysqlRows(db *sql.DB, parent, tableId string) *MysqlRows {
	return &MysqlRows{
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

func (db *MysqlRows) query(iterator ItemIterator, query string, args ...interface{}) {
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
		iterator(&r)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}

func (db *MysqlRows) Ascend(iterator ItemIterator) {
	db.query(iterator, "SELECT row_key, families FROM rows_t WHERE parent = ? and table_id = ? ORDER BY row_key ASC", db.parent, db.tableId)
}

func (db *MysqlRows) AscendGreaterOrEqual(pivot Item, iterator ItemIterator) {
	row := pivot.(*row)
	db.query(iterator, "SELECT row_key, families FROM rows_t WHERE parent = ? and table_id = ? and row_key >= ? ORDER BY row_key ASC", db.parent, db.tableId, row.key)
}

func (db *MysqlRows) AscendLessThan(pivot Item, iterator ItemIterator) {
	row := pivot.(*row)
	db.query(iterator, "SELECT row_key, families FROM rows_t WHERE parent = ? and table_id = ? and row_key < ? ORDER BY row_key ASC", db.parent, db.tableId, row.key)
}

func (db *MysqlRows) AscendRange(greaterOrEqual, lessThan Item, iterator ItemIterator) {
	ge := greaterOrEqual.(*row)
	lt := lessThan.(*row)
	db.query(iterator, "SELECT row_key, families FROM rows_t WHERE parent = ? and table_id = ? and row_key >= ? and row_key < ? ORDER BY row_key ASC", db.parent, db.tableId, ge.key, lt.key)
}

func (db *MysqlRows) DeleteAll() {
	_, err := db.db.Exec("DELETE FROM rows_t WHERE parent = ? and table_id = ?", db.parent, db.tableId)
	if err != nil {
		log.Fatal(err)
	}

}

func (db *MysqlRows) Delete(item Item) {
	row := item.(*row)
	_, err := db.db.Exec("DELETE FROM rows_t WHERE parent = ? and table_id = ? and row_key = ?", db.parent, db.tableId, row.key)
	if err != nil {
		log.Fatal(err)
	}
}

func (db *MysqlRows) Get(key Item) Item {
	row := key.(*row)
	if row.families == nil {
		row.families = make(map[string]*family)
	}
	err := db.db.QueryRow("SELECT families FROM rows_t WHERE parent = ? and table_id = ? and row_key = ?", db.parent, db.tableId, row.key).Scan(row)
	if err == sql.ErrNoRows {
		return row
	}
	if err != nil {
		log.Fatal(err)
	}
	return row
}

func (db *MysqlRows) Len() int {
	var count int
	err := db.db.QueryRow("SELECT count(*) FROM rows_t WHERE parent = ? and table_id = ?", db.parent, db.tableId).Scan(&count)
	if err != nil {
		log.Fatal(err)
	}
	return count
}

func (db *MysqlRows) ReplaceOrInsert(item Item) Item {
	row := item.(*row)
	families, err := row.Bytes()
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.db.Exec("INSERT INTO rows_t (parent, table_id, row_key, families) values (?, ?, ?, ?) ON DUPLICATE KEY UPDATE families = ?", db.parent, db.tableId, row.key, families, families)
	if err != nil {
		log.Fatal(err)
	}
	return row
}
