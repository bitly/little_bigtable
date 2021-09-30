package bttest

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"log"

	"github.com/mattn/go-sqlite3"
)

// SqlTables persists tables to tables_t
type SqlTables struct {
	db *sql.DB
}

func NewSqlTables(db *sql.DB) *SqlTables {
	return &SqlTables{
		db: db,
	}
}

func (t *table) Scan(src interface{}) error {
	switch src := src.(type) {
	case nil:
		return nil
	case []byte:
	default:
		return fmt.Errorf("unknown type %T", src)
	}

	b := bytes.NewBuffer(src.([]byte))
	err := gob.NewDecoder(b).Decode(&t.families)

	t.counter = uint64(len(t.families))
	for _, f := range t.families {
		if f.Order > t.counter {
			t.counter = f.Order
		}
	}
	return err
}

func (t *table) Bytes() ([]byte, error) {
	if t == nil {
		return nil, nil
	}
	b := new(bytes.Buffer)
	err := gob.NewEncoder(b).Encode(t.families)
	return b.Bytes(), err
}

func (db *SqlTables) Get(parent, tableId string) *table {
	tbl := &table{
		parent:  parent,
		tableId: tableId,
		rows:    NewSqlRows(db.db, parent, tableId),
	}
	err := db.db.QueryRow("SELECT metadata FROM tables_t WHERE parent = ? AND table_id = ?", parent, tableId).Scan(tbl)
	if err == sql.ErrNoRows {
		return nil
	}
	return tbl
}

func (db *SqlTables) GetAll() []*table {
	var tables []*table

	rows, err := db.db.Query("SELECT parent, table_id, metadata FROM tables_t")
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		t := table{}
		if err := rows.Scan(&t.parent, &t.tableId, &t); err != nil {
			log.Fatal(err)
		}
		t.rows = NewSqlRows(db.db, t.parent, t.tableId)
		tables = append(tables, &t)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	return tables
}

func (db *SqlTables) Save(t *table) {
	metadata, err := t.Bytes()
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.db.Exec("INSERT INTO tables_t (parent, table_id, metadata) VALUES (?, ?, ?)", t.parent, t.tableId, metadata)
	if e, ok := err.(sqlite3.Error); ok && e.Code == 19 {
		_, err = db.db.Exec("UPDATE tables_t SET metadata = ? WHERE parent = ? AND table_id = ?", metadata, t.parent, t.tableId)
	}
	if err != nil {
		log.Fatalf("%#v", err)
	}
}

func (db *SqlTables) Delete(t *table) {
	_, err := db.db.Exec("DELETE FROM tables_t WHERE parent = ? AND table_id = ? ", t.parent, t.tableId)
	if err != nil {
		log.Fatal(err)
	}
}
