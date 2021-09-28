package bttest

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"log"
)

// MysqlTables persists tables
type MysqlTables struct {
	db *sql.DB
}

func NewMysqlTables(db *sql.DB) *MysqlTables {
	return &MysqlTables{
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
	return gob.NewDecoder(b).Decode(&t.families)

	// TODO: set to max
	t.counter = uint64(len(t.families))
	return nil
}

func (t *table) Bytes() ([]byte, error) {
	if t == nil {
		return nil, nil
	}
	b := new(bytes.Buffer)
	err := gob.NewEncoder(b).Encode(t.families)
	return b.Bytes(), err
}

func (db *MysqlTables) Get(parent, tableId string) *table {
	tbl := &table{
		parent:  parent,
		tableId: tableId,
		rows:    NewMysqlRows(db.db, parent, tableId),
	}
	err := db.db.QueryRow("SELECT metadata FROM tables_t WHERE parent = ? and table_id = ?", parent, tableId).Scan(tbl)
	if err == sql.ErrNoRows {
		return nil
	}
	return tbl
}

func (db *MysqlTables) GetAll() []*table {
	var tables []*table

	rows, err := db.db.Query("SELECT parent, table_id, metadata from tables_t")
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
		t.rows = NewMysqlRows(db.db, t.parent, t.tableId)
		tables = append(tables, &t)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	return tables
}

func (db *MysqlTables) Save(t *table) {
	metadata, err := t.Bytes()
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.db.Exec("INSERT INTO tables_t (parent, table_id, metadata) values (?, ?, ?) ON DUPLICATE KEY UPDATE metadata = ?", t.parent, t.tableId, metadata, metadata)
	if err != nil {
		log.Fatal(err)
	}
}

func (db *MysqlTables) Delete(t *table) {
	_, err := db.db.Exec("DELETE FROM tables_t WHERE parent = ? and table_id = ? ", t.parent, t.tableId)
	if err != nil {
		log.Fatal(err)
	}
}
