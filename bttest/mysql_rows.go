package bttest

import (
	"database/sql"

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

type ItemIterator = btree.ItemIterator
type Item = btree.Item

func (db *MysqlRows) Ascend(iterator ItemIterator)                                     {}
func (db *MysqlRows) AscendGreaterOrEqual(pivot Item, iterator ItemIterator)           {}
func (db *MysqlRows) AscendLessThan(pivot Item, iterator ItemIterator)                 {}
func (db *MysqlRows) AscendRange(greaterOrEqual, lessThan Item, iterator ItemIterator) {}

func (db *MysqlRows) DeleteAll() {}

func (db *MysqlRows) Delete(item Item) Item { return nil }

func (db *MysqlRows) Get(key Item) Item { return &row{} }

func (db *MysqlRows) Len() int { return 0 }

func (db *MysqlRows) ReplaceOrInsert(item Item) Item { return &row{} }
