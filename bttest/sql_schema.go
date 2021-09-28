package bttest

import (
	"context"
	"database/sql"
)

func CreateTables(ctx context.Context, db *sql.DB) error {
	query := "CREATE TABLE IF NOT EXISTS rows_t ( \n" +
		"`parent` TEXT NOT NULL,\n" +
		"`table_id` TEXT NOT NULL,\n" +
		"`row_key` TEXT NOT NULL,\n" +
		"`families` BLOB NOT NULL,\n" +
		"PRIMARY KEY (`parent`, `table_id`, `row_key`)\n" +
		")"
	// this table could be WITHOUT ROWID but that is only supported in sqllite 3.8.2+
	// https://www.sqlite.org/releaselog/3_8_2.html
	// log.Print(query)
	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	query = "CREATE TABLE IF NOT EXISTS tables_t ( \n" +
		"`parent` TEXT NOT NULL,\n" +
		"`table_id` TEXT NOT NULL,\n" +
		"`metadata` BLOG NOT NULL,\n" +
		"PRIMARY KEY  (`parent`, `table_id`)\n" +
		")"
	// log.Print(query)
	_, err = db.ExecContext(ctx, query)
	if err != nil {
		return err
	}
	return nil
}
