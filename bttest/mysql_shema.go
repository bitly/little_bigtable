package bttest

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/go-sql-driver/mysql"
)

func CreateMysqlDatabase(ctx context.Context, cfg *mysql.Config) error {
	dbName := cfg.DBName
	cfg.DBName = ""
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return err
	}
	defer db.Close()
	log.Printf("creating database %s", dbName)
	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName))
	return err
}

func mysqlTableExists(ctx context.Context, db *sql.DB, tablename string) (bool, error) {
	var parent string
	err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT parent FROM `%s` LIMIT 1", tablename)).Scan(&parent)
	if err == sql.ErrNoRows {
		return true, nil
	}
	if e, ok := err.(*mysql.MySQLError); ok {
		switch e.Number {
		case 1146: // Error 1146: Table 'little_bigtable.rows_t' doesn't exist
			return false, nil
		}
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func CreateTables(ctx context.Context, db *sql.DB) error {
	ok, err := mysqlTableExists(ctx, db, "rows_t")
	if err != nil {
		return err
	}
	if !ok {
		log.Printf("creating table rows_t")
		query := "CREATE TABLE rows_t ( \n" +
			"`parent` varchar(128) NOT NULL,\n" +
			"`table_id` varchar(128) NOT NULL,\n" +
			"`row_key` varchar(255) NOT NULL,\n" +
			"`families` blob NOT NULL,\n" +
			"PRIMARY KEY  (`parent`, `table_id`, `row_key`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
		// log.Print(query)
		_, err := db.ExecContext(ctx, query)
		if err != nil {
			return err
		}
	}

	ok, err = mysqlTableExists(ctx, db, "tables_t")
	if err != nil {
		return err
	}
	if !ok {
		log.Printf("creating table tables_t")
		query := "CREATE TABLE tables_t ( \n" +
			"`parent` varchar(128) NOT NULL,\n" +
			"`table_id` varchar(128) NOT NULL,\n" +
			"`metadata` blob NOT NULL,\n" +
			"PRIMARY KEY  (`parent`, `table_id`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
		// log.Print(query)
		_, err := db.ExecContext(ctx, query)
		if err != nil {
			return err
		}
	}

	return nil
}
