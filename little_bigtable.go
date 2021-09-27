/*
little_bigtable launches the mysql backed Bigtable emulator on the given address.
*/
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/bitly/little_bigtable/bttest"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
)

var (
	host = flag.String("host", "localhost", "the address to bind to on the local machine")
	port = flag.Int("port", 9000, "the port number to bind to on the local machine")
	dsn  = flag.String("mysql-dsn", "/little_bigtable?sql_mode=STRICT_ALL_TABLES", "[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]")
)

const (
	maxMsgSize = 256 * 1024 * 1024 // 256 MiB
)

func main() {
	grpc.EnableTracing = false
	flag.Parse()
	cfg, err := mysql.ParseDSN(*dsn)
	if err != nil {
		log.Fatal(err)
	}
	if cfg.DBName == "" {
		log.Fatal("--mysql-dsn must include database name")
	}

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		log.Fatalf("failed creating mysql connection %v", err)
	}
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	if err := db.Ping(); err != nil {
		if e, ok := err.(*mysql.MySQLError); ok {
			switch e.Number {
			case 1049:
				dbName := cfg.DBName
				cfg.DBName = ""
				tempDSN := cfg.FormatDSN()
				db2, err := sql.Open("mysql", tempDSN)
				if err != nil {
					log.Fatal(err)
				}
				// create table
				log.Printf("Creating database %s", dbName)
				_, err = db2.Exec(fmt.Sprintf("create database %s", dbName))
				if err != nil {
					log.Fatal(err)
				}
				db2.Close()
			}
		}
		log.Printf("warning: %s", err)
	}

	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
	}
	srv, err := bttest.NewServer(fmt.Sprintf("%s:%d", *host, *port), opts...)
	if err != nil {
		log.Fatalf("failed to start emulator: %v", err)
	}

	log.Printf("\"little\" Bigtable emulator running on %s", srv.Addr)
	select {}
}
