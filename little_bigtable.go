/*
little_bigtable launches the mysql backed Bigtable emulator on the given address.
*/
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"

	"github.com/bitly/little_bigtable/bttest"
	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/grpc"
)

var (
	host   = flag.String("host", "localhost", "the address to bind to on the local machine")
	port   = flag.Int("port", 9000, "the port number to bind to on the local machine")
	dbFile = flag.String("db-file", "little_bigtable.db", "path to data file")
)

const (
	maxMsgSize = 256 * 1024 * 1024 // 256 MiB
)

func main() {
	ctx := context.Background()
	grpc.EnableTracing = false
	flag.Parse()
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	if *dbFile == "" {
		log.Fatal("missing --db-file")
	}

	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared", *dbFile))
	if err != nil {
		log.Fatalf("failed creating mysql connection %v", err)
	}
	// db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	// // ping so we can log about connection errors early
	// // and so we can detect when the database doesn't exist
	// if err := db.Ping(); err != nil {
	// 	if e, ok := err.(*mysql.MySQLError); ok {
	// 		switch e.Number {
	// 		case 1049:
	// 			// Error 1049: Unknown database '...'
	// 			// this is fatal so we need to reconnect without the database name in DSN
	// 			err = bttest.CreateMysqlDatabase(ctx, cfg)
	// 			if err != nil {
	// 				log.Fatal(err)
	// 			}
	// 			err = db.Ping()
	// 		}
	// 	}
	// 	if err != nil {
	// 		log.Printf("warning: %s", err)
	// 	}
	// }
	err = bttest.CreateTables(ctx, db)
	if err != nil {
		log.Fatalf("%#v", err)
	}

	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
	}
	srv, err := bttest.NewServer(fmt.Sprintf("%s:%d", *host, *port), db, opts...)
	if err != nil {
		log.Fatalf("failed to start emulator: %v", err)
	}

	log.Printf("\"little\" Bigtable emulator running. DB:%s Connect with environment variable BIGTABLE_EMULATOR_HOST=%q", *dbFile, srv.Addr)
	select {}
}
