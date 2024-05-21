/*
little_bigtable launches the sqlite3 backed Bigtable emulator on the given address.
*/
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/bitly/little_bigtable/bttest"
	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/grpc"
)

const (
	maxMsgSize = 256 * 1024 * 1024 // 256 MiB
	version    = "0.1.1"
)

func main() {
	host := flag.String("host", "localhost", "the address to bind to on the local machine")
	port := flag.Int("port", 9000, "the port number to bind to on the local machine")
	dbFile := flag.String("db-file", "little_bigtable.db", "path to data file")
	showVersion := flag.Bool("version", false, "show version")

	ctx := context.Background()
	grpc.EnableTracing = false
	flag.Parse()
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	if *showVersion {
		fmt.Printf("little_bigtable v%s (built w/%s)", version, runtime.Version())
		os.Exit(0)
	}

	if *dbFile == "" {
		log.Fatal("missing --db-file")
	}

	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared", *dbFile))
	if err != nil {
		log.Fatalf("failed creating sqlite3 connection %v", err)
	}
	db.SetMaxOpenConns(1)

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
