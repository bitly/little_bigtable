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
	"time"

	"github.com/bitly/little_bigtable/bttest"
	"github.com/go-sql-driver/mysql"
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
	ctx := context.Background()
	grpc.EnableTracing = false
	flag.Parse()
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	cfg, err := mysql.ParseDSN(*dsn)
	if err != nil {
		log.Fatalf("error connecting to database %#v", err)
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

	// ping so we can log about connection errors early
	// and so we can detect when the database doesn't exist
	if err := db.Ping(); err != nil {
		if e, ok := err.(*mysql.MySQLError); ok {
			switch e.Number {
			case 1049:
				// Error 1049: Unknown database '...'
				// this is fatal so we need to reconnect without the database name in DSN
				err = bttest.CreateMysqlDatabase(ctx, cfg)
				if err != nil {
					log.Fatal(err)
				}
				err = db.Ping()
			}
		}
		if err != nil {
			log.Printf("warning: %s", err)
		}
	}
	err = bttest.CreateTables(ctx, db)
	if err != nil {
		log.Fatal(err)
	}

	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
	}
	srv, err := bttest.NewServer(fmt.Sprintf("%s:%d", *host, *port), db, opts...)
	if err != nil {
		log.Fatalf("failed to start emulator: %v", err)
	}

	log.Printf("\"little\" Bigtable emulator running. Connect with environment variable BIGTABLE_EMULATOR_HOST=%q", srv.Addr)
	select {}
}
