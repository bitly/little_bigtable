/*
little_bigtable launches the mysql backed Bigtable emulator on the given address.
*/
package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/bitly/little_bigtable/bttest"
	"google.golang.org/grpc"
)

var (
	host = flag.String("host", "localhost", "the address to bind to on the local machine")
	port = flag.Int("port", 9000, "the port number to bind to on the local machine")
)

const (
	maxMsgSize = 256 * 1024 * 1024 // 256 MiB
)

func main() {
	grpc.EnableTracing = false
	flag.Parse()
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

