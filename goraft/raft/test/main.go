package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/shaleman/raft/goraft/raft"
)

func main() {
	var trace bool
	var debug bool
	var host string
	var port int
	var name string
	var join string

	flag.BoolVar(&trace, "trace", false, "Raft trace debugging")
	flag.BoolVar(&debug, "debug", false, "Raft debugging")
	flag.StringVar(&host, "h", "localhost", "hostname")
	flag.StringVar(&name, "n", "", "name")
	flag.IntVar(&port, "p", 9501, "port")
	flag.StringVar(&join, "join", "", "host:port of leader to join")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s \n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	// raft.SetLogLevel(raft.Trace)
	rand.Seed(time.Now().UnixNano())

	// Setup commands.
	raft.RegisterCommand(&WriteCommand{})

	// Create servers
	server := NewServer(name, host, port)

	// start and join them
	server.ListenAndServe(join)

	time.Sleep(300 * time.Hour)
}
