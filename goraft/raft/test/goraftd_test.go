package main

import (
	"math/rand"
	"os"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/shaleman/raft/goraft/raft"
)

func setGetTest(t *testing.T, ws *Server, rs *Server, key, value string) {
	log.Debugf("Writing key %s value %s to :%s", ws.connectionString())

	err := ws.WriteKey(key, value)
	if err != nil {
		t.Fatalf("Error setting the key. Err: %v", err)
	}

	log.Debugf("Reading key %s from %s", key, rs.connectionString())

	gval, err := rs.ReadKey(key)
	if err != nil {
		t.Fatalf("Test failed. Read error: %v", err)
	}
	if gval != value {
		t.Fatalf("Test failed. Expecting %s. Got %v", value, gval)
	}
}

func TestRaftServer(t *testing.T) {
	// cleanup old state
	os.RemoveAll("server1")
	os.RemoveAll("server2")
	os.RemoveAll("server3")

	// create directories for servers
	os.Mkdir("server1", 0777)
	os.Mkdir("server2", 0777)
	os.Mkdir("server3", 0777)

	// cleanup old state
	defer os.RemoveAll("server1")
	defer os.RemoveAll("server2")
	defer os.RemoveAll("server3")

	// raft.SetLogLevel(raft.Trace)
	rand.Seed(time.Now().UnixNano())

	// Setup commands.
	raft.RegisterCommand(&WriteCommand{})

	// Create servers
	server1 := NewServer("server1", "localhost", 9501)
	server2 := NewServer("server2", "localhost", 9502)
	server3 := NewServer("server3", "localhost", 9503)

	// start and join them
	go server1.ListenAndServe("")

	time.Sleep(1 * time.Second)

	go server2.ListenAndServe("localhost:9501")
	go server3.ListenAndServe("localhost:9501")

	time.Sleep(1 * time.Second)

	log.Infof("Server1: %s, leader: %s", server1.raftServer.Name(), server1.raftServer.Leader())
	for pidx, peer := range server1.raftServer.Peers() {
		log.Infof("Peer: %+v[%+v]", pidx, peer)
	}

	// write key to one server and read from all
	setGetTest(t, server1, server1, "testKey", "testValue1")
	setGetTest(t, server1, server2, "testKey", "testValue2")
	setGetTest(t, server1, server3, "testKey", "testValue3")
	setGetTest(t, server2, server2, "testKey", "testValue4")
	setGetTest(t, server3, server3, "testKey", "testValue5")
	setGetTest(t, server2, server3, "testKey", "testValue6")

}
