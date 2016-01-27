package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/shaleman/raft/hraft/raft"
	"github.com/shaleman/raft/hraft/raft-boltdb"
	"github.com/ugorji/go/codec"

	log "github.com/Sirupsen/logrus"
)

// MockFSM is an implementation of the FSM interface, and just stores
// the logs sequentially.
type MockFSM struct {
	sync.Mutex
	logs [][]byte
}

type MockSnapshot struct {
	logs     [][]byte
	maxIndex int
}

func (m *MockFSM) Apply(log *raft.Log) interface{} {
	m.Lock()
	defer m.Unlock()
	m.logs = append(m.logs, log.Data)
	return len(m.logs)
}

func (m *MockFSM) Snapshot() (raft.FSMSnapshot, error) {
	m.Lock()
	defer m.Unlock()
	return &MockSnapshot{m.logs, len(m.logs)}, nil
}

func (m *MockFSM) Restore(inp io.ReadCloser) error {
	m.Lock()
	defer m.Unlock()
	defer inp.Close()
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(inp, &hd)

	m.logs = nil
	return dec.Decode(&m.logs)
}

func (m *MockSnapshot) Persist(sink raft.SnapshotSink) error {
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(sink, &hd)
	if err := enc.Encode(m.logs[:m.maxIndex]); err != nil {
		sink.Cancel()
		return err
	}
	sink.Close()
	return nil
}

func (m *MockSnapshot) Release() {
}

var raftServ *raft.Raft

func setupRaft(listenUrl string, path string) (*raft.Raft, error) {
	// create the transport
	trans, err := raft.NewTCPTransport(listenUrl, nil, 2, time.Second, nil)
	if err != nil {
		log.Errorf("NewTCPTransport failed. Err: %v", err)
		return nil, err
	}

	// Create the BoltDB backend
	stable, err := raftboltdb.NewBoltStore(filepath.Join(path, "raft.db"))
	if err != nil {
		log.Errorf("NewBoltStore failed. Err: %v", err)
		return nil, err
	}

	// Wrap the store in a LogCache to improve performance
	logStore, err := raft.NewLogCache(512, stable)
	if err != nil {
		stable.Close()
		log.Errorf("NewLogCache failed. Err: %v", err)
		return nil, err
	}

	// Create the snapshot store
	snap, err := raft.NewFileSnapshotStore(path, 3, nil)
	if err != nil {
		stable.Close()
		log.Errorf("NewFileSnapshotStore failed. Err: %v", err)
		return nil, err
	}

	// Setup the peer store
	peers := raft.NewJSONPeers(path, trans)

	// setup config
	config := raft.DefaultConfig()
	config.EnableSingleNode = true

	// Create new raft server
	raftInst, err := raft.NewRaft(config, &MockFSM{}, logStore, stable, snap, peers, trans)
	if err != nil {
		stable.Close()
		log.Errorf("NewRaft failed. Err: %v", err)
		return nil, err
	}

	return raftInst, nil
}

func InitHttp(listenUrl string) {
	router := mux.NewRouter()
	router.HandleFunc("/db/{key}", readHandler).Methods("GET")
	router.HandleFunc("/db/{key}", writeHandler).Methods("POST")
	router.HandleFunc("/join", joinHandler).Methods("POST")

	log.Println("Listening at:", listenUrl)

	http.ListenAndServe(listenUrl, router)
}

func joinHandler(w http.ResponseWriter, req *http.Request) {
	// Read the value from the POST body.
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	joinUrl := string(b)

	log.Infof("Joining client %s to cluster", joinUrl)

	future := raftServ.AddPeer(joinUrl)
	if err := future.Error(); err != nil {
		log.Errorf("join err: %v", err)
	}
}

var forwarding_enabled = false

func readHandler(w http.ResponseWriter, req *http.Request) {
	var value string

	vars := mux.Vars(req)

	log.Infof("Reading %s", vars["key"])

	w.Write([]byte(value))
}

func writeHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)

	// Read the value from the POST body.
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	value := string(b)

	log.Infof("Writing key %s, value %s.", vars["key"], value)

	// Execute the command against the Raft server.
	raftServ.Apply([]byte("test"), 0)
	w.Write([]byte(value))
}

func main() {
	var raftPort int
	var httpPort int
	var path string
	var err error

	flag.StringVar(&path, "p", "", "path")
	flag.IntVar(&raftPort, "r", 9501, "raft port")
	flag.IntVar(&httpPort, "h", 9502, "http port")
	flag.Parse()

	raftServ, err = setupRaft(fmt.Sprintf("localhost:%d", raftPort), path)
	if err != nil {
		log.Fatalf("Raft init failed. Err: %v", err)
	}
	InitHttp(fmt.Sprintf("localhost:%d", httpPort))
}
