package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/shaleman/raft/goraft/raft"
)

// The key-value database.
type DB struct {
	data  map[string]string
	mutex sync.RWMutex
}

// Creates a new database.
func NewDB() *DB {
	return &DB{
		data: make(map[string]string),
	}
}

// Retrieves the value for a given key.
func (db *DB) Get(key string) string {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	return db.data[key]
}

// Sets the value for a given key.
func (db *DB) Put(key string, value string) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	db.data[key] = value
}

// This command writes a value to a key.
type WriteCommand struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Creates a new write command.
func NewWriteCommand(key string, value string) *WriteCommand {
	return &WriteCommand{
		Key:   key,
		Value: value,
	}
}

// The name of the command in the log.
func (c *WriteCommand) CommandName() string {
	return "write"
}

// Writes a value to a key.
func (c *WriteCommand) Apply(server raft.Server) (interface{}, error) {
	db := server.Context().(*DB)
	db.Put(c.Key, c.Value)
	return nil, nil
}

// The raftd server is a combination of the Raft server and an HTTP
// server which acts as the transport.
type Server struct {
	name       string
	host       string
	port       int
	path       string
	router     *mux.Router
	raftServer raft.Server
	httpServer *http.Server
	db         *DB
	mutex      sync.RWMutex
}

// Creates a new server.
func NewServer(path string, host string, port int) *Server {
	s := &Server{
		host:   host,
		port:   port,
		path:   path,
		db:     NewDB(),
		router: mux.NewRouter(),
	}

	// Read existing name or generate a new one.
	if b, err := ioutil.ReadFile(filepath.Join(path, "name")); err == nil {
		s.name = string(b)
	} else {
		s.name = fmt.Sprintf("%07x", rand.Int())[0:7]
		if err = ioutil.WriteFile(filepath.Join(path, "name"), []byte(s.name), 0644); err != nil {
			panic(err)
		}
	}

	return s
}

// Returns the connection string.
func (s *Server) connectionString() string {
	return fmt.Sprintf("http://%s:%d", s.host, s.port)
}

// Starts the server.
func (s *Server) ListenAndServe(leader string) error {
	var err error

	log.Printf("Initializing Raft Server: %s", s.path)

	// Initialize and start Raft server.
	transporter := raft.NewHTTPTransporter("/raft", 200*time.Millisecond)
	s.raftServer, err = raft.NewServer(s.name, s.path, transporter, nil, s.db, "")
	if err != nil {
		log.Fatal(err)
	}
	transporter.Install(s.raftServer, s)
	s.raftServer.Start()

	if leader != "" {
		// Join to leader if specified.

		log.Println("Attempting to join leader:", leader)

		if !s.raftServer.IsLogEmpty() {
			log.Fatal("Cannot join with an existing log")
		}
		if err := s.Join(leader); err != nil {
			log.Fatal(err)
		}

	} else if s.raftServer.IsLogEmpty() {
		// Initialize the server by joining itself.

		log.Println("Initializing new cluster")

		_, err := s.raftServer.Do(&raft.DefaultJoinCommand{
			Name:             s.raftServer.Name(),
			ConnectionString: s.connectionString(),
		})
		if err != nil {
			log.Fatal(err)
		}

	} else {
		log.Println("Recovered from log")
	}

	log.Println("Initializing HTTP server")

	// Initialize and start HTTP server.
	s.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: s.router,
	}

	s.router.HandleFunc("/db/{key}", s.readHandler).Methods("GET")
	s.router.HandleFunc("/db/{key}", s.writeHandler).Methods("POST")
	s.router.HandleFunc("/join", s.joinHandler).Methods("POST")

	log.Println("Listening at:", s.connectionString())

	return s.httpServer.ListenAndServe()
}

// This is a hack around Gorilla mux not providing the correct net/http
// HandleFunc() interface.
func (s *Server) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.router.HandleFunc(pattern, handler)
}

// Joins to the leader of an existing cluster.
func (s *Server) Join(leader string) error {
	command := &raft.DefaultJoinCommand{
		Name:             s.raftServer.Name(),
		ConnectionString: s.connectionString(),
	}

	var b bytes.Buffer
	json.NewEncoder(&b).Encode(command)
	resp, err := http.Post(fmt.Sprintf("http://%s/join", leader), "application/json", &b)
	if err != nil {
		return err
	}
	resp.Body.Close()

	return nil
}

func (s *Server) WriteKey(key, value string) error {
	resp, err := http.Post(fmt.Sprintf("%s/db/%s", s.connectionString(), key),
		"application/json", bytes.NewBuffer([]byte(value)))
	if err != nil {
		return err
	}
	resp.Body.Close()

	return nil
}

func (s *Server) ReadKey(key string) (string, error) {
	var value string
	resp, err := http.Get(fmt.Sprintf("%s/db/%s", s.connectionString(), key))
	if err != nil {
		return value, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return value, err
	}
	value = string(b)
	resp.Body.Close()

	return value, nil
}

func (s *Server) joinHandler(w http.ResponseWriter, req *http.Request) {
	command := &raft.DefaultJoinCommand{}

	if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Infof("Joining client {%+v} to server %s", command, s.connectionString())
	if _, err := s.raftServer.Do(command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

var forwarding_enabled = false

func (s *Server) readHandler(w http.ResponseWriter, req *http.Request) {
	var value string

	vars := mux.Vars(req)

	if forwarding_enabled && s.raftServer.Name() != s.raftServer.Leader() {
		leader := s.raftServer.Peers()[s.raftServer.Leader()]
		url := fmt.Sprintf("%s/db/%s", leader.ConnectionString, vars["key"])
		log.Infof("Reading from: %s", url)
		resp, err := http.Get(url)
		if err != nil {
			log.Errorf("Error during http get. Err: %v", err)
			w.WriteHeader(500)
			return
		}
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			w.WriteHeader(500)
			return
		}
		value = string(b)
		log.Infof("Got resp: %s", value)
		resp.Body.Close()
	} else {
		value = s.db.Get(vars["key"])
		log.Infof("Got value: %s", value)

	}

	w.Write([]byte(value))
}

func (s *Server) writeHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)

	// Read the value from the POST body.
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	value := string(b)

	log.Infof("%s: Writing key %s, value %s. Leader(%s)", s.connectionString(), vars["key"], value, s.raftServer.Leader())

	// Execute the command against the Raft server.
	_, err = s.raftServer.Do(NewWriteCommand(vars["key"], value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}
