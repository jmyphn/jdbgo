package web

import (
	"distributed-db/m/db"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
)

// Server contains HTTP method handlers for the database.
type Server struct {
	db         *db.DB
	shardID    int
	shardCount int
	addrs      map[int]string
}

// NewServer creates a new instance of Server
func NewServer(db *db.DB, shardCount int, shardID int, addrs map[int]string) *Server {
	return &Server{
		db:         db,
		shardCount: shardCount,
		shardID:    shardID,
		addrs:      addrs,
	}
}

func (s *Server) getShard(key string) int {
	h := fnv.New64a()
	h.Write([]byte(key))
	return int(h.Sum64() % uint64(s.shardCount))
}

func (s *Server) redirect(shard int, w http.ResponseWriter, r *http.Request) {
	url := "http://" + s.addrs[shard] + r.RequestURI
	fmt.Fprintf(w, "redirecting from shard %d to shard %d (%q)\n", s.shardID, shard, url)
	// http.Redirect(w, r, url, http.StatusTemporaryRedirect)

	resp, err := http.Get(url)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error redirecting the request: %v\n", err)
		return
	}
	defer resp.Body.Close()

	io.Copy(w, resp.Body)
}

// GetHandler handles GET requests to the server.
func (s *Server) GetHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Called get\n")
	r.ParseForm()
	key := r.Form.Get("key")

	shard := s.getShard(key)
	if shard != s.shardID {
		s.redirect(shard, w, r)
		return
	}

	value, err := s.db.GetKey(key)

	fmt.Fprintf(w, "Shard : %d, ShardID : %d, addr = %q Value : %q, Error: %v\n",
		shard, s.shardID, s.addrs[shard], value, err)
}

// SetHandler handles PUT requests to the server.
func (s *Server) SetHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Called set\n")
	r.ParseForm()
	key := r.Form.Get("key")
	value := r.Form.Get("value")

	shard := s.getShard(key)
	if shard != s.shardID {
		s.redirect(shard, w, r)
		return
	}

	err := s.db.SetKey(key, []byte(value))
	fmt.Fprintf(w, "Shard : %d, shardID : %d, Error : %v\n", shard, s.shardID, err)
}

// ListenAndServe starts the HTTP server.
func (s *Server) ListenAndServe(httpAddress *string) error {
	return http.ListenAndServe(*httpAddress, nil)
}
