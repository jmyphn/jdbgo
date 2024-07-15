package server

import (
	"distributed-db/config"
	"distributed-db/db"
	"fmt"
	"io"
	"net/http"
)

// Server contains HTTP method handlers for the database.
type Server struct {
	db     *db.DB
	shards *config.Shards
}

// NewServer creates a new instance of Server
func NewServer(db *db.DB, shards *config.Shards) *Server {
	return &Server{
		db:     db,
		shards: shards,
	}
}

func (s *Server) redirect(shard int, w http.ResponseWriter, r *http.Request) {
	url := "http://" + s.shards.Addrs[shard] + r.RequestURI
	fmt.Fprintf(w, "redirecting from shard %d to shard %d (%q)\n", s.shards.CurID, shard, url)
	// http.Redirect(w, r, url, http.StatusTemporaryRedirect)

	resp, err := http.Get(url)
	if err != nil {
		fmt.Fprintf(w, "Error redirecting the request: %v\n", err)
		return
	}
	defer resp.Body.Close()

	io.Copy(w, resp.Body)
}

// GetHandler handles GET requests to the server.
func (s *Server) GetHandler(w http.ResponseWriter, r *http.Request) {
	// fmt.Fprintf(w, "Called get\n")
	r.ParseForm()
	key := r.Form.Get("key")

	shard := s.shards.Id(key)
	if shard != s.shards.CurID {
		s.redirect(shard, w, r)
		return
	}

	value, err := s.db.GetKey(key)

	fmt.Fprintf(w, "Shard : %d, ShardID : %d, addr = %q Value : %q, Error: %v\n",
		shard, s.shards.CurID, s.shards.Addrs[shard], value, err)
}

// SetHandler handles PUT requests to the server.
func (s *Server) SetHandler(w http.ResponseWriter, r *http.Request) {
	// fmt.Fprintf(w, "Called set\n")
	r.ParseForm()
	key := r.Form.Get("key")
	value := r.Form.Get("value")

	shard := s.shards.Id(key)
	if shard != s.shards.CurID {
		s.redirect(shard, w, r)
		return
	}

	err := s.db.SetKey(key, []byte(value))
	fmt.Fprintf(w, "Shard : %d, shardID : %d, Error : %v\n", shard, s.shards.CurID, err)
}

// ListenAndServe starts the HTTP server.
func (s *Server) ListenAndServe(httpAddress *string) error {
	return http.ListenAndServe(*httpAddress, nil)
}

// DeleteExtraKeysHandler deletes all keys that do not belong to the current shard.
func (s *Server) DeleteExtraKeysHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Error: %v\n", s.db.DeleteExtraKeys(func(key string) bool {
		return s.shards.Id(key) != s.shards.CurID
	}))
}
