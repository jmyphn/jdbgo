package web_handlers

import (
	"distributed-db/m/db"
	"fmt"
	"net/http"
)

// Server contains HTTP method handlers for the database.
type Server struct {
	db *db.DB
}

// NewServer creates a new instance of Server
func NewServer(db *db.DB) *Server {
	return &Server{db: db}
}

// GetHandler handles GET requests to the server.
func (s *Server) GetHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Called get\n")
	r.ParseForm()
	key := r.Form.Get("key")

	value, err := s.db.GetKey(key)
	fmt.Fprintf(w, "Value : %q, Error: %v", value, err)
	fmt.Fprintf(w, "\n")
}

// SetHandler handles SET requests to the server.
func (s *Server) SetHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Called set\n")
	r.ParseForm()
	key := r.Form.Get("key")
	value := r.Form.Get("value")

	err := s.db.SetKey(key, []byte(value))
	fmt.Fprintf(w, "Error: %v", err)
	fmt.Fprintf(w, "\n")
}

// ListenAndServe starts the HTTP server.
func (s *Server) ListenAndServe(httpAddress *string) error {
	return http.ListenAndServe(*httpAddress, nil)
}
