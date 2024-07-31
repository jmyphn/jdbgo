package server_test

import (
	"bytes"
	"distributed-db/config"
	"distributed-db/db"
	"distributed-db/server"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func createShardDB(t *testing.T, id int) *db.DB {
	t.Helper()

	tmp, err := os.CreateTemp(os.TempDir(), fmt.Sprintf("dbtest-%d", id))
	if err != nil {
		t.Fatalf("Could not create a temp db: %v", err)
	}
	name := tmp.Name()
	tmp.Close()

	db, closeFunc, err := db.NewDB(name, false)
	if err != nil {
		t.Fatalf("NewDB: Could not create a new database: %v", err)
	}

	t.Cleanup(func() {
		closeFunc()
		os.Remove(name)
	})

	return db
}

func createShardServer(t *testing.T, id int, addrs map[int]string) (*db.DB, *server.Server) {
	t.Helper()

	db := createShardDB(t, id)
	shards := &config.Shards{
		CurID: id,
		Addrs: addrs,
		Count: len(addrs),
	}

	s := server.NewServer(db, shards)
	return db, s
}

func TestServerCreate(t *testing.T) {
	var GetHandler1, SetHandler1 func(w http.ResponseWriter, r *http.Request)
	var GetHandler2, SetHandler2 func(w http.ResponseWriter, r *http.Request)

	one := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.RequestURI, "/get") {
			GetHandler1(w, r)
		} else if strings.HasPrefix(r.RequestURI, "/set") {
			SetHandler1(w, r)
		}
	}))
	defer one.Close()

	two := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.RequestURI, "/get") {
			GetHandler2(w, r)
		} else if strings.HasPrefix(r.RequestURI, "/set") {
			SetHandler2(w, r)
		}
	}))
	defer two.Close()

	addrs := map[int]string{
		0: strings.TrimPrefix(one.URL, "http://"),
		1: strings.TrimPrefix(two.URL, "http://"),
	}

	db1, server1 := createShardServer(t, 0, addrs)
	db2, server2 := createShardServer(t, 1, addrs)

	// manual calculation
	keys := map[string]int{
		"a": 0,
		"b": 1,
	}

	GetHandler1 = server1.GetHandler
	SetHandler1 = server1.SetHandler
	GetHandler2 = server2.GetHandler
	SetHandler2 = server2.SetHandler

	for key := range keys {
		_, err := http.Get(fmt.Sprintf(one.URL+"/set?key=%s&value=value-%s", key, key))
		if err != nil {
			t.Fatalf("Could not set key %q: %v", key, err)
		}
	}

	for key := range keys {
		resp, err := http.Get(fmt.Sprintf(one.URL+"/get?key=%s", key))
		if err != nil {
			t.Fatalf("Could not get key %q: %v", key, err)
		}
		contents, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("Could not read contents of key %q: %v", key, err)
		}

		want := []byte("value-" + key)
		if !bytes.Contains(contents, want) {
			t.Errorf("Unexpected contents for key %q: got %q, want %q", key, contents, want)
		}

		log.Printf("Key %q: %q\n", key, contents)
	}

	val1, err := db1.GetKey("a")
	if err != nil {
		t.Fatalf("GetKey: Could not get key: %v", err)
	}

	want1 := "value-a"
	if !bytes.Equal(val1, []byte(want1)) {
		t.Errorf("Unexpected value for key 'a': got %q, want %q", val1, want1)
	}

	val2, err := db2.GetKey("b")
	if err != nil {
		t.Fatalf("GetKey: Could not get key: %v", err)
	}

	want2 := "value-b"
	if !bytes.Equal(val2, []byte(want2)) {
		t.Errorf("Unexpected value for key 'b': got %q, want %q", val2, want2)
	}
}
