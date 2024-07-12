package db_test

import (
	"bytes"
	"distributed-db/db"
	"os"
	"testing"
)

func TestGetSet(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "dbtest")
	if err != nil {
		t.Fatalf("Could not create a temp file: %v", err)
	}
	name := f.Name()
	f.Close()
	defer os.Remove(name)

	db, closeFunc, err := db.NewDB(name)
	if err != nil {
		t.Fatalf("NewDB: Could not create a new database: %v", err)
	}
	defer closeFunc()

	if err = db.SetKey("a", []byte("b")); err != nil {
		t.Fatalf("SetKey: Could not set key: %v", err)
	}

	val, err := db.GetKey("a")
	if err != nil {
		t.Fatalf("GetKey: Could not get key: %v", err)
	}

	if !bytes.Equal(val, []byte("b")) {
		t.Errorf("Unexpected value for key 'a': got %q, want %q", val, "b")
	}
}
