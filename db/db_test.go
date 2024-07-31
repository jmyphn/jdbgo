package db_test

import (
	"bytes"
	"distributed-db/db"
	"os"
	"testing"
)

func createTempDb(t *testing.T, readOnly bool) *db.DB {
	t.Helper()

	f, err := os.CreateTemp(os.TempDir(), "dbtest")
	if err != nil {
		t.Fatalf("Could not create a temp file: %v", err)
	}
	name := f.Name()
	f.Close()
	t.Cleanup(func() { os.Remove(name) })

	db, closeFunc, err := db.NewDB(name, readOnly)
	if err != nil {
		t.Fatalf("NewDB: Could not create a new database: %v", err)
	}
	t.Cleanup(func() { closeFunc() })

	return db
}

func setKey(t *testing.T, db *db.DB, key string, value string) {
	t.Helper()

	if err := db.SetKey(key, []byte(value)); err != nil {
		t.Fatalf("SetKey(%q, %q): failed to set key %q: %v", key, value, key, err)
	}
}

func getKey(t *testing.T, db *db.DB, key string) string {
	t.Helper()

	val, err := db.GetKey(key)
	if err != nil {
		t.Fatalf("GetKey(%q): failed to get key %q: %v", key, key, err)
	}

	return string(val)
}

func TestGetSet(t *testing.T) {
	db := createTempDb(t, false)

	setKey(t, db, "a", "b")

	if value := getKey(t, db, "a"); value != "b" {
		t.Errorf("Unexpected value for key 'a': got %q, want %q", value, "b")
	}

	value := getKey(t, db, "b")
	if value != "" {
		t.Errorf("Unexpected value for key 'b': got %q, want %q", value, "")
	}

	value = getKey(t, db, "a")
	if !bytes.Equal([]byte(value), []byte("b")) {
		t.Errorf("Bytes.Equal failed")
	}

	k, v, err := db.GetNextKeyForReplication()
	if err != nil {
		t.Fatalf("GetNextKeyForReplication: got error %v, want nil", err)
	}

	if !bytes.Equal(k, []byte("a")) || !bytes.Equal(v, []byte("b")) || err != nil {
		t.Errorf("GetNextKeyForReplication: got (%q, %q, %v), want (%q, %q, nil)", k, v, err, "a", "b")
	}
}

func TestDeleteReplicationKey(t *testing.T) {
	db := createTempDb(t, false)

	setKey(t, db, "a", "b")

	k, v, err := db.GetNextKeyForReplication()
	if err != nil {
		t.Fatalf("GetNextKeyForReplication: got error %v, want nil", err)
	}

	if !bytes.Equal(k, []byte("a")) || !bytes.Equal(v, []byte("b")) {
		t.Errorf("GetNextKeyForReplication: got (%q, %q, %v), want (%q, %q, nil)", k, v, err, "a", "b")
	}

	if err := db.DeleteReplicationKey([]byte("a"), []byte("c")); err == nil {
		t.Fatalf("DeleteReplicationKey(%q, %q): got nil error, want non-nil error", k, "c")
	}

	if err := db.DeleteReplicationKey([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("DeleteReplicationKey(%q, %q): got error %v, want nil", k, v, err)
	}

	k, v, err = db.GetNextKeyForReplication()
	if err != nil {
		t.Fatalf("GetNextKeyForReplication: got error %v, want nil", err)
	}

	if k != nil || v != nil {
		t.Errorf("GetNextKeyForReplication: got (%q, %q), want (nil, nil)", k, v)
	}
}

func TestSetReadOnly(t *testing.T) {
	db := createTempDb(t, true)

	if err := db.SetKey("a", []byte("b")); err == nil {
		t.Fatalf("SetKey(%q, %q): got nil error, wanted non-nil error", "a", "b")
	}
}

func TestDeleteExtraKeys(t *testing.T) {
	db := createTempDb(t, false)

	setKey(t, db, "a", "b")
	setKey(t, db, "b", "c")

	if err := db.DeleteExtraKeys(func(name string) bool {
		return name == "b"
	}); err != nil {
		t.Fatalf("Could not delete extra keys: %v", err)
	}

	if value := getKey(t, db, "a"); value != "b" {
		t.Errorf("Unexpected value for key 'a' after deleting extra keys: "+
			"got %q, want %q", value, "b")
	}

	if value := getKey(t, db, "b"); value != "" {
		t.Errorf("Unexpected value for key 'b' after deleting extra keys: "+
			"got %q, want %q", value, "")
	}
}
