package db_test

import (
	"distributed-db/db"
	"os"
	"testing"
)

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

func TestDeleteExtraKeys(t *testing.T) {
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
