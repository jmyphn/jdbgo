package db

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// DB is a wrapper around the BoltDB database.
type DB struct {
	db *bolt.DB
}

var defaultBucket = []byte("default")

// NewDB returns an instance of a database.
func NewDB(dbPath string) (db *DB, closeFunc func() error, err error) {
	boltDB, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, nil, err
	}

	db = &DB{db: boltDB}
	closeFunc = boltDB.Close

	if err := db.createDefaultBucket(); err != nil {
		closeFunc()
		return nil, nil, fmt.Errorf("creating default bucket: %w", err)
	}

	return db, closeFunc, nil
}

// create a bucket in the database
func (d *DB) createDefaultBucket() error {
	return d.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(defaultBucket)
		return err
	})
}

// SetKey sets a key in the database. Returns an error if the operation fails.
func (d *DB) SetKey(key string, value []byte) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		return b.Put([]byte(key), value)
	})
}

// GetKey gets the value of a given key in the requested database.
func (d *DB) GetKey(key string) ([]byte, error) {
	var result []byte
	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		result = b.Get([]byte(key))
		return nil
	})
	if err == nil {
		return result, nil
	}
	return nil, err
}
