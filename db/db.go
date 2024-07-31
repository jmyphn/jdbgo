package db

import (
	"bytes"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// DB is a wrapper around the BoltDB database.
type DB struct {
	db       *bolt.DB
	readOnly bool
}

var defaultBucket = []byte("default")
var replicateBucket = []byte("replication")

// NewDB returns an instance of a database.
func NewDB(dbPath string, readOnly bool) (db *DB, closeFunc func() error, err error) {
	boltDB, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, nil, err
	}

	//	boltDB.NoSync = true

	db = &DB{db: boltDB, readOnly: readOnly}
	closeFunc = boltDB.Close

	if err := db.createBuckets(); err != nil {
		closeFunc()
		return nil, nil, fmt.Errorf("creating default bucket: %w", err)
	}

	return db, closeFunc, nil
}

// create a bucket in the database
func (d *DB) createBuckets() error {
	return d.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(defaultBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(replicateBucket); err != nil {
			return err
		}
		return nil
	})
}

// SetKey sets a key in the database. Returns an error if the operation fails.
func (d *DB) SetKey(key string, value []byte) error {
	if d.readOnly {
		return errors.New("read-only mode")
	}

	return d.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(defaultBucket).Put([]byte(key), value); err != nil {
			return err
		}

		return tx.Bucket(replicateBucket).Put([]byte(key), value)
	})
}

// SetKeyOnReplica sets the key to the requested value into the default
// database. It does not write to the replication queue.
// This method is only intended to be used on replicas.
func (d *DB) SetKeyOnReplica(key string, value []byte) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(defaultBucket).Put([]byte(key), value)
	})
}

// copyByteSlice copies a byte slice into a new byte slice. Returns nil if the
// input slice is nil.
func copyByteSlice(b []byte) []byte {
	if b == nil {
		return nil
	}
	res := make([]byte, len(b))
	copy(res, b)
	return res
}

// GetNextKeyForReplication gets the key and value for the keys that have
// changed and have not been updated in the replica database(s).
// If no keys are found, nil key and nil value are returned
func (d *DB) GetNextKeyForReplication() (key, value []byte, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicateBucket)
		k, v := b.Cursor().First()
		key = copyByteSlice(k)
		value = copyByteSlice(v)
		copy(key, k)
		copy(value, v)
		return nil
	})

	if err != nil {
		return nil, nil, err
	}
	return key, value, nil
}

// DeleteReplicationKey deletes the key from the replication queue
// if the value matches the contents or if the key is already absent.
func (d *DB) DeleteReplicationKey(key, value []byte) (err error) {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicateBucket)

		v := b.Get(key)
		if v == nil {
			return errors.New("key not found")
		}

		if !bytes.Equal(v, value) {
			return errors.New("value mismatch")
		}

		return b.Delete(key)
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

// DeleteExtraKeys deletes all keys that do not belong to the current shard.
func (d *DB) DeleteExtraKeys(isExtra func(string) bool) error {
	var keys []string
	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		return b.ForEach(func(k, v []byte) error {
			ks := string(k)
			if isExtra(ks) {
				keys = append(keys, ks)
			}
			return nil
		})
	})

	if err != nil {
		return err
	}

	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)

		for _, k := range keys {
			if err := b.Delete([]byte(k)); err != nil {
				return err
			}
		}
		return nil
	})

}
