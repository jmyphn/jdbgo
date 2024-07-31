package replication

import (
	"bytes"
	"distributed-db/db"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"
)

// NextKeyValue is a struct to hold the next key-value pair for replication.
type NextKeyValue struct {
	Key   string
	Value string
	Err   error
}

type client struct {
	db       *db.DB
	mainAddr string
}

// ClientLoop continuously polls the server for new key-value pairs to replicate.
func ClientLoop(db *db.DB, addr string) {
	c := &client{db: db, mainAddr: addr}
	for {
		present, err := c.loop()

		if err != nil {
			log.Printf("ClientLoop: %v\n", err)
			time.Sleep(time.Second)
			continue
		}

		if !present {
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (c *client) loop() (present bool, err error) {
	resp, err := http.Get("http://" + c.mainAddr + "/next-replication-key")
	if err != nil {
		return false, err
	}

	var res NextKeyValue
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return false, err
	}

	defer resp.Body.Close()

	if res.Err != nil {
		return false, res.Err
	}

	if res.Key == "" {
		return false, nil
	}

	if err := c.db.SetKeyOnReplica(res.Key, []byte(res.Value)); err != nil {
		return false, err
	}

	if err := c.deleteFromReplicationQueue(res.Key, res.Value); err != nil {
		log.Printf("DeleteKeyFromReplication failed: %v", err)
	}

	return true, nil
}

func (c *client) deleteFromReplicationQueue(key, value string) error {
	u := url.Values{}
	u.Set("key", key)
	u.Set("value", value)

	log.Printf("Deleting key=%q, value=%q, from replication queue on %q", key, value, c.mainAddr)

	resp, err := http.Get("http://" + c.mainAddr + "/delete-replication-key?" + u.Encode())
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	res, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	res = bytes.TrimSpace(res)

	if !bytes.Equal(res, []byte("ok")) {
		return errors.New(string(res))
	}
	return nil
}
