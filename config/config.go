package config

import (
	"fmt"
	"hash/fnv"

	"github.com/BurntSushi/toml"
)

// Shard represents a shard that holds a subset of the data.
// Each shard has a unique set of keys
type Shard struct {
	Name    string
	ShardID int
	Address string
}

// Config represents the sharding configuration of the system.
type Config struct {
	Shards []Shard
}

// Shards is a representation of the sharding config: the shard count, the
// ID of the current shard, and the addresses of other shards
type Shards struct {
	Count int
	CurID int
	Addrs map[int]string
}

// ParseFile parses the config file and returns a Config struct upon success.
// Exits the program with error code 1 if the config file cannot be parsed.
func ParseFile(configFile string) (Config, error) {
	var c Config
	if _, err := toml.DecodeFile(configFile, &c); err != nil {
		return Config{}, err
	}
	return c, nil
}

// ParseShards converts and verifies the list of shards specified
// in the config file into a Shards struct, which can be used for routing
// by the server.
func ParseShards(shards []Shard, curShard string) (*Shards, error) {
	shardCount := len(shards)
	shardIdx := -1
	addrs := make(map[int]string)

	for _, s := range shards {
		if _, ok := addrs[s.ShardID]; ok {
			return nil, fmt.Errorf("duplicate shard ID %d", s.ShardID)
		}
		addrs[s.ShardID] = s.Address
		if s.Name == curShard {
			shardIdx = s.ShardID
		}
	}

	for i := 0; i < shardCount; i++ {
		if _, ok := addrs[i]; !ok {
			return nil, fmt.Errorf("shard %d not found in config file", i)
		}
	}

	if shardIdx < 0 {
		return nil, fmt.Errorf("shard %q not found in config file", curShard)
	}

	return &Shards{
		Count: shardCount,
		CurID: shardIdx,
		Addrs: addrs,
	}, nil
}

// Id returns the shard ID for the given key.
func (s *Shards) Id(key string) int {
	h := fnv.New64a()
	h.Write([]byte(key))
	return int(h.Sum64() % uint64(s.Count))
}
