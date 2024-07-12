package config

// Shard represents a shard that holds a subset of the data.
// Each shard has a unique set of keys
type Shard struct {
	Name    string
	ShardID int
	Address string
}

// Config represents the sharding configuration of the system.
type Config struct {
	Shard []Shard
}
