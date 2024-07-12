package config_test

import (
	"distributed-db/config"
	"os"
	"reflect"
	"testing"
)

func createConfig(t *testing.T, contents string) config.Config {
	t.Helper()

	f, err := os.CreateTemp(os.TempDir(), "config.toml")
	if err != nil {
		t.Fatalf("Could not create a temp file: %v", err)
	}
	defer f.Close()

	name := f.Name()
	defer os.Remove(name)

	_, err = f.WriteString(contents)
	if err != nil {
		t.Fatalf("Could not write to file: %v", err)
	}

	c, err := config.ParseFile(name)
	if err != nil {
		t.Fatalf("ParseFile: error parsing file %q: %v", name, err)
	}
	return c
}

func TestConfigParse(t *testing.T) {
	got := createConfig(t, `[[shards]]
	name = "shard1"
	shardID = 0
	address = "localhost:8080"`)

	want := config.Config{
		Shards: []config.Shard{
			{
				Name:    "shard1",
				ShardID: 0,
				Address: "localhost:8080",
			},
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Mismatch config file: got %#v, want %#v", got, want)
	}

}

func TestParseShards(t *testing.T) {
	c := createConfig(t, `[[shards]]
	name = "shard1"
	shardID = 0
	address = "localhost:8080"
	[[shards]]
	name = "shard2"
	shardID = 1
	address = "localhost:8081"`)

	shards, err := config.ParseShards(c.Shards, "shard2")
	if err != nil {
		t.Fatalf("ParseShards: %v", err)
	}

	want := &config.Shards{
		Count: 2,
		CurID: 1,
		Addrs: map[int]string{
			0: "localhost:8080",
			1: "localhost:8081",
		},
	}

	if !reflect.DeepEqual(shards, want) {
		t.Errorf("Mismatch shards: got %#v, want %#v", shards, want)
	}

}
