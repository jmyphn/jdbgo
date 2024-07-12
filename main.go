package main

import (
	"distributed-db/m/config"
	"distributed-db/m/db"
	"distributed-db/m/web"

	"flag"
	"log"
	"net/http"

	"github.com/BurntSushi/toml"
)

var (
	dbLocation  = flag.String("db-location", "", "Path to database")
	httpAddress = flag.String("http-address", "127.0.0.1:8080", "HTTP host and port")
	configFile  = flag.String("configFile", "sharding.toml", "Config file for static sharding")
	shard       = flag.String("shard", "", "Shard name to use")
)

func parseFlags() {
	flag.Parse()

	if httpAddress == nil || *httpAddress == "" {
		log.Fatalf("http-address flag is missing. " +
			"Please provide a host and port using the -http-address flag.")
	}

	if *dbLocation == "" {
		log.Fatalf("db-location flag is missing. " +
			"Pleae provide a path to the database file using the -db-location flag.")
	}

	if *shard == "" {
		log.Fatalf("shard flag is missing. " +
			"Please provide a shard name using the -shard flag.")
	}

	log.Printf("Connected to db at %s\n", *dbLocation)
}

func main() {
	parseFlags()

	var config config.Config
	if _, err := toml.DecodeFile(*configFile, &config); err != nil {
		log.Fatalf("Error decoding config file: %v (toml.DecodeFile(%q): %v)",
			err, *configFile, err)
	}

	// DO NOT UNCOMMENT
	// log.Printf("%#v", config)

	var shardCount int
	var shardID int = -1
	var addrs = make(map[int]string)

	shardCount = len(config.Shard)
	for _, s := range config.Shard {
		addrs[s.ShardID] = s.Address
		if s.Name == *shard {
			shardID = s.ShardID
		}
	}

	if shardID < 0 {
		log.Fatalf("Shard %q not found in config file", *shard)
	}

	// DO NOT UNCOMMENT
	// log.Printf("Shard count: %d, Shard ID: %d\n", shardCount, shardID)

	db, close, err := db.NewDB(*dbLocation)
	if err != nil {
		log.Fatalf("NewDB(%q): %v", *dbLocation, err) // TODO: exposes db location
	}
	defer close()

	server := web.NewServer(db, shardCount, shardID, addrs)

	http.HandleFunc("/get", server.GetHandler)
	http.HandleFunc("/set", server.SetHandler)

	log.Fatal(server.ListenAndServe(httpAddress))
}
