package main

import (
	"distributed-db/config"
	"distributed-db/db"
	"distributed-db/server"

	"flag"
	"log"
	"net/http"
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

	c, err := config.ParseFile(*configFile)
	if err != nil {
		log.Fatalf("ParseFile: error parsing file %q: %v", *configFile, err)
	}

	shards, err := config.ParseShards(c.Shards, *shard)
	if err != nil {
		log.Fatalf("ParseShards: %v", err)
	}

	db, close, err := db.NewDB(*dbLocation)
	if err != nil {
		log.Fatalf("NewDB(%q): %v", *dbLocation, err) // TODO: exposes db location
	}
	defer close()

	server := server.NewServer(db, shards)

	http.HandleFunc("/get", server.GetHandler)
	http.HandleFunc("/set", server.SetHandler)

	log.Fatal(server.ListenAndServe(httpAddress))
}
