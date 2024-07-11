package main

import (
	"distributed-db/m/db"
	"distributed-db/m/web_handlers"
	"flag"
	"fmt"
	"log"
	"net/http"
)

var (
	dbLocation  = flag.String("db-location", "", "path to BoltDB database")
	httpAddress = flag.String("http-address", "", "HTTP host and port")
)

func parseFlags() {
	flag.Parse()
	if *dbLocation == "" {
		log.Fatalf("db-location flag is missing. " +
			"Pleae provide a path to the database file using the -db-location flag.")
	}
	fmt.Printf("Connected to db at %s\n", *dbLocation)
}

func main() {
	parseFlags()
	db, close, err := db.NewDB(*dbLocation)
	if err != nil {
		log.Fatalf("NewDB(%q): %v", *dbLocation, err) // TODO: exposes db location
	}
	defer close()

	server := web_handlers.NewServer(db)

	http.HandleFunc("/get", server.GetHandler)
	http.HandleFunc("/set", server.SetHandler)

	log.Fatal(server.ListenAndServe(httpAddress))
}
