package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/montanaflynn/stats"
)

var (
	N           = flag.Int("N", 10000, "Number of requests to make")
	addr        = flag.String("addr", "localhost:8080", "HTTP host port to connect to the server")
	threading   = flag.Bool("t", false, "Whether to run multiple processes")
	concurrency = flag.Int("c", 4, "Number of concurrent threads to run if threading is enabled")
	help        = flag.Bool("help", false, "Prints help message")
)

func errorHandler(name string, err error) {
	if err != nil {
		log.Fatalf("Error while calculating %s: %v", name, err)
	}
}

func benchmark(f func()) {
	start := time.Now()
	values := make([]float64, *N)
	for i := 0; i < *N; i++ {
		s := time.Now()
		f()
		values[i] = float64(time.Since(s))
	}
	median, err := stats.Median(values)
	errorHandler("Median", err)
	percentile90, err := stats.Percentile(values, 90)
	errorHandler("Percentile", err)
	percentile99, err := stats.Percentile(values, 99)
	errorHandler("Percentile", err)
	qps := float64(*N) / (float64(time.Since(start)) / float64(time.Second))
	fmt.Printf(
		"-------------------Results-------------------\n"+
			"Median: %.2fns\n"+
			"90th percentile: %.2fns\n"+
			"99th percentile: %.2fns\n"+
			"QPS: %.1f\n"+
			"---------------------------------------------\n",
		median, percentile90, percentile99, qps)
}

// writeRandKey writes a random key-value pair to the server.
func writeRandKey() {
	key := fmt.Sprintf("key_%d", rand.Intn(100000))
	val := fmt.Sprintf("value_%d", rand.Intn(100000))

	vals := url.Values{}
	vals.Set("key", key)
	vals.Set("value", val)

	resp, err := http.Get("http://" + *addr + "/set?key=" + vals.Encode())
	if err != nil {
		log.Fatalf("Error while setting value: %v", err)
	}
	defer resp.Body.Close()

	// fmt.Printf("Writing key %q with value %q\n", key, val)
}

func main() {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	flag.Parse()
	if *help {
		flag.PrintDefaults()
		return
	}

	// this writes to disk
	if *threading {
		fmt.Printf("Running benchmark with N = %d iterations and concurrency level %d\n",
			*N, *concurrency)

		var wg sync.WaitGroup
		for i := 0; i < *concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				benchmark(writeRandKey)
			}()
		}

		wg.Wait()
	} else {
		fmt.Printf("Running benchmark with N = %d iterations\n", *N)
		benchmark(writeRandKey)
	}
}
