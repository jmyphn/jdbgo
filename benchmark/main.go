package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/montanaflynn/stats"
)

var (
	help        = flag.Bool("help", false, "Prints help message")
	write       = flag.Int("write-iter", 2500, "Number of write requests to make")
	read        = flag.Int("read-iter", 2500, "Number of read requests to make")
	addr        = flag.String("addr", "localhost:8080", "HTTP host port to connect to the server")
	concurrency = flag.Int("c", 4, "Number of concurrent threads to run")
)

var httpClient = http.Client{
	Transport: &http.Transport{
		IdleConnTimeout:     90 * time.Second,
		MaxConnsPerHost:     32,
		MaxIdleConns:        32,
		MaxIdleConnsPerHost: 32,
	},
}

func errorHandler(name string, err error) {
	if err != nil {
		log.Fatalf("Error while calculating %s: %v", name, err)
	}
}

func benchmark(f func() string, iters int, concurrency int) (qps float64, strs []string) {

	start := time.Now()
	values := make([]float64, iters)
	for i := 0; i < iters; i++ {
		s := time.Now()
		strs = append(strs, f())
		values[i] = float64(time.Since(s))
	}
	median, err := stats.Median(values)
	errorHandler("Median", err)
	percentile90, err := stats.Percentile(values, 90)
	errorHandler("Percentile", err)
	percentile99, err := stats.Percentile(values, 99)
	errorHandler("Percentile", err)
	qps = float64(iters) / (float64(time.Since(start)) / float64(time.Second))
	fmt.Printf(
		"-------------------Results %d-------------------\n"+
			"Median: %.2fns\n"+
			"90th percentile: %.2fns\n"+
			"99th percentile: %.2fns\n"+
			"QPS: %.1f\n"+
			"---------------------------------------------\n",
		concurrency, median, percentile90, percentile99, qps)
	return qps, strs
}

// writeRandKey writes a random key-value pair to the server.
func writeRandKey() string {
	key := fmt.Sprintf("key_%d", rand.Intn(100000))
	val := fmt.Sprintf("value_%d", rand.Intn(100000))

	vals := url.Values{}
	vals.Set("key", key)
	vals.Set("value", val)

	resp, err := httpClient.Get("http://" + *addr + "/set?key=" + vals.Encode())
	errorHandler("Set", err)

	io.Copy(io.Discard, resp.Body)
	defer resp.Body.Close()

	return key
	// fmt.Printf("Writing key %q with value %q\n", key, val)
}

func readRandKey(allKeys []string) string {
	key := allKeys[rand.Intn(len(allKeys))]

	vals := url.Values{}
	vals.Set("key", key)

	resp, err := httpClient.Get("http://" + *addr + "/get?" + vals.Encode())
	errorHandler("Get", err)
	defer resp.Body.Close()

	return key
}

func benchmarkWrite() (allKeys []string) {

	var wg sync.WaitGroup
	var mu sync.Mutex
	var totalQps float64

	fmt.Printf("Running benchmark with N = %d writes and concurrency level %d\n",
		*write, *concurrency)

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			qps, strs := benchmark(writeRandKey, *write, i)
			mu.Lock()
			allKeys = append(allKeys, strs...)
			totalQps += qps
			mu.Unlock()
		}()
	}

	wg.Wait()
	log.Printf("Write total QPS: %.2f, set %d keys\n", totalQps, len(allKeys))
	return allKeys
}

func benchmarkRead(allKeys []string) {

	var totalQps float64
	var mu sync.Mutex
	var wg sync.WaitGroup

	log.Printf("Now running %d reads\n", *read)

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			qps, _ := benchmark(func() string { return readRandKey(allKeys) }, *read, i)
			mu.Lock()
			totalQps += qps
			mu.Unlock()
		}()
	}
	wg.Wait()

	log.Printf("Read total QPS: %.2f on %d keys\n", totalQps, len(allKeys))
}

func main() {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	flag.Parse()
	if *help {
		flag.PrintDefaults()
		return
	}

	allKeys := benchmarkWrite()

	// uncomement below to run in writes and reads in parallel
	// go benchmarkWrite()
	benchmarkRead(allKeys)
}
