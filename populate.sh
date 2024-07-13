#!/bin/bash

for shard in localhost:8080 localhost:8081 localhost:8082 localhost:8083 localhost:8084 localhost:8085 localhost:8086 localhost:8087; do
  echo $shard
  for i in {1...1000}; do
    curl "http://$shard/set?key=$RANDOM&value=value-$RANDOM"
  done
done