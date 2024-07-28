#!/bin/bash

for shard in 127.0.0.0:8080 127.0.0.1:8080 127.0.0.2:8080 127.0.0.3:8080 127.0.0.4:8080 127.0.0.5:8080 127.0.0.6:8080 127.0.0.7:8080; do
  echo $shard
  for i in {1...1000}; do
    curl "http://$shard/set?key=$RANDOM&value=value-$RANDOM"
  done
done