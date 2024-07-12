#!/bin/bash
set -e

trap 'killall distributed-db' SIGINT

cd $(dirname $0)

killall distributed-db || true
sleep 0.5

go build -o distributed-db
mkdir databases || true

./distributed-db -db-location=databases/boston.db -http-address=127.0.0.1:8080 -configFile=sharding.toml -shard='Boston' &
./distributed-db -db-location=databases/new_york.db -http-address=127.0.0.1:8081 -configFile=sharding.toml -shard='New York' &
./distributed-db -db-location=databases/chicago.db -http-address=127.0.0.1:8082 -configFile=sharding.toml -shard='Chicago' &
./distributed-db -db-location=databases/san_francisco.db -http-address=127.0.0.1:8083 -configFile=sharding.toml -shard='San Francisco' &
./distributed-db -db-location=databases/denver.db -http-address=127.0.0.1:8084 -configFile=sharding.toml -shard='Denver' &

wait