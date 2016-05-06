#!/bin/bash

name=$1
serverPort=$2

startPort=$3
numNodes=$4
endPort=$((startPort + numNodes - 1))

announceIp=$5

docker ps -a -q -f name="$name" | xargs docker rm -f

docker run --name="$name"-"$serverPort" -d --publish="$serverPort":"$serverPort" jamespedwards42/alpine-redis-testing:unstable "$serverPort" 1 --requirepass 42 --save \"\" --repl-diskless-sync yes --appendfsync no --activerehashing no &

docker run -d --name="$name-cluster" -p "$startPort-$endPort:$startPort-$endPort" jamespedwards42/alpine-redis-testing:unstable "$startPort" "$numNodes" --cluster-enabled yes --appendfsync no --activerehashing no --cluster-announce-ip "$announceIp"

wait

exit 0
