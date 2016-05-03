#!/bin/bash

name=$1
serverPort=$2

nodePort=$3
numNodes=$4

docker --log-level=fatal ps -a -q -f name="$name" | xargs docker --log-level=fatal rm -f

docker run --name="$name"-"$serverPort" -d --publish="$serverPort":"$serverPort" redis:alpine redis-server --port "$serverPort" --requirepass pass --save \"\" --repl-diskless-sync yes &

for ((i = 0; i < numNodes; i++, nodePort++)); do

  docker run --name="$name"-"$nodePort" -d --publish="$nodePort":"$nodePort" redis:alpine redis-server --port "$nodePort" --save \"\" --cluster-enabled yes --repl-diskless-sync yes &
done

wait

exit 0
