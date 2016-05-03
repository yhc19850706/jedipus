#!/bin/bash

serverPort=$1
numNodes=$2
nodePort=$3

docker ps -a -q -f name=jedipus | xargs docker rm -f

docker run --name=jedipus-"$serverPort" -d --publish="$serverPort":"$serverPort" redis:alpine redis-server --port "$serverPort" --requirepass pass --save \"\" --repl-diskless-sync yes &

for ((i = 0; i < numNodes; i++, nodePort++)); do

  docker run --name=jedipus-"$nodePort" -d --publish="$nodePort":"$nodePort" redis:alpine redis-server --port "$nodePort" --save \"\" --cluster-enabled yes --repl-diskless-sync yes &
done

wait

exit 0
