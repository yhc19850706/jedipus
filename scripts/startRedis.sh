#!/bin/bash

name=$1
serverPort=$2

nodePort=$3
numNodes=$4

docker ps -a -q -f name="$name" | xargs docker rm -f

docker run --name="$name"-"$serverPort" -d --publish="$serverPort":"$serverPort" redis:alpine redis-server --port "$serverPort" --requirepass 42 --save \"\" --repl-diskless-sync yes &

nodePortC=$nodePort
publish=""
for ((i = 0; i < numNodes; i++, nodePortC++)); do
   publish="$publish --publish=$nodePortC:$nodePortC"
done

docker run -d --name="$name-cluster" $publish jamespedwards42/alpine-redis-testing:unstable redis-server "$nodePort" "$numNodes" --cluster-announce-ip 127.0.0.1

wait

exit 0 
