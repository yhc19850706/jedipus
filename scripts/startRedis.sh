#!/bin/bash

name=$1
serverPort=$2

startPort=$3
numNodes=$4
endPort=$((startPort + numNodes - 1))

announceIp=$5

docker ps -a -q -f name="$name" | xargs docker rm -f

docker run --name="$name"-"$serverPort" -d --publish="$serverPort":"$serverPort" jamespedwards42/alpine-redis-testing:unstable "$serverPort" 1 --requirepass 42 --protected-mode no --save \"\" --repl-diskless-sync yes --appendfsync no --activerehashing no &

docker run -d --name="$name-cluster" -p "$startPort-$endPort:$startPort-$endPort" jamespedwards42/alpine-redis-testing:unstable "$startPort" "$numNodes" \
   --cluster-enabled yes --cluster-node-timeout 200 --cluster-require-full-coverage yes --cluster-announce-ip "$announceIp" \
   --repl-diskless-sync yes --appendfsync no --save \"\" \
   --protected-mode no --activerehashing no \
   --lazyfree-lazy-eviction yes --lazyfree-lazy-expire yes --lazyfree-lazy-server-del yes --slave-lazy-flush yes

wait

exit 0
