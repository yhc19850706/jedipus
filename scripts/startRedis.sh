#!/bin/bash

name=$1
serverPort=$2
pass=$3

startPort=$4
numNodes=$5
endPort=$((startPort + numNodes - 1))

announceIp=$6

docker ps -a -q -f name="$name" | xargs docker rm -f

DOCKER_IMAGE="jamespedwards42/alpine-redis-testing:unstable"
MOUNT_MODULES="/redis/modules:/redis/modules"

docker run -d --name="$name-$serverPort" -p "$serverPort:$serverPort" -v "$MOUNT_MODULES" "$DOCKER_IMAGE" "$serverPort" 1 \
   --requirepass "$pass" --protected-mode no --save \"\" --repl-diskless-sync yes --appendfsync no --activerehashing no &

docker run -d --name="$name-cluster" -p "$startPort-$endPort:$startPort-$endPort" -v "$MOUNT_MODULES" "$DOCKER_IMAGE" "$startPort" "$numNodes" \
   --cluster-enabled yes --cluster-node-timeout 200 --cluster-require-full-coverage yes --cluster-announce-ip "$announceIp" \
   --repl-diskless-sync yes --appendfsync no --save \"\" \
   --protected-mode no --activerehashing no \
   --lazyfree-lazy-eviction yes --lazyfree-lazy-expire yes --lazyfree-lazy-server-del yes --slave-lazy-flush yes

wait

exit 0
