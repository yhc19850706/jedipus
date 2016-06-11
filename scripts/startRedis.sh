#!/bin/bash

name=$1
serverPort=$2
pass=$3

startPort=$4
numNodes=$5
endPort=$((startPort + numNodes - 1))

announceIp=$6

stunnelPort=$7

docker ps -a -q -f name="$name" | xargs docker rm -f

DOCKER_IMAGE="jamespedwards42/alpine-redis-testing:unstable"
MOUNT_MODULES="$(pwd)/redis/modules:/redis/modules"
REDIS_CONTAINER_NAME="$name-$serverPort"

docker run -d --name="$REDIS_CONTAINER_NAME" -p "$serverPort:$serverPort" -v "$MOUNT_MODULES" "$DOCKER_IMAGE" "$serverPort" 1 \
   --requirepass "$pass" --protected-mode no --save \"\" --repl-diskless-sync yes --appendfsync no --activerehashing no &

docker run -d --name="$name-cluster" -p "$startPort-$endPort:$startPort-$endPort" -v "$MOUNT_MODULES" "$DOCKER_IMAGE" "$startPort" "$numNodes" \
   --cluster-enabled yes --cluster-node-timeout 200 --cluster-require-full-coverage yes --cluster-announce-ip "$announceIp" \
   --repl-diskless-sync yes --appendfsync no --save \"\" \
   --protected-mode no --activerehashing no &

docker pull jamespedwards42/alpine-stunnel:latest
perl -pi -e "s/accept.*/accept = $stunnelPort/" $(pwd)/stunnel/stunnel.conf

wait

REDIS_CONTAINER_IP=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' "$REDIS_CONTAINER_NAME")
perl -pi -e "s/connect.*/connect = $REDIS_CONTAINER_IP:$serverPort/" "$(pwd)/stunnel/stunnel.conf"
docker run -d -p "$stunnelPort:$stunnelPort" --name="$name-stunnel" -v "$(pwd)/stunnel:/etc/stunnel/" jamespedwards42/alpine-stunnel:latest

exit 0
