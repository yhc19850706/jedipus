#!/bin/bash

docker ps -a -q -f name="$1" | xargs docker rm -f

exit 0
