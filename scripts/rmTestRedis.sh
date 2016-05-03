#!/bin/bash

docker ps -a -q -f name=jedipus | xargs docker rm -f

exit 0
