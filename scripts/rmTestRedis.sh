#!/bin/bash

docker --log-level=fatal ps -a -q -f name=jedipus | xargs docker --log-level=fatal rm -f

exit 0
