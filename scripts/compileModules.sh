#!/bin/sh

MODULE=$1 

gcc -dynamic -fno-common -std=gnu99 -c -o "$MODULE".o "$MODULE".c

ld -o "$MODULE".so "$MODULE".o -bundle -undefined dynamic_lookup -lc

exit 0
