#!/bin/sh

MODULE=$1

gcc -shared -o "$MODULE".o -fPIC "$MODULE".c

exit 0
