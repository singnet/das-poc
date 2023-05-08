#!/bin/bash

docker build --no-cache -t das \
    --build-arg USER_ID=$(id -u) \
    --build-arg GROUP_ID=$(id -g) \
    -f Dockerfile .
