#!/bin/bash

docker build -t das_service \
    --build-arg USER_ID=$(id -u) \
    --build-arg GROUP_ID=$(id -g) \
    -f Dockerfile.service .
