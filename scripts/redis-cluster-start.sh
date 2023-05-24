#!/bin/bash

docker exec -it redis_7000 yes yes | redis-cli -p 7000 --cluster create  \
    45.32.140.218:7000 \
    45.63.84.83:7000 \
    45.32.141.233:7000 \
    --cluster-replicas 0
