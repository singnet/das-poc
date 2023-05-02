#!/bin/bash

docker exec -it redis_7000 redis-cli -p 7000 --cluster create  \
    127.0.0.1:7000 \
    127.0.0.1:7001 \
    127.0.0.1:7002 \
    tornado:7000 \
    tornado:7001 \
    tornado:7002 \
    --cluster-replicas 1
