#!/bin/bash

docker exec -it redis_7000 yes yes | redis-cli -p 7000 --cluster create  \
    192.168.1.8:7000 \
    192.168.1.8:7001 \
    192.168.1.8:7002 \
    192.168.1.136:7003 \
    192.168.1.136:7004 \
    192.168.1.136:7005 \
    --cluster-replicas 1
