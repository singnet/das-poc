#!/bin/bash

docker stop redis >& /dev/null
docker rm redis >& /dev/null
docker run \
    --detach \
    --name redis \
    --env TZ=${TZ} \
    --env REDIS_ARGS="--port ${DAS_REDIS_PORT:-6379}" \
    --network="host" \
    --volume /tmp:/tmp \
    --volume /mnt:/mnt \
    redis/redis-stack-server:latest
