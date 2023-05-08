#!/bin/bash

if [ -z "$1" ]
then
    PORT=${DAS_REDIS_PORT:-6379}
else
    PORT=$1
fi

echo "Starting Redis on port $PORT"

cp ./redis.conf /tmp/redis_$1.conf
echo "port $PORT" >> /tmp/redis_$PORT.conf

docker stop redis_$PORT >& /dev/null
docker rm redis_$PORT >& /dev/null

docker run \
    --detach \
    --name redis_$PORT \
    --env TZ=${TZ} \
    --network="host" \
    --volume /tmp:/tmp \
    --volume /mnt:/mnt \
    redis/redis-stack-server:latest \
    redis-server /tmp/redis_$PORT.conf
