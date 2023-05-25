#!/bin/bash

echo "Starting MongoDB on port $DAS_MONGODB_PORT"

docker stop mongo_$DAS_MONGODB_PORT >& /dev/null
docker rm mongo_$DAS_MONGODB_PORT >& /dev/null
docker volume rm mongodbdata >& /dev/null
docker run \
    --detach \
    --name mongo_$DAS_MONGODB_PORT \
    --env MONGO_INITDB_ROOT_USERNAME=${DAS_DATABASE_USERNAME:-dbadmin} \
    --env MONGO_INITDB_ROOT_PASSWORD=${DAS_DATABASE_PASSWORD:-dassecret} \
    --env TZ=${TZ} \
    --network="host" \
    --volume /tmp:/tmp \
    --volume /mnt:/mnt \
    --volume mongodbdata:/data/db \
    mongo:latest \
    mongod --port $DAS_MONGODB_PORT
