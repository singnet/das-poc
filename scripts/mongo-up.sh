#!/bin/bash

docker stop das_mongo_1
docker rm das_mongo_1
docker run \
    --env MONGO_INITDB_ROOT_USERNAME=${DAS_DATABASE_USERNAME:-dbadmin}
    --env MONGO_INITDB_ROOT_PASSWORD=${DAS_DATABASE_PASSWORD:-dassecret}
    --env TZ=${TZ}
    --network="host"
    --volume /tmp:/tmp
    --volume /mnt:/mnt
    --volume mongodbdata:/data/db
    mongod --port ${DAS_MONGODB_PORT:-27017}
