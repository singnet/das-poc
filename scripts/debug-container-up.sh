#!/bin/bash

./scripts/empty-db-up.sh
./scripts/load.sh /data/samples/animals.metta

docker run \
    --name debug \
    --env DAS_MONGODB_HOSTNAME=${DAS_MONGODB_HOSTNAME:-mongo} \
    --env DAS_MONGODB_PORT=${DAS_MONGODB_PORT:-27017} \
    --env DAS_REDIS_HOSTNAME=${DAS_REDIS_HOSTNAME:-redis} \
    --env DAS_REDIS_PORT=${DAS_REDIS_PORT:-6379} \
    --env DAS_DATABASE_USERNAME=${DAS_DATABASE_USERNAME:-dbadmin} \
    --env DAS_DATABASE_PASSWORD=${DAS_DATABASE_PASSWORD:-dassecret} \
    --env TZ=${TZ} \
    --env PYTHONPATH=/app/das:/app/das/das \
    --network="host" \
    --volume /tmp:/tmp \
    --volume $(pwd):/app/das \
    -ti \
    das:latest \
    bash

docker rm debug >& /dev/null
