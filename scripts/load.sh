#!/bin/bash

docker run \
    --name load \
    --env DAS_MONGODB_HOSTNAME=${DAS_MONGODB_HOSTNAME:-mongo} \
    --env DAS_MONGODB_PORT=${DAS_MONGODB_PORT:-27017} \
    --env DAS_REDIS_HOSTNAME=${DAS_REDIS_HOSTNAME:-redis} \
    --env DAS_REDIS_PORT=${DAS_REDIS_PORT:-6379} \
    --env DAS_DATABASE_USERNAME=${DAS_DATABASE_USERNAME:-dbadmin} \
    --env DAS_DATABASE_PASSWORD=${DAS_DATABASE_PASSWORD:-dassecret} \
    --env PYTHONPATH=/app \
    --env TZ=${TZ} \
    --network="host" \
    --volume /tmp:/tmp \
    --volume /mnt:/mnt \
    das:latest \
    python3 scripts/load_das.py --knowledge-base $1

docker rm load >& /dev/null
