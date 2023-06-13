#!/bin/bash

if [ "$#" -lt 2 ]
then
  echo "Usage: jupyter-notebook-custom.sh <tag> <port>"
  echo "example: jupyter-notebook-custom.sh senna 9001"
  exit 1
fi

PORT=$2

docker run \
    --name jupyter-notebook-$1 \
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
    --volume /opt/das/data:/data \
    --detach \
    das:latest \
    jupyter-notebook --ip 0.0.0.0 --port $PORT --no-browser --allow-root

echo "URL: http://`curl https://ipinfo.io/ip`:${PORT}/tree/notebooks"
echo "Token: `docker exec jupyter-notebook-senna jupyter notebook list | tail -1 | cut -d" " -f1 | cut -d"=" -f2`"
