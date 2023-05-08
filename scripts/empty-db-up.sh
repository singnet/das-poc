#!/bin/bash

docker stop mongo_27018 >& /dev/null
docker rm mongo_27018 >& /dev/null
docker volume rm mongodbdata >& /dev/null
./scripts/mongo-up.sh
./scripts/redis-up.sh
