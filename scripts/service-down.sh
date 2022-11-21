#!/bin/bash

docker-compose -f docker-compose-service.yml down
docker volume rm das_couchbasedata
docker volume rm das_mongodbdata
docker volume rm das_couchbasesetup
