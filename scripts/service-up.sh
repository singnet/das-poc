#!/bin/bash

docker-compose -f docker-compose-service.yml up --detach
./scripts/couchbase_setup.sh
