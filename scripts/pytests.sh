#!/bin/bash

./scripts/empty-db-up.sh
./scripts/load.sh /data/samples/animals.metta

docker run \
    --name pytests \
    --env DAS_MONGODB_HOSTNAME=${DAS_MONGODB_HOSTNAME:-mongo} \
    --env DAS_MONGODB_PORT=${DAS_MONGODB_PORT:-27017} \
    --env DAS_REDIS_HOSTNAME=${DAS_REDIS_HOSTNAME:-redis} \
    --env DAS_REDIS_PORT=${DAS_REDIS_PORT:-6379} \
    --env DAS_DATABASE_USERNAME=${DAS_DATABASE_USERNAME:-dbadmin} \
    --env DAS_DATABASE_PASSWORD=${DAS_DATABASE_PASSWORD:-dassecret} \
    --env TZ=${TZ} \
    --network="host" \
    --volume /tmp:/tmp \
    das:latest \
    pytest \
        das/metta_lex_test.py \
        das/metta_yacc_test.py \
        das/atomese_lex_test.py \
        das/atomese_yacc_test.py\
        das/database/redis_mongo_db_test.py \
        das/distributed_atom_space_test.py \
        das/pattern_matcher/pattern_matcher_test.py \

docker rm pytests >& /dev/null

docker run \
    --name pytests \
    --env DAS_MONGODB_HOSTNAME=${DAS_MONGODB_HOSTNAME:-mongo} \
    --env DAS_MONGODB_PORT=${DAS_MONGODB_PORT:-27017} \
    --env DAS_REDIS_HOSTNAME=${DAS_REDIS_HOSTNAME:-redis} \
    --env DAS_REDIS_PORT=${DAS_REDIS_PORT:-6379} \
    --env DAS_DATABASE_USERNAME=${DAS_DATABASE_USERNAME:-dbadmin} \
    --env DAS_DATABASE_PASSWORD=${DAS_DATABASE_PASSWORD:-dassecret} \
    --env TZ=${TZ} \
    --network="host" \
    --volume /tmp:/tmp \
    das:latest \
    pytest --disable-warnings das/das_update_test.py

docker rm pytests >& /dev/null
