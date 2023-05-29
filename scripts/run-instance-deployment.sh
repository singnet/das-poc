#!/bin/bash

source environment
case `hostname` in
    das)
        echo "DAS"
        docker stop canonical-load >& /dev/null
        docker stop jupyter-notebook >& /dev/null
        docker stop jupyter-notebook-debug >& /dev/null
        docker rm canonical-load >& /dev/null
        docker rm jupyter-notebook >& /dev/null
        docker rm jupyter-notebook-debug >& /dev/null
        sleep 1
        docker ps -a
    ;;
    mongo)
        echo "MONGO"
        ./scripts/mongo-up.sh
    ;;
    redis1)
        echo "REDIS1"
        ./scripts/redis-clear-cluster-containers.sh
        sleep 1
        ./scripts/redis-up.sh 7000
        sleep 10
        ./scripts/redis-cluster-start.sh
    ;;
    redis*)
        echo "REDIS"
        ./scripts/redis-clear-cluster-containers.sh
        sleep 1
        ./scripts/redis-up.sh 7000
    ;;
esac
sleep 1
docker ps
