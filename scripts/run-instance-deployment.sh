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
        sleep 5
	./scripts/build-das.sh
        docker ps -a
    ;;
    mongo)
        echo "MONGO"
        ./scripts/mongo-up.sh
    ;;
    redis1)
        echo "REDIS1"
	echo "Shuting down container..."
        ./scripts/redis-clear-cluster-containers.sh
        echo "Done"
        echo "Press <ENTER> when all cluster elements have been shut down"
	read NOP
        ./scripts/redis-up.sh 7000
        sleep 5
        ./scripts/redis-cluster-start.sh
    ;;
    redis*)
        echo "REDIS"
	echo "Shuting down container..."
        ./scripts/redis-clear-cluster-containers.sh
        echo "Done"
        echo "Press <ENTER> when all cluster elements have been shut down"
	read NOP
        ./scripts/redis-up.sh 7000
    ;;
esac
sleep 1
docker ps
