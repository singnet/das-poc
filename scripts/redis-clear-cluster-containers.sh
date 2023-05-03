#!/bin/bash

docker stop $(docker ps | grep redis_ | awk '{print $1}') >& /dev/null
docker rm $(docker ps -a | grep redis_ | awk '{print $1}') >& /dev/null

