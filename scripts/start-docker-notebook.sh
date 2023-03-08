#!/bin/bash

docker-compose exec app jupyter-notebook --ip 0.0.0.0 --port 8887 --no-browser --allow-root
