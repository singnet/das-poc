Scripts
===

This scripts run actions related to DAS context (Eg. conversion, loading).

## Run using docker-compose

Assuming there is a running console where the current directory is the root of this project:

```sh
docker-compose up -d
```

If this is the first time this command is executed two containers was created:
- `das_mongo_1`
- `das_scripts_1`

At this moment:
- Is possible run scripts in `scripts` directory
- The mongo database instance is available on port `27017`
- There are some `.metta` files available in `/data` directory
    - Use `docker-compose exec scripts ls /data` to see them without need to attach the container.


### `das.py`

This script load MeTTa files into a specified mongo database.

```sh
# show help message
docker-compose exec scripts python das.py --help

# load data from file.metta to mongo database with default config to connection
docker-compose exec scripts python das.py file.metta

# the following command load data from /data/Go-Plus-UBERON_2020-10-20.metta file
# into a mongo database named UBERON
docker-compose exec scripts python das.py -d UBERON /data/Go-Plus-UBERON_2020-10-20.metta
```
