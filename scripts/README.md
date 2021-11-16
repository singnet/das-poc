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

At this moment is possible run scripts in `scripts` directory.


### `das.py`

This script load MeTTa files into a specified mongo database.

```sh
# show help message
docker-compose exec scripts python das.py --help

# load data from file.metta to mongo database with default config to connection
docker-compose exec scripts python das.py file.metta
```
