Scripts
===

These scripts run actions related to DAS context (Eg. conversion, loading).

## Run using docker-compose

Assuming there is a running console where the current directory is the root of this project:

```sh
# MongoDB variables
export DAS_DATABASE_HOSTNAME=localhost
export DAS_DATABASE_PORT=27017
# Change the following values when running on a public instance
export DAS_DATABASE_USERNAME=dbadmin
export DAS_DATABASE_PASSWORD=das#secret
docker-compose up -d
```

The command creates three containers:

- `das_couchbase_1`
- `das_mongo_1`
- `das_scripts_1`

At this moment:

- Is possible run scripts in `scripts` directory
- The mongo database instance is available on port `27017`
- There are some `.metta` files available in `/data` directory into the `das_scripts_1` container
    - Use `docker-compose exec scripts ls /data` to see them without need to attach the container.

### `das.py`

This script loads MeTTa files into a specified mongo database.

```sh
# show help message
docker-compose exec scripts python das.py --help

# load data from file.metta to mongo database with default config to connection
docker-compose exec scripts python das.py file.metta

# the following command load data from /data/Go-Plus-UBERON_2020-10-20.metta file
# into a mongo database named UBERON
docker-compose exec scripts python das.py -d UBERON /data/Go-Plus-UBERON_2020-10-20.metta
```

### Populating Couchbase from file

First, generate `(id, value)` file running:

```sh
# This will use the MongoDB "das" database
docker-compose exec scripts python das_generate_file.py --file-path /tmp/all_pairs.txt
# To run it with another database:
docker-compose exec scripts python das_generate_file.py --file-path /tmp/all_pairs.txt --mongo-database UBERON
```

Now, upload the data to couchbase by running:

```sh
docker-compose exec scripts python das_upload_to_couch_from_file.py --file-path /tmp/all_pairs.txt
```