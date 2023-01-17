DAS
===

These scripts run actions related to DAS context (Eg. conversion, loading).

## Run using docker-compose

Assuming there is a running console where the current directory is the root of this project:

```sh
# MongoDB variables
export DAS_MONGODB_HOSTNAME=mongo
export DAS_MONGODB_PORT=27017
# Couchbase variables (Using 8Gb of RAM)
export DAS_COUCHBASE_HOSTNAME=couchbase
export DAS_COUCHBASE_BUCKET_RAMSIZE=$((8*1024))
# Change the following values when running on a public instance (used by MongoDB and Couchbase)
export DAS_DATABASE_USERNAME=dbadmin
export DAS_DATABASE_PASSWORD=dassecret

# Build and run containers
docker-compose up -d

# Setup Couchbase
./scripts/couchbase_test_setup.sh
# > INFO: Waiting for Couchbase...
# > SUCCESS: Cluster initialized
# > SUCCESS: Bucket created
# > SUCCESS: Couchbase is ready.
```

The command creates three containers:

- `das-couchbase-1`
- `das-mongo-1`
- `das-app-1`

At this moment:

- Is possible run scripts in `das` directory
- The mongo database instance is available on port `27017`
- There are some `.metta` files available in `/data` directory into the `das-app-1` container
    - Use `docker-compose exec app ls /data` to see them without need to attach the container.

### Stop and reset environment

```
docker-compose rm -s -f -v
docker volume rm das_couchbasedata das_couchbasesetup das_mongodbdata
```

### Uploading MeTTa data to MongoDB

The `load_das.py` script loads MeTTa files into a mongo database.

```sh
# show help message
docker-compose exec app python scripts/load_das.py --help

# load data from ./data/annotation_service/ChEBI2Reactome_PE_Pathway.txt_2020-10-20.metta
# to mongo database with default config to connection
docker-compose exec app python scripts/load_das.py --knowledge-base \
    ./data/annotation_service/ChEBI2Reactome_PE_Pathway.txt_2020-10-20.metta
```

### Populating Couchbase from file

First, generate `(id, value)` file running:

```sh
# This will use the MongoDB "das" database
docker-compose exec app python das/das_generate_file.py --file-path /tmp/all_pairs.txt
# To run it against a specific mongo database:
docker-compose exec app python das/das_generate_file.py --file-path /tmp/all_pairs.txt -d UBERON
# To run it using a index file
docker-compose exec app python das/das_generate_file.py --file-path /tmp/all_pairs.txt --index-path /tmp/index.txt
# Where a valid content of `/tmp/index.txt` would be:
# Evaluation 2 2 3
```

Now, upload the data to couchbase by running:

```sh
docker-compose exec app python das/das_upload_to_couch_from_file.py --file-path /tmp/all_pairs.txt
```

## Tests

There are 2 exceptional tests here to pay attention to:

- `das/pattern_matcher/pattern_matcher_test.py`; and
- `das/pattern_matcher/regression.py`.

Both them are runned with different commands.

The first one is runned with the following command:
```bash
pytest das/pattern_matcher/pattern_matcher_test.py
```
> It is runned directly, not being captured just by running `pytest`.

The second one is an integration test that is runned as follows:
```bash
PYTHONPATH=.:$PYTHONPATH python scripts/regression.py
```

## Docker Volumes

As we are using docker containers to run the databases, there is a need to create volumes to persitent the data between inevitable containers restarts.  
Whenever a database reset is needed, the volumes should be removes.

First, identify the volumes that are being used:
```bash
# Listing volumes
docker volume ls
```

There are at least this 2 volumes:
```
# DRIVER    VOLUME NAME
# local     das_couchbasedata
# local     das_mongodbdata
```

More information about the volumes can be obtained by running `docker volume inspect VOLUME_NAME` command.

Once the volume targeted to be deleted was identified, remove it by running:

```bash
# Removing volume
docker volume rm VOLUME_NAME
```

> **Warning:** This action will remove the volume and all the data inside.
