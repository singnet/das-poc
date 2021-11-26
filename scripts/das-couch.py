import argparse
import os
import logging

from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.management.collections import CollectionSpec
from couchbase import exceptions as cb_exceptions

from pymongo.collection import Collection
from pymongo.mongo_client import MongoClient

logger = logging.getLogger("das")
logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

formatter = logging.Formatter("[%(asctime)s %(levelname)s]: %(message)s")
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

INCOMING_COLL_NAME = 'IncomingSet'
OUTGOING_COLL_NAME = 'OutgoingSet'


def append(coll, key, new_value):
    value = []
    try:
        result = coll.get(key)
        value = result.content
    except:
        pass
    value.extend(new_value)
    coll.upsert(key, list(set(value)))


def populate_sets(collection: Collection, bucket):
    incoming_set = bucket.collection(INCOMING_COLL_NAME)
    outgoing_set = bucket.collection(OUTGOING_COLL_NAME)
    cursor = collection.find({}, no_cursor_timeout=True).batch_size(100)
    for doc in cursor:
        _id = doc['_id']
        if 'keys' in doc:
            keys = doc['keys']
        else:
            keys = [v for k, v in doc.items() if k.startswith('key')]
        append(outgoing_set, key=_id, new_value=keys)
        for key in keys:
            append(incoming_set, key=key, new_value=[_id])
    cursor.close()


def create_collections(bucket, collections_names=None):
    if collections_names is None:
        collections_names = []
    # Creating Couchbase collections
    coll_manager = bucket.collections()
    for name in collections_names:
        print(f'Creating Couchbase collection: "{name}"...')
        try:
            coll_manager.create_collection(CollectionSpec(name))
        except cb_exceptions.CollectionAlreadyExistsException as _:
            print(f'Collection exists!')
            pass
        except Exception as e:
            print(e)


def get_mongodb(mongo_hostname, mongo_port, mongo_username, mongo_password, mongo_database):
    client = MongoClient(f"mongodb://{mongo_username}:{mongo_password}@{mongo_hostname}:{mongo_port}")
    return client[mongo_database]


def main(mongo_hostname, mongo_port, mongo_username, mongo_password, mongo_database):
    cluster = Cluster(
        'couchbase://localhost',
        authenticator=PasswordAuthenticator(mongo_username, mongo_password))
    bucket = cluster.bucket('das')

    create_collections(
        bucket=bucket,
        collections_names=[INCOMING_COLL_NAME, OUTGOING_COLL_NAME])

    db = get_mongodb(mongo_hostname, mongo_port, mongo_username, mongo_password, mongo_database)

    logger.info("Indexing links_1")
    populate_sets(db['links_1'], bucket)
    logger.info("Indexing links_2")
    populate_sets(db['links_2'], bucket)
    logger.info("Indexing links_3")
    populate_sets(db['links_3'], bucket)
    logger.info("Indexing links")
    populate_sets(db['links'], bucket)


def run():
    parser = argparse.ArgumentParser(
        "Indexes DAS to Couchbase", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--mongo-hostname",
        type=str,
        default=os.environ.get("DAS_MONGO_HOSTNAME", "localhost"),
        metavar="HOSTNAME",
        dest="mongo_hostname",
        help="mongo hostname to connect to",
    )
    parser.add_argument(
        "--mongo-port",
        type=int,
        default=os.environ.get("DAS_MONGO_PORT", "27017"),
        metavar="PORT",
        dest="mongo_port",
        help="mongo port to connect to",
    )
    parser.add_argument(
        "--mongo-database",
        type=str,
        default="das",
        metavar="NAME",
        dest="mongo_database",
        help="mongo database name to connect to",
    )
    parser.add_argument(
        "--mongo-username",
        type=str,
        default=os.environ.get("DAS_MONGO_USERNAME", "mongoadmin"),
        metavar="USERNAME",
        dest="mongo_username",
        help="mongo username",
    )
    parser.add_argument(
        "--mongo-password",
        type=str,
        default=os.environ.get("DAS_MONGO_PASSWORD", "das#secret"),
        metavar="PASSWORD",
        dest="mongo_password",
        help="mongo password",
    )
    args = parser.parse_args()
    main(**vars(args))


if __name__ == '__main__':
    run()
