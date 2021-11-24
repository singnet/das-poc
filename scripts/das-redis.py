import argparse
import os
from typing import Any
import logging

import redis
from pymongo.collection import Collection
from pymongo.mongo_client import MongoClient
from redis.client import Redis

from helpers import extract_by_prefix

logger = logging.getLogger("das")
logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

formatter = logging.Formatter("[%(asctime)s %(levelname)s]: %(message)s")
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

OUTGOING_SET = 'OutgoingSet:{}'.format
INCOMING_SET = 'IncomingSet:{}'.format
R_INCOMING_SET = 'RecursiveIncomingSet:{}'.format
R_OUTGOING_SET = 'RecursiveOutgoingSet:{}'.format

def populate_sets(collection: Collection, r: Redis):
    cursor = collection.find()
    for doc in cursor:
        _id = doc['_id']
        if 'keys' in doc:
            keys = doc['keys']
        else:
            keys = [v for k, v in doc.items() if k.startswith('key')]
        r.sadd(OUTGOING_SET(_id), *keys)
        for key in keys:
            r.sadd(INCOMING_SET(key), _id)

def populate_recursive_sets(fn_origin, fn_dest, r: Redis):
    keys = { key for key in r.keys(fn_origin("*")) }

    for key in keys:
        members = r.smembers(key)
        recursive_set = members.copy()
        #  print('->', key, members)
        while recur_keys := [m for m in members if fn_origin(m) in keys]:
            members = set()
            for recur_key in recur_keys:
                recur_members = r.smembers(fn_origin(recur_key))
                recursive_set.update(recur_members)
                members.update(recur_members)

        #  print('                                             ->', recursive_set)
        #  print()
        r.sadd(fn_dest(key.split(':')[-1]), *recursive_set)


def main(redis_args: dict[str, Any], mongo_args: dict[str, Any]):
    r = redis.StrictRedis(**redis_args, charset='utf-8', decode_responses=True)

    database = mongo_args.pop('database')
    db = MongoClient(**mongo_args)[database]

    logger.info("Indexing links_1")
    populate_sets(db['links_1'], r)
    logger.info("Indexing links_2")
    populate_sets(db['links_2'], r)
    logger.info("Indexing links_3")
    populate_sets(db['links_3'], r)
    logger.info("Indexing links")
    populate_sets(db['links'], r)

    #  logger.info("Indexing RecursiveIncomingSet")
    #  populate_recursive_sets(INCOMING_SET, R_INCOMING_SET, r)
    #  logger.info("Indexing RecursiveOutgoingSet")
    #  populate_recursive_sets(OUTGOING_SET, R_OUTGOING_SET, r)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        "Indexes DAS to redis", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--mongo-hostname",
        type=str,
        default=os.environ.get("DAS_MONGO_HOSTNAME", "localhost"),
        metavar="HOSTNAME",
        dest="mongo_host",
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
        "--redis-hostname",
        type=str,
        default=os.environ.get("DAS_REDIS_HOSTNAME", "localhost"),
        metavar="HOSTNAME",
        dest="redis_host",
        help="redis hostname to connect to",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=os.environ.get("DAS_REDIS_PORT", "6379"),
        metavar="PORT",
        dest="redis_port",
        help="redis port to connect to",
    )
    parser.add_argument(
        "--redis-database",
        type=int,
        default=0,
        metavar="NAME",
        dest="redis_db",
        help="redis database name to connect to",
    )

    args = parser.parse_args()
    kwargs = vars(args)
    mongo_args = extract_by_prefix('mongo_', kwargs)
    redis_args = extract_by_prefix('redis_', kwargs)
    main(redis_args, mongo_args)
