import argparse
import os
from typing import Any

import redis
import pymongo


def extract_by_prefix(key):
    return {k.removeprefix(key): v for k, v in kwargs.items() if k.startswith(key)}

def main(redis_args: dict[str, Any], mongo_args: dict[str, Any]):
    r = redis.Redis(**redis_args)

    mongo_database = mongo_args.pop("database")
    db = pymongo.MongoClient(**mongo_args)[mongo_database]

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
    mongo_args = extract_by_prefix('mongo_')
    redis_args = extract_by_prefix('redis_')
    main(redis_args, mongo_args)
