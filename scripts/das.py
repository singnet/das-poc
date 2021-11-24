import argparse
from datetime import datetime
import glob
import logging
import os

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError
from pymongo.operations import DeleteMany
from pymongo.results import InsertOneResult

from atomese2metta.translator import AtomType, Expression, MSet
from metta_lex import MettaParser
from hashing import Hasher, sort_by_key_hash
from helpers import evaluate_hash, get_filesize_mb, human_time


logger = logging.getLogger("das")
logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

formatter = logging.Formatter("[%(asctime)s %(levelname)s]: %(message)s")

stream_handler.setFormatter(formatter)

logger.addHandler(stream_handler)


class DAS:
    NODE_TYPES = "node_types"
    NODES = "nodes"
    LINKS = "links"
    LINKS_1 = "links_1"
    LINKS_2 = "links_2"
    LINKS_3 = "links_3"

    def __init__(self, db: Database, hasher: Hasher):
        self.db = db
        self.hasher = hasher
        self.collections_name = [
            self.NODE_TYPES,
            self.NODES,
            self.LINKS,
            self.LINKS_1,
            self.LINKS_2,
            self.LINKS_3,
        ]

    def links_collection_by_order(self, order: int) -> Collection:
        if hasattr(self, f"LINKS_{order}"):
            return self.db[getattr(self, f"LINKS_{order}")]
        return self.db[self.LINKS]

    @staticmethod
    def insert_many(collection: Collection, data: list[dict], step: int = 1000):
        logger.info(f"Collection: {collection.name}")
        logger.info(f"Data length: {len(data)}")
        i = 0
        data_len = len(data)
        while i < data_len:
            data_ = data[i : min(i + step, data_len)]
            collection.insert_many(data_)
            i += step

    def clean_collections(self):
        for collection_name in self.collections_name:
            collection = self.db[collection_name]
            collection.bulk_write(
                [
                    DeleteMany({}),
                ]
            )

    def insert_node_type(self, node_type: AtomType) -> InsertOneResult:
        collection: Collection = self.db[self.NODE_TYPES]
        return collection.insert_one(self.atom_type_to_dict(node_type))

    def insert_node(self, node: AtomType) -> InsertOneResult:
        collection: Collection = self.db[self.NODES]
        return collection.insert_one(self.atom_type_to_dict(node))

    def insert_link(self, link: Expression) -> InsertOneResult:
        collection = self.links_collection_by_order(len(link))
        return collection.insert_one(self.expression_to_dict(link))

    def atom_type_to_dict(self, atom_type: AtomType) -> dict:
        return {
            "_id": atom_type._id,
            "type": self.retrieve_id(atom_type.type)
            if atom_type.type is not None
            else None,
            "name": atom_type.symbol,
        }

    def expression_to_dict(self, expression: Expression) -> dict:
        keys_hashes = [self.retrieve_id(e) for e in expression]
        type_ = self.retrieve_expression_type(expression)

        if is_set := isinstance(expression, MSet):
            keys_hashes, type_ = sort_by_key_hash(keys_hashes, type_)

        result = {
            "_id": expression._id,
            "type": type_,
            "is_root": expression.is_root,
            "is_ordered": not is_set,
        }

        if len(expression) > 3:
            keys = {
                "keys": keys_hashes
            }
        else:
            keys = {
                f"key{i}": e for i, e in enumerate(keys_hashes, start=1)
            }

        result.update(keys)
        return result

    def retrieve_id(self, value) -> str:
        if isinstance(value, str):
            return self.hasher.search_by_name(value)._id
        elif isinstance(value, Expression):
            return value._id
        else:
            raise TypeError(f"Invalid type {type(value)}")

    def retrieve_expression_type(self, expression: Expression) -> list:
        expression_type = []
        for e in expression:
            if isinstance(e, str):
                expression_type.append(self.hasher.get_type(e)._id)
            elif isinstance(e, Expression):
                expression_type.append(self.retrieve_expression_type(e))
            else:
                raise TypeError(e)

        return expression_type


def main(
  source, mongo_hostname, mongo_port, mongo_username, mongo_password, mongo_database, raise_duplicated):
    metta_files = []
    if source.endswith('.metta'):
        metta_files.append(source)
    else:
        metta_files = glob.glob(f'{source}/*.metta')

    hasher = Hasher()
    client = MongoClient(f"mongodb://{mongo_username}:{mongo_password}@{mongo_hostname}:{mongo_port}")
    das = DAS(client[mongo_database], hasher)

    d1 = datetime.now()
    for idx, file_path in enumerate(metta_files):
        logger.info(
            f"Loading file: {file_path} "
            f"[{get_filesize_mb(file_path)} MB] "
            f"({idx+1}/{len(metta_files)})")
        d2 = datetime.now()
        with open(file_path, "r") as f:
            text = f.read()

        for type_name, expression in MettaParser.parse(text):
            logger.debug(f"{type_name} {expression}")
            if type_name == MettaParser.EXPRESSION:
                hasher.hash_expression(expression)
                try:
                    das.insert_link(expression)
                except DuplicateKeyError as e:
                    if raise_duplicated:
                        raise e
                    logger.debug(f"Duplicated: {expression}")
            else:
                hasher.hash_atom_type(expression)
                try:
                    if type_name == MettaParser.NODE_TYPE:
                        das.insert_node_type(expression)
                    elif type_name == MettaParser.NODE:
                        das.insert_node(expression)
                except DuplicateKeyError as e:
                    if raise_duplicated:
                        raise e
                    logger.debug(f"Duplicated: {expression}")

        logger.info(f"Took {human_time((datetime.now() - d2))} to process {file_path}.")
        logger.info(f"Partial time of processing: {human_time((datetime.now() - d1))}")

    logger.info(f"Took {human_time((datetime.now() - d1))} to process {len(metta_files)} file(s)")
    evaluate_hash(hash_dict=hasher.hash_index, logger=logger)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        "Load MeTTa data into DAS", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument("source", type=str, help="metta file(s) directory or path to load data from")
    parser.add_argument(
        "--hostname",
        "-H",
        type=str,
        default=os.environ.get("DAS_MONGO_HOSTNAME", "localhost"),
        metavar="HOSTNAME",
        dest="mongo_hostname",
        help="mongo hostname to connect to",
    )
    parser.add_argument(
        "--port",
        "-p",
        type=int,
        default=os.environ.get("DAS_MONGO_PORT", "27017"),
        metavar="PORT",
        dest="mongo_port",
        help="mongo port to connect to",
    )
    parser.add_argument(
        "--username",
        "-user",
        type=str,
        default=os.environ.get("DAS_MONGO_USERNAME", "mongoadmin"),
        metavar="USERNAME",
        dest="mongo_username",
        help="mongo username",
    )
    parser.add_argument(
        "--password",
        "-pass",
        type=str,
        default=os.environ.get("DAS_MONGO_PASSWORD", "das#secret"),
        metavar="PASSWORD",
        dest="mongo_password",
        help="mongo password",
    )
    parser.add_argument(
        "--database",
        "-d",
        type=str,
        default="das",
        metavar="NAME",
        dest="mongo_database",
        help="mongo database name to connect to",
    )
    parser.add_argument(
        "--raise-on-duplicated",
        action='store_true',
        default=False,
        dest="raise_duplicated",
        help="raise error when duplicated insert error found",
    )
    args = parser.parse_args()

    main(**vars(args))
