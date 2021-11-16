import argparse
import logging

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError
from pymongo.operations import DeleteMany
from pymongo.results import InsertOneResult

from atomese2metta.parser import LexParser
from atomese2metta.translator import Translator, MettaDocument, AtomType, Expression
from atomese2metta.collections import OrderedSet
from metta_lex import MettaParser
from hashing import Hasher


logger = logging.getLogger('das')
logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

formatter = logging.Formatter("[%(asctime)s %(levelname)s]: %(message)s")

stream_handler.setFormatter(formatter)

logger.addHandler(stream_handler)


class DAS:
    NODE_TYPES = 'node_types'
    NODES = 'nodes'
    LINKS = 'links'
    LINKS_1 = 'links_1'
    LINKS_2 = 'links_2'
    LINKS_3 = 'links_3'

    def __init__(self, db: Database, hasher: Hasher):
        self.db = db
        self.hasher = hasher
        self.collections_name = [
            self.NODE_TYPES,
            self.NODES,
            self.LINKS,
        ]

    def links_collection(self, expression: Expression) -> Collection:
        order = len(expression)
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
            data_ = data[i:min(i + step, data_len)]
            collection.insert_many(data_)
            i += step

    def clean_collections(self):
        for collection_name in self.collections_name:
            collection = self.db[collection_name]
            collection.bulk_write([ DeleteMany({}), ])

    def insert_node_type(self, node_type: AtomType) -> InsertOneResult:
        collection: Collection = self.db[self.NODE_TYPES]
        return collection.insert_one(self.atom_type_to_dict(node_type))

    def insert_node(self, node: AtomType) -> InsertOneResult:
        collection: Collection = self.db[self.NODES]
        return collection.insert_one(self.atom_type_to_dict(node))

    def insert_link(self, link: Expression) -> InsertOneResult:
        collection = self.links_collection(link)
        return collection.insert_one(self.expression_to_dict(link))

    def atom_type_to_dict(self, atom_type: AtomType) -> dict:
        return {
            '_id': atom_type._id,
            'type': self.retrieve_id(atom_type.type) if atom_type.type is not None else None,
            'name': atom_type.symbol,
        }

    def expression_to_dict(self, expression: Expression) -> dict:
        result = {
            '_id': expression._id,
            'type': self.retrieve_expression_type(expression),
            'is_root': expression.is_root,
        }
        keys = { f'key{i}': self.retrieve_id(e) for i, e in enumerate(expression, start=1) }
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

def main(filename, database_name='das'):
    logger.info(f"Loading file: {filename}")
    client = MongoClient()

    with open(filename, 'r') as f:
        text = f.read()

    hasher = Hasher()
    das = DAS(client[database_name], hasher)

    for type_name, expression in MettaParser.parse(text):
        logger.debug(f"{type_name} {expression}")
        if type_name == MettaParser.EXPRESSION:
            hasher.hash_expression(expression)
            try:
                das.insert_link(expression)
            except DuplicateKeyError:
                logger.error(f"Duplicated: {expression}")
        else:
            hasher.hash_atom_type(expression)
            try:
                if type_name == MettaParser.NODE_TYPE:
                    das.insert_node_type(expression)
                elif type_name == MettaParser.NODE:
                    das.insert_node(expression)
            except DuplicateKeyError:
                logger.error(f"Duplicated: {expression}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Insert data into DAS")

    parser.add_argument('filename', type=str)
    parser.add_argument('--database', '-d', type=str, default='das', metavar='NAME', dest='database_name')
    args = parser.parse_args()

    main(**vars(args))
