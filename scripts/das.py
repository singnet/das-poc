import argparse
import logging

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from bson import ObjectId
from pymongo.operations import DeleteMany

from atomese2metta.parser import LexParser
from atomese2metta.translator import Translator, MettaDocument, AtomType, Expression
from atomese2metta.collections import OrderedSet
from hashing import Hasher


logger = logging.getLogger('das')
logger.setLevel(logging.DEBUG)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)

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

    @staticmethod
    def insert_many(collection: Collection, data: list[dict], step: int = 1000):
        logger.info(f"Collection: {collection.name}")
        logger.info(f"Data length: {len(data)}")
        i = 0
        data_len = len(data)
        while i < data_len:
            data_ = data[i:min(i + step, data_len)]
            from pprint import pformat
            logger.debug(pformat(data_))
            collection.insert_many(data_)
            i += step

    def clean_collections(self):
        for collection_name in self.collections_name:
            collection = self.db[collection_name]
            collection.bulk_write([ DeleteMany({}), ])

    def insert_node_types(self, db: Database):
        collection: Collection = db.node_types
        data: list[dict] = [self.atom_type_to_dict(node_type) for node_type in self.metta_doc.node_types]
        self.insert_many(collection, data)

    def insert_nodes(self, db: Database):
        collection: Collection = db.nodes
        data: list[dict] = [self.atom_type_to_dict(node) for node in self.metta_doc.nodes]
        self.insert_many(collection, data)

    def insert_links(self, db: Database):
        collection: Collection = db.links
        data: list[dict] = [self.expression_to_dict(expr) for expr in OrderedSet(self.metta_doc.body)]
        self.insert_many(collection, data)

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
                expression_type.append(self.hasher.search_by_name(e).type)
            elif isinstance(e, Expression):
                expression_type.append(self.retrieve_expression_type(e))
            else:
                raise TypeError(e)

        return expression_type

def main(filenames):
    mettas = []

    for file_path in filenames:
        logger.info(f"Processing: {file_path}")
        parser = LexParser()

        with open(file_path, "r") as f:
            parsed_expressions = parser.parse(f.read())

        mettas.append(Translator.build(parsed_expressions))

    metta = mettas[0] 
    if len(mettas) > 1:
        metta = sum(mettas[1:], start=metta)

    logger.info("Hashing data")
    hasher = Hasher(metta)
    hasher.hash_atom_types()
    hasher.hash_expressions()

    from parser import evaluate_hash

    evaluate_hash(hasher.hash_index, '/tmp/hash.txt')

    das = DAS(metta, hasher)

    logger.info("Inserting data")
    client = MongoClient()
    db = client.das

    das.clean_collections(db)

    das.insert_node_types(db)
    das.insert_nodes(db)
    das.insert_links(db)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Insert data into DAS")

    parser.add_argument('filenames', type=str, nargs='+')
    args = parser.parse_args()

    main(args.filenames)
