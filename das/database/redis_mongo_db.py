import os
from signal import raise_signal
from typing import List, Dict, Optional, Union, Any, Tuple
from redis import Redis
import pickle

from pymongo.database import Database

from das.expression_hasher import ExpressionHasher
from das.database.key_value_schema import CollectionNames as KeyPrefix, build_redis_key
from das.database.mongo_schema import CollectionNames as MongoCollectionNames, FieldNames as MongoFieldNames

from .db_interface import DBInterface, WILDCARD, UNORDERED_LINK_TYPES

USE_CACHED_NODES = False

class NodeDocuments():

    def __init__(self, collection):
        self.mongo_collection = collection
        self.cached_nodes = {}
        self.count = 0

    def add(self, node_id, document):
        if USE_CACHED_NODES:
            self.cached_nodes[node_id] = document
        self.count += 1

    def get(self, handle, default_value):
        if USE_CACHED_NODES:
            return self.cached_nodes.get(handle, default_value)
        else:
            mongo_filter = {MongoFieldNames.ID_HASH: handle}
            node = self.mongo_collection.find_one(mongo_filter)
            return node if node else default_value

    def size(self):
        if USE_CACHED_NODES:
            return len(self.cached_nodes)
        else:
            return self.count

    def values(self):
        for document in self.cached_nodes.values() if USE_CACHED_NODES else self.mongo_collection.find():
            yield document

class RedisMongoDB(DBInterface):

    def __init__(self, redis: Redis, mongo_db: Database):
        self.redis = redis
        self.mongo_db = mongo_db
        self.mongo_link_collection = {
            '1': self.mongo_db.get_collection(MongoCollectionNames.LINKS_ARITY_1),
            '2': self.mongo_db.get_collection(MongoCollectionNames.LINKS_ARITY_2),
            'N': self.mongo_db.get_collection(MongoCollectionNames.LINKS_ARITY_N),
        }
        self.mongo_nodes_collection = self.mongo_db.get_collection(MongoCollectionNames.NODES)
        self.mongo_types_collection = self.mongo_db.get_collection(MongoCollectionNames.ATOM_TYPES)
        self.wildcard_hash = ExpressionHasher._compute_hash(WILDCARD)
        self.named_type_hash = None
        self.named_type_hash_reverse = None
        self.named_types = None
        self.symbol_hash = None
        self.parent_type = None
        self.node_documents = None
        self.terminal_hash = None
        self.typedef_mark_hash = ExpressionHasher._compute_hash(":")
        self.typedef_base_type_hash = ExpressionHasher._compute_hash("Type")
        self.typedef_composite_type_hash = ExpressionHasher.composite_hash([
            self.typedef_mark_hash,
            self.typedef_base_type_hash,
            self.typedef_base_type_hash])
        self.use_targets = [KeyPrefix.PATTERNS, KeyPrefix.TEMPLATES]

    def _get_atom_type_hash(self, atom_type):
        #TODO: implement a proper mongo collection to atom types so instead
        #      of this lazy hashmap, we should load the hashmap during prefetch
        named_type_hash = self.named_type_hash.get(atom_type, None)
        if named_type_hash is None:
            named_type_hash = ExpressionHasher.named_type_hash(atom_type)
            self.named_type_hash[atom_type] = named_type_hash
            self.named_type_hash_reverse[named_type_hash] = atom_type
        return named_type_hash

    def prefetch(self) -> None:
        self.named_type_hash = {}
        self.named_type_hash_reverse = {}
        self.named_types = {}
        self.symbol_hash = {}
        self.parent_type = {}
        self.terminal_hash = {}
        self.node_documents = NodeDocuments(self.mongo_nodes_collection)
        if USE_CACHED_NODES:
            for document in self.mongo_nodes_collection.find():
                node_id = document[MongoFieldNames.ID_HASH]
                node_type = document[MongoFieldNames.TYPE_NAME]
                node_name = document[MongoFieldNames.NODE_NAME]
                self.node_documents.add(node_id, document)
        else:
            self.node_documents.count = self.mongo_nodes_collection.count_documents({})
        for document in self.mongo_types_collection.find():
            hash_id = document[MongoFieldNames.ID_HASH]
            named_type = document[MongoFieldNames.TYPE_NAME]
            named_type_hash = document[MongoFieldNames.TYPE_NAME_HASH]
            composite_type_hash = document[MongoFieldNames.TYPE]
            type_document = self.mongo_types_collection.find_one({
                MongoFieldNames.ID_HASH: composite_type_hash
            })
            self.named_type_hash[named_type] = named_type_hash
            self.named_type_hash_reverse[named_type_hash] = named_type
            if type_document is not None:
                self.named_types[named_type] = type_document[MongoFieldNames.TYPE_NAME]
                self.parent_type[named_type_hash] = type_document[MongoFieldNames.TYPE_NAME_HASH]
            self.symbol_hash[named_type] = hash_id

    def _retrieve_mongo_document(self, handle: str, arity=-1) -> dict:
        mongo_filter = {MongoFieldNames.ID_HASH: handle}
        if arity >= 0:
            if arity == 0:
                collection = self.mongo_nodes_collection
            if arity == 2:
                collection = self.mongo_link_collection['2']
            elif arity == 1:
                collection = self.mongo_link_collection['1']
            else:
                collection = self.mongo_link_collection['N']
            return collection.find_one(mongo_filter)
        # The order of keys in search is important. Greater to smallest probability of proper arity
        for collection in [self.mongo_link_collection[key] for key in ['2', '1', 'N']]:
            document = collection.find_one(mongo_filter)
            if document:
                return document
        return None

    def _retrieve_key_value(self, prefix: str, key: str) -> List[str]:
        if prefix in self.use_targets:
            return [pickle.loads(t) for t in self.redis.smembers(build_redis_key(prefix, key))]
        else:
            return [* self.redis.smembers(build_redis_key(prefix, key))]

    def _build_named_type_hash_template(self, template: Union[str, List[Any]]) -> List[Any]:
        if isinstance(template, str):
            return self._get_atom_type_hash(template)
        else:
            answer = []
            for element in template:
                v = self._build_named_type_hash_template(element)
                answer.append(v)
            return answer

    def _build_named_type_template(self, template: Union[str, List[Any]]) -> List[Any]:
        if isinstance(template, str):
            return self.named_type_hash_reverse.get(template, None)
        else:
            answer = []
            for element in template:
                v = self._build_named_type_template(element)
                answer.append(v)
            return answer

    def _get_mongo_document_keys(self, document: Dict) -> List[str]:
        answer = document.get(MongoFieldNames.KEYS, None)
        if answer is not None:
            return answer
        answer = []
        index = 0
        while True:
            key = document.get(f'{MongoFieldNames.KEY_PREFIX}_{index}', None)
            if key is None:
                return answer
            else:
                answer.append(key)
            index += 1

    def _build_deep_representation(self, handle, arity=-1):
        answer = {}
        document = self.node_documents.get(handle, None)
        if document is None:
            document = self._retrieve_mongo_document(handle, arity)
            answer["type"] = document[MongoFieldNames.TYPE_NAME]
            answer["targets"] = []
            for target_handle in self._get_mongo_document_keys(document):
                answer["targets"].append(self._build_deep_representation(target_handle))
        else:
            answer["type"] = document[MongoFieldNames.TYPE_NAME]
            answer["name"] = document[MongoFieldNames.NODE_NAME]
        return answer


    # DB interface methods

    def node_exists(self, node_type: str, node_name: str) -> bool:
        node_handle = ExpressionHasher.terminal_hash(node_type, node_name)
        # TODO: use a specific query to nodes table
        document = self._retrieve_mongo_document(node_handle)
        return document is not None

    def link_exists(self, link_type: str, target_handles: List[str]) -> bool:
        link_handle = ExpressionHasher.expression_hash(self._get_atom_type_hash(link_type), target_handles)
        document = self._retrieve_mongo_document(link_handle, len(target_handles))
        return document is not None

    def get_node_handle(self, node_type: str, node_name: str) -> str:
        return ExpressionHasher.terminal_hash(node_type, node_name)

    def get_link_handle(self, link_type: str, target_handles: List[str]) -> str:
        link_handle = ExpressionHasher.expression_hash(self._get_atom_type_hash(link_type), target_handles)
        return link_handle

    def get_link_targets(self, link_handle: str) -> List[str]:
        answer = self._retrieve_key_value(KeyPrefix.OUTGOING_SET, link_handle)
        if not answer:
            raise ValueError(f"Invalid handle: {link_handle}")
        return answer[1:]

    def is_ordered(self, link_handle: str) -> bool:
        document = self._retrieve_mongo_document(link_handle)
        if document is None:
            raise ValueError(f'Invalid handle: {link_handle}')
        return True

    def get_matched_links(self, link_type: str, target_handles: List[str]):
        if link_type != WILDCARD and WILDCARD not in target_handles:
            try:
                link_handle = self.get_link_handle(link_type, target_handles)
                document = self._retrieve_mongo_document(link_handle, len(target_handles))
                return [link_handle] if document else []
            except ValueError:
                return []
        if link_type == WILDCARD:
            link_type_hash = WILDCARD
        else:
            link_type_hash = self._get_atom_type_hash(link_type)
        if link_type_hash is None:
            return []
        if link_type in UNORDERED_LINK_TYPES:
            target_handles = sorted(target_handles)
        pattern_hash = ExpressionHasher.composite_hash([link_type_hash, *target_handles])
        return self._retrieve_key_value(KeyPrefix.PATTERNS, pattern_hash)

    def get_all_nodes(self, node_type: str, names: bool = False) -> List[str]:
        node_type_hash = self._get_atom_type_hash(node_type)
        if node_type_hash is None:
            raise ValueError(f'Invalid node type: {node_type}')
        if names:
            return [\
                document[MongoFieldNames.NODE_NAME] \
                for document in self.node_documents.values() \
                if document[MongoFieldNames.TYPE] == node_type_hash]
        else:
            return [\
                document[MongoFieldNames.ID_HASH] \
                for document in self.node_documents.values() \
                if document[MongoFieldNames.TYPE] == node_type_hash]

    def get_matched_type_template(self, template: List[Any]) -> List[str]:
        try:
            template = self._build_named_type_hash_template(template)
            template_hash = ExpressionHasher.composite_hash(template)
        except KeyError as exception:
            raise ValueError(f'{exception}\nInvalid type')
        return self._retrieve_key_value(KeyPrefix.TEMPLATES, template_hash)

    def get_matched_type(self, link_type: str) -> List[str]:
        named_type_hash = self._get_atom_type_hash(link_type)
        return self._retrieve_key_value(KeyPrefix.TEMPLATES, named_type_hash)

    def get_node_name(self, node_handle: str) -> str:
        document = self.node_documents.get(node_handle, None)
        if not document:
            raise ValueError(f'Invalid node handle: {node_handle}')
        return document[MongoFieldNames.NODE_NAME]

    def get_matched_node_name(self, node_type: str, substring: str) -> str: 
        node_type_hash = self._get_atom_type_hash(node_type)
        mongo_filter = {
            MongoFieldNames.TYPE: node_type_hash,
            MongoFieldNames.NODE_NAME: {'$regex': substring}
        }
        return [document[MongoFieldNames.ID_HASH] for document in self.mongo_nodes_collection.find(mongo_filter)]

    #################################

    def get_atom_as_dict(self, handle, arity=-1) -> dict:
        answer = {}
        document = self.node_documents.get(handle, None) if arity <= 0 else None
        if document is None:
            document = self._retrieve_mongo_document(handle, arity)
            if document:
                answer["handle"] = document[MongoFieldNames.ID_HASH]
                answer["type"] = document[MongoFieldNames.TYPE_NAME]
                answer["template"] = self._build_named_type_template(document[MongoFieldNames.COMPOSITE_TYPE])
                answer["targets"] = self._get_mongo_document_keys(document)
        else:
            answer["handle"] = document[MongoFieldNames.ID_HASH]
            answer["type"] = document[MongoFieldNames.TYPE_NAME]
            answer["name"] = document[MongoFieldNames.NODE_NAME]
        return answer

    def get_atom_as_deep_representation(self, handle: str, arity=-1) -> str:
        return self._build_deep_representation(handle, arity)

    def count_atoms(self) -> Tuple[int, int]:
        node_count = self.mongo_nodes_collection.estimated_document_count()
        link_count = 0
        for collection in self.mongo_link_collection.values():
            link_count += collection.estimated_document_count()
        return (node_count, link_count)
