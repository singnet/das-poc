import os
from signal import raise_signal
from typing import List, Dict, Optional, Union, Any

from couchbase.bucket import Bucket
from couchbase.collection import CBCollection as CouchbaseCollection
from couchbase.exceptions import DocumentNotFoundException
from pymongo.database import Database

from das.expression_hasher import ExpressionHasher
from das.database.couchbase_schema import CollectionNames as CouchbaseCollectionNames
from das.database.mongo_schema import CollectionNames as MongoCollectionNames, FieldNames as MongoFieldNames

from .db_interface import DBInterface, WILDCARD, UNORDERED_LINK_TYPES

class CouchMongoDB(DBInterface):

    def __init__(self, couch_db: Bucket, mongo_db: Database):
        self.couch_db = couch_db
        self.mongo_db = mongo_db
        self.couch_incoming_collection = couch_db.collection(CouchbaseCollectionNames.INCOMING_SET)
        self.couch_outgoing_collection = couch_db.collection(CouchbaseCollectionNames.OUTGOING_SET)
        self.couch_patterns_collection = couch_db.collection(CouchbaseCollectionNames.PATTERNS)
        self.couch_templates_collection = couch_db.collection(CouchbaseCollectionNames.TEMPLATES)
        self.mongo_link_collection = {
            '1': self.mongo_db.get_collection(MongoCollectionNames.LINKS_ARITY_1),
            '2': self.mongo_db.get_collection(MongoCollectionNames.LINKS_ARITY_2),
            'N': self.mongo_db.get_collection(MongoCollectionNames.LINKS_ARITY_N),
        }
        self.mongo_nodes_collection = self.mongo_db.get_collection(MongoCollectionNames.NODES)
        self.mongo_types_collection = self.mongo_db.get_collection(MongoCollectionNames.ATOM_TYPES)
        self.wildcard_hash = ExpressionHasher._compute_hash(WILDCARD)
        self.node_handles = None
        self.node_documents = None
        self.atom_type_hash = None
        self.atom_type_hash_reverse = None
        self.typedef_mark_hash = ExpressionHasher._compute_hash(":")
        self.typedef_base_type_hash = ExpressionHasher._compute_hash("Type")
        self.typedef_composite_type_hash = ExpressionHasher.composite_hash([
            self.typedef_mark_hash,
            self.typedef_base_type_hash,
            self.typedef_base_type_hash])

    def _build_composite_node_name(self, node_type: str, node_name: str) -> str:
        return f'{node_type}:{node_name}'

    def _get_atom_type_hash(self, atom_type):
        #TODO: implement a proper mongo collection to atom types so instead
        #      of this lazy hashmap, we should load the hashmap during prefetch
        atom_type_hash = self.atom_type_hash.get(atom_type, None)
        if atom_type_hash is None:
            atom_type_hash = ExpressionHasher.named_type_hash(atom_type)
            self.atom_type_hash[atom_type] = atom_type_hash
            self.atom_type_hash_reverse[atom_type_hash] = atom_type
        return atom_type_hash

    def _get_node_handle(self, node_type, node_name):
        composite_name = self._build_composite_node_name(node_type, node_name)
        node_handle = self.node_handles.get(composite_name, None)
        if node_handle is None:
            node_handle = ExpressionHasher.terminal_hash(node_type, node_name)
            self.node_handles[composite_name] = node_handle
        return node_handle

    def prefetch(self) -> None:
        self.node_handles = {}
        self.node_documents = {}
        self.atom_type_hash = {}
        self.atom_type_hash_reverse = {}
        collection = self.mongo_nodes_collection
        for document in collection.find():
            node_id = document[MongoFieldNames.ID_HASH]
            node_type = document[MongoFieldNames.TYPE_NAME]
            node_name = document[MongoFieldNames.NODE_NAME]
            self.node_documents[node_id] = document
            self.node_handles[self._build_composite_node_name(node_type, node_name)] = node_id
        for document in self.mongo_types_collection.find():
            named_type = document[MongoFieldNames.TYPE_NAME]
            named_type_hash = document["named_type_hash"]
            self.atom_type_hash[named_type] = named_type_hash
            self.atom_type_hash_reverse[named_type_hash] = named_type

    def _retrieve_mongo_document(self, handle: str, arity=-1) -> dict:
        mongo_filter = {MongoFieldNames.ID_HASH: handle}
        if arity > 0:
            if arity == 2:
                collection = self.mongo_link_collection['2']
            elif arity == 1:
                collection = self.mongo_link_collection['1']
            else:
                collection = self.mongo_link_collection['N']
            return collection.find_one(mongo_filter)
        document = self.node_documents.get(handle, None)
        if document:
            return document
        # The order of keys in search is important. Greater to smallest probability of proper arity
        for collection in [self.mongo_link_collection[key] for key in ['2', '1', 'N']]:
            document = collection.find_one(mongo_filter)
            if document:
                return document
        return None

    def _retrieve_couchbase_value(self, collection: CouchbaseCollection, key: str) -> List[str]:
        try:
            value = collection.get(key)
        except DocumentNotFoundException as e:
            return []
        if isinstance(value.content, list):
            return value.content
        answer = []
        for i in range(value.content):
            answer.extend(collection.get(key + f'_{i}').content)
        return answer

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
            return self.atom_type_hash_reverse.get(template, None)
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


    # DB interface methods

    def node_exists(self, node_type: str, node_name: str) -> bool:
        node_handle = self._get_node_handle(node_type, node_name)
        # TODO: use a specific query to nodes table
        document = self._retrieve_mongo_document(node_handle)
        return document is not None

    def link_exists(self, link_type: str, target_handles: List[str]) -> bool:
        link_handle = ExpressionHasher.expression_hash(self._get_atom_type_hash(link_type), target_handles)
        document = self._retrieve_mongo_document(link_handle, len(target_handles))
        return document is not None

    def get_node_handle(self, node_type: str, node_name: str) -> str:
        return self._get_node_handle(node_type, node_name)

    def get_link_handle(self, link_type: str, target_handles: List[str]) -> str:
        link_handle = ExpressionHasher.expression_hash(self._get_atom_type_hash(link_type), target_handles)
        return link_handle

    def get_link_targets(self, link_handle: str) -> List[str]:
        answer = self._retrieve_couchbase_value(self.couch_outgoing_collection, link_handle)
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
        return self._retrieve_couchbase_value(self.couch_patterns_collection, pattern_hash)

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
        return self._retrieve_couchbase_value(self.couch_templates_collection, template_hash)

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

    def get_link_as_dict(self, handle: str, arity=-1) -> dict:
        document = self._retrieve_mongo_document(handle, arity)
        answer = {}
        answer["handle"] = document[MongoFieldNames.ID_HASH]
        answer["type"] = document[MongoFieldNames.TYPE_NAME]
        answer["template"] = self._build_named_type_template(document[MongoFieldNames.COMPOSITE_TYPE])
        answer["targets"] = self._get_mongo_document_keys(document)
        return answer

    def get_node_as_dict(self, handle) -> dict:
        document = self._retrieve_mongo_document(handle)
        answer = {}
        answer["handle"] = document[MongoFieldNames.ID_HASH]
        answer["type"] = document[MongoFieldNames.TYPE_NAME]
        answer["name"] = document[MongoFieldNames.NODE_NAME]
        return answer
