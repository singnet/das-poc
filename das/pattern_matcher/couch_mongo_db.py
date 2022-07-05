import os
from signal import raise_signal
from typing import List, Dict, Optional, Union, Any

from couchbase.auth import PasswordAuthenticator
from couchbase.bucket import Bucket
from couchbase.cluster import Cluster
from couchbase.collection import CBCollection
from couchbase.exceptions import DocumentNotFoundException
from pymongo.collection import Collection
from pymongo.database import Database

from das.hashing import Hasher, flatten_list
from das.couchbase_schema import CollectionNames as CouchbaseCollectionNames
from das.mongo_schema import CollectionNames as MongoCollectionNames, FieldNames as MongoFieldNames

from .db_interface import DBInterface, WILDCARD, UNORDERED_LINK_TYPES


class CouchMongoDB(DBInterface):

    def __init__(self, couch_db: Bucket, mongo_db: Database):
        self.couch_db = couch_db
        self.mongo_db = mongo_db
        if couch_db:
            self.couch_incoming_collection = couch_db.collection(CouchbaseCollectionNames.INCOMING_SET)
            self.couch_outgoing_collection = couch_db.collection(CouchbaseCollectionNames.OUTGOING_SET)
            self.couch_patterns_collection = couch_db.collection(CouchbaseCollectionNames.PATTERNS)
            self.couch_templates_collection = couch_db.collection(CouchbaseCollectionNames.TEMPLATES)
        self.mongo_link_collection = {
            '1': self.mongo_db.get_collection(MongoCollectionNames.LINKS_ARITY_1),
            '2': self.mongo_db.get_collection(MongoCollectionNames.LINKS_ARITY_2),
            'N': self.mongo_db.get_collection(MongoCollectionNames.LINKS_ARITY_N),
        }
        self.node_handles = None
        self.node_documents = None
        self.atom_type_hash = None
        self.type_hash = None
        self.atom_type_hash_reverse = None
        self._prefetch()

    def _build_composite_node_name(self, node_type: str, node_name: str) -> str:
        node_type_hash = self.atom_type_hash.get(node_type, None)
        return f'{node_type_hash}:{node_name}'

    def _prefetch(self) -> None:
        self.node_handles = {}
        self.node_documents = {}
        self.atom_type_hash = {}
        self.type_hash = {}
        self.atom_type_hash_reverse = {}
        collection = self.mongo_db.get_collection(MongoCollectionNames.NODES)
        for document in collection.find():
            self.node_documents[document[MongoFieldNames.ID_HASH]] = document
            self.node_handles[f'{document[MongoFieldNames.TYPE]}:{document[MongoFieldNames.NODE_NAME]}'] = \
                document[MongoFieldNames.ID_HASH]
        collection = self.mongo_db.get_collection(MongoCollectionNames.ATOM_TYPES)
        for document in collection.find():
            self.atom_type_hash[document[MongoFieldNames.TYPE_NAME]] = document[MongoFieldNames.ID_HASH]
            self.type_hash[document[MongoFieldNames.ID_HASH]] = document[MongoFieldNames.TYPE]
            self.atom_type_hash_reverse[document[MongoFieldNames.ID_HASH]] = document[MongoFieldNames.TYPE_NAME]

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

    def _retrieve_couchbase_value(self, collection: CBCollection, key: str) -> List[str]:
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


    def _build_link_handle(self, link_type: str, target_handles: List[str]) -> str:
        hash_input = []
        link_type_hash = self.atom_type_hash[link_type]
        if link_type in UNORDERED_LINK_TYPES:
            hash_input.append("True")
            target_handles = sorted(target_handles)
        hash_input.append(self.type_hash[link_type_hash])
        target_types = []
        for handle in target_handles:
            if handle == WILDCARD:
                target_types.append(handle)
            else:
                document = self._retrieve_mongo_document(handle)
                target_types.append(document[MongoFieldNames.TYPE])
        if link_type in UNORDERED_LINK_TYPES:
            hash_input.extend(sorted(target_types))
        else:
            hash_input.extend(target_types)
        link_composite_type_hash = Hasher.apply_alg("".join(hash_input))
        hash_input = [link_composite_type_hash, link_type_hash, *target_handles]
        return Hasher.apply_alg("".join(hash_input))

    def _build_hash_template(self, template: Union[str, List[Any]]) -> List[Any]:
        if isinstance(template, str):
            return self.atom_type_hash[template]
        else:
            answer = []
            for element in template:
                v = self._build_hash_template(element)
                answer.append(v)
            return answer

    def _get_mongo_document_keys(self, document: Dict) -> List[str]:
        answer = document.get(MongoFieldNames.KEYS, None)
        if answer is not None:
            return answer
        answer = []
        index = 1
        while True:
            key = document.get(f'{MongoFieldNames.KEY_PREFIX}{index}', None)
            if key is None:
                return answer
            else:
                answer.append(key)
            index += 1

    # DB interface methods

    def node_exists(self, node_type: str, node_name: str) -> bool:
        return self._build_composite_node_name(node_type, node_name) in self.node_handles

    def link_exists(self, link_type: str, target_handles: List[str]) -> bool:
        link_handle = self._build_link_handle(link_type, target_handles)
        document = self._retrieve_mongo_document(link_handle, len(target_handles))
        return document is not None

    def get_node_handle(self, node_type: str, node_name: str) -> str:
        try:
            return self.node_handles[self._build_composite_node_name(node_type, node_name)]
        except KeyError:
            raise ValueError(f'Invalid node: type={node_type} name={node_name}')

    def get_link_handle(self, link_type: str, target_handles: List[str]) -> str:
        link_handle = self._build_link_handle(link_type, target_handles)
        document = self._retrieve_mongo_document(link_handle, len(target_handles))
        if document is None:
            raise ValueError(f'Invalid link: type={link_type} targets={target_handles}')
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
        return document['set_from'] is None

    def get_matched_links(self, link_type: str, target_handles: List[str]):
        if WILDCARD not in target_handles:
            try:
                answer = self.get_link_handle(link_type, target_handles)
                return [answer]
            except ValueError:
                return []
        link_type_hash = self.atom_type_hash.get(link_type, None)
        if not link_type_hash:
            return []
        if link_type in UNORDERED_LINK_TYPES:
            target_handles = sorted(target_handles)
        pattern_hash = Hasher.apply_alg("".join([link_type_hash, *target_handles]))
        return self._retrieve_couchbase_value(self.couch_patterns_collection, pattern_hash)

    def get_all_nodes(self, node_type: str, names: bool = False) -> List[str]:
        node_type_hash = self.atom_type_hash.get(node_type, None)
        if not node_type_hash:
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
            template = self._build_hash_template(template)
            template_hash = Hasher.apply_alg(flatten_list(template))
        except KeyError as exception:
            raise ValueError(f'{exception}\nInvalid type')
        return self._retrieve_couchbase_value(self.couch_templates_collection, template_hash)

    def get_node_name(self, node_handle: str) -> str:
        document = self.node_documents.get(node_handle, None)
        if not document:
            raise ValueError(f'Invalid node handle: {node_handle}')
        return document[MongoFieldNames.NODE_NAME]
