import re
from pymongo.database import Database
from couchbase.bucket import Bucket
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from typing import List, Any

from .db_interface import WILDCARD, UNORDERED_LINK_TYPES
from das.couchbase_schema import CollectionNames as CouchbaseCollectionNames
from .couch_mongo_db import CouchMongoDB
from das.hashing import Hasher


class CouchDB(CouchMongoDB):

    def __init__(self, couch_db: Bucket, mongo_db: Database):
        super().__init__(couch_db, mongo_db)
        self.couch_named_entities_collection = self.couch_db.collection(CouchbaseCollectionNames.NAMED_ENTITIES)

    def link_exists(self, link_type: str, target_handles: List[str]) -> bool:
        link_handle = self._build_link_handle(link_type, target_handles)
        return bool(self._retrieve_couchbase_value(self.couch_outgoing_collection, link_handle))

    def get_link_handle(self, link_type: str, target_handles: List[str]) -> str:
        link_handle = self._build_link_handle(link_type, target_handles)
        targets = self._retrieve_couchbase_value(self.couch_outgoing_collection, link_handle)
        if targets is None:
            raise ValueError(f'Invalid link: type={link_type} targets={target_handles}')
        return link_handle

    def is_ordered(self, link_handle: str) -> bool:
        keys = self._retrieve_couchbase_value(self.couch_outgoing_collection, link_handle)
        if not keys:
            raise ValueError(f'Invalid link handle: {link_handle}')
        link_type = self.atom_type_hash_reverse.get(keys[0], None)
        if not link_type:
            raise ValueError(f'Invalid link type hash: {keys[0]}')
        return link_type not in UNORDERED_LINK_TYPES

    def get_matched_node_name(self, substring: str) -> str:
        ###### TODO Fix this
        couchbase_specs = {
            "hostname": "couchbase",
            "username": "dbadmin",
            "password": "dassecret",
        }
        cluster = Cluster(
            f'couchbase://{couchbase_specs["hostname"]}',
            authenticator=PasswordAuthenticator(
                couchbase_specs["username"], couchbase_specs["password"]
            ),
        )
        ######
        query_result = cluster.query("SELECT META().id,Names FROM `das`._default.Names")
        answer = []
        for row in query_result.rows():
            name = row['Names'][0]
            handle = row['id']
            if substring in name:
                answer.append(handle)
        return answer
