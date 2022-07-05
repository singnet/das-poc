from pymongo.database import Database
from couchbase.bucket import Bucket
from typing import List, Any

from .db_interface import WILDCARD, UNORDERED_LINK_TYPES
from .couch_mongo_db import CouchMongoDB
from das.hashing import Hasher


class CouchDB(CouchMongoDB):

    def __init__(self, couch_db: Bucket, mongo_db: Database):
        super().__init__(couch_db, mongo_db)

    def _build_link_handle(self, link_type: str, target_handles: List[str]) -> str:
        link_type_hash = self.atom_type_hash[link_type]
        if link_type_hash is None:
            return False
        if link_type in UNORDERED_LINK_TYPES:
            target_handles = sorted(target_handles)
        handle_list = [link_type_hash, *target_handles]
        return Hasher.apply_alg("".join(handle_list))

    def link_exists(self, link_type: str, target_handles: List[str]) -> bool:
        link_handle = self._build_link_handle(link_type, target_handles)
        return self._retrieve_couchbase_value(self.couch_outgoing_collection, link_handle) is not None

    def get_link_handle(self, link_type: str, target_handles: List[str]) -> str:
        link_handle = self._build_link_handle(link_type, target_handles)
        targets = self._retrieve_couchbase_value(self.couch_outgoing_collection, link_handle)
        if targets is None:
            raise ValueError(f'Invalid link: type={link_type} targets={target_handles}')
        return link_handle

    def is_ordered(self, link_handle: str) -> bool:
        keys = self._retrieve_couchbase_value(self.couch_outgoing_collection, link_handle)
        if not Keys:
            raise ValueError(f'Invalid link handle: {link_handle}')
        link_type = self.atom_type_hash_reverse.get(keys[0], None)
        if not link_type:
            raise ValueError(f'Invalid link type hash: {keys[0]}')
        return link_type not in UNORDERED_LINK_TYPES
