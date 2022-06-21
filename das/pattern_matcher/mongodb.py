from typing import List

from pymongo.collection import Collection
from pymongo.database import Database

from das.hashing import Hasher
from das.pattern_matcher.couch_mongo_db import CouchMongoDB


class DASMongoDB(CouchMongoDB):
    COLL_NODE_TYPES = "node_types"  # atom_type
    COLL_NODES = "nodes"

    COLL_LINKS_1 = "links_1"
    COLL_LINKS_2 = "links_2"
    COLL_LINKS_3 = "links_3"
    COLL_LINKS = "links"
    EXPRESSION_COLLS = [
        COLL_LINKS_1,
        COLL_LINKS_2,
        COLL_LINKS_3,
        COLL_LINKS,
    ]

    C_COLL_INCOMING_NAME = "IncomingSet"
    C_COLL_OUTGOING_NAME = "OutgoingSet"

    def __init__(self, mongo_db: Database):
        self.mongo_db = mongo_db
        if hasattr(self, 'couch_db'):
            del self.couch_db

    def get_link_targets(self, handle: str) -> List[str]:
        for col in self.ALL_COLLS:
            res = self.mongo_db[col].find_one({"_id": handle})
            if res is not None:
                return res
        raise ValueError(f"invalid handle: {handle}") from e

    def get_matched_links(self, link_type: str, target_handles: List[str]) -> List[str]:
        atom_type_handle = self._get_type_handle(link_type)
        link_handle = self._get_matched_handle(atom_type_handle, target_handles)
        collection = self.couch_db.collection(self.C_COLL_INCOMING_NAME)
        try:
            result = collection.get(link_handle)
        except DocumentNotFoundException as e:
            return []
        return result.content
