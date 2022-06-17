import os
from typing import List

from couchbase.auth import PasswordAuthenticator
from couchbase.bucket import Bucket
from couchbase.cluster import Cluster
from couchbase.exceptions import DocumentNotFoundException
from pymongo.collection import Collection
from pymongo.database import Database

from scripts.hashing import Hasher
from scripts.helpers import get_mongodb

from .db_interface import DBInterface


class CouchMongoDB(DBInterface):
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

    def __init__(self, couch_db: Bucket, mongo_db: Database):
        self.couch_db = couch_db
        self.mongo_db = mongo_db

    @property
    def _coll_node_types(self) -> Collection:
        return self.mongo_db.get_collection(self.COLL_NODE_TYPES)

    @property
    def _coll_nodes(self) -> Collection:
        return self.mongo_db.get_collection(self.COLL_NODES)

    @property
    def NODE_COLLS(self) -> List[str]:
        return [self._coll_node_types, self._coll_nodes]

    def node_exists(self, atom_type: str, node_name: str) -> bool:
        name = f'"{atom_type}:{node_name}"'
        len_results = self._coll_nodes.count_documents({"name": name})
        return len_results > 0

    def link_exists(self, atom_type: str, targets: List[str]) -> bool:
        return self._get_link_handle(atom_type, targets) is not None

    def _get_type_handle(self, type_: str) -> str:
        res = self._coll_node_types.find_one({"name": type_})
        if res is None:
            raise ValueError(f"invalid type: {type_}")
        return res["_id"]

    def get_node_handle(self, node_type: str, node_name: str) -> str:
        name = f'"{node_type}:{node_name}"'
        node = self._coll_nodes.find_one({"name": name})
        if node is None:
            raise ValueError(f"invalid node: type: {node_type}, name: {node_name}")
        return node["_id"]

    def get_link_handle(self, link_type: str, target_handles: List[str]) -> str:
        link_handle = self._get_link_handle(link_type, target_handles)
        if link_handle is None:
            raise ValueError(
                f"invalid link type/targets: type={link_type}, target={target_handles}"
            )
        return link_handle

    def _get_link_handle(self, link_type: str, target_handles: List[str]) -> str:
        atom_type_handle = self._get_type_handle(link_type)
        handles = [atom_type_handle, *target_handles]
        link_handle = Hasher().apply_alg("".join(handles))
        if len(handles) == 1:
            collection = self.mongo_db[self.COLL_LINKS_1]
        elif len(handles) == 2:
            collection = self.mongo_db[self.COLL_LINKS_2]
        elif len(handles) == 3:
            collection = self.mongo_db[self.COLL_LINKS_3]
        else:
            collection = self.mongo_db[self.COLL_LINKS]

        link = collection.find_one({"_id": link_handle})
        if link is None:
            return None
        return link["_id"]

    def get_link_targets(self, handle: str) -> List[str]:
        collection = self.couch_db.collection(self.C_COLL_OUTGOING_NAME)
        try:
            result = collection.get(handle)
        except DocumentNotFoundException as e:
            raise ValueError(f"invalid handle: {handle}") from e
        return result.content

    def is_ordered(self, handle: str) -> bool:
        for collection in self.EXPRESSION_COLLS:
            expr = self.mongo_db[collection].find_one({"_id": handle})
            if expr is not None:
                return expr["set_from"] is None
        raise ValueError(f"invalid handle: {handle}")

    def _generate_link_handle(
        self, atom_type_handle: str, target_handles: List[str]
    ) -> str:
        target_handles = self._sort_link(atom_type_handle, target_handles)
        handles = [atom_type_handle, *target_handles]
        print(handles)
        return Hasher().apply_alg("".join(handles))

    def _sort_link(self, link_type: str, target_handles: List[str]) -> str:
        if link_type in ["Set", "Similarity"]:
            return sorted(target_handles)
        return target_handles[:]

    def _should_order(self, link_type: str) -> bool:
        return link_type in ["Set", ""]

    def get_matched_links(self, link_type: str, target_handles: List[str]) -> str:
        atom_type_handle = self._get_type_handle(link_type)
        link_handle = self._generate_link_handle(atom_type_handle, target_handles)
        collection = self.couch_db.collection(self.C_COLL_INCOMING_NAME)
        try:
            result = collection.get(link_handle)
        except DocumentNotFoundException as e:
            raise ValueError(
                f"invalid params: link_type: {link_type}, target_handles: {target_handles}"
            ) from e
        return result.content