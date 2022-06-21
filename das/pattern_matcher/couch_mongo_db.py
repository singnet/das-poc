import os
from signal import raise_signal
from typing import List

from couchbase.auth import PasswordAuthenticator
from couchbase.bucket import Bucket
from couchbase.cluster import Cluster
from couchbase.exceptions import DocumentNotFoundException
from pymongo.collection import Collection
from pymongo.database import Database

from das.hashing import Hasher

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
    def ALL_COLLS(self) -> List[str]:
        return [
            self.COLL_NODE_TYPES,
            self.COLL_NODES,
            *self.EXPRESSION_COLLS,
        ]

    def node_exists(self, atom_type: str, node_name: str) -> bool:
        name = f'"{atom_type}:{node_name}"'
        len_results = self._coll_nodes.count_documents({"name": name})
        return len_results > 0

    def link_exists(self, atom_type: str, targets: List[str]) -> bool:
        try:
            return self._get_link_handle(atom_type, targets) is not None
        except ValueError:
            return False

    def _get_type_handle(self, type_: str) -> str:
        """Returns the handle of the type.

        :param type_: the type of the node as human readable name
        :return: the handle of the type hashed with the algorithm"""
        res = self._coll_node_types.find_one({"name": type_})
        if res is None:
            raise ValueError(f"invalid type: {type_}")
        return res["_id"]

    def _get_doc(self, handle: str, coll=None) -> dict:
        """Returns the document with the given handle.
        If the document is not found, raises a ValueError.

        :param handle: the handle of the document
        :param coll: the collection name to search in
        :return: the document"""
        if coll is None:
            colls = self.ALL_COLLS
        else:
            if isinstance(coll, str):
                colls = [coll]
            else:
                raise TypeError(f"coll must be a string")
        for collection in colls:
            node = self.mongo_db[collection].find_one({"_id": handle})
            if node is not None:
                return node
        return None

    def get_node_handle(self, node_type: str, node_name: str) -> str:
        """Returns the handle of the node.

        :param node_type: the type of the node as human readable name
        :param node_name: the name of the node as human readable name
        :return: the handle of the node hashed with the algorithm"""
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
        link_handle = self._generate_link_handle(atom_type_handle, target_handles)
        targets_len = len(target_handles) + 1

        collection_name = {
            1: self.COLL_LINKS_1,
            2: self.COLL_LINKS_2,
            3: self.COLL_LINKS_3,
        }.get(targets_len, self.COLL_LINKS)

        collection = self.mongo_db[collection_name]
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
        return result.content[1:]

    def is_ordered(self, handle: str) -> bool:
        for collection in self.EXPRESSION_COLLS:
            expr = self.mongo_db[collection].find_one({"_id": handle})
            if expr is not None:
                return expr["set_from"] is None
        raise ValueError(f"invalid handle: {handle}")

    def _generate_type_link_handle(self, link_type_handle: str, target_handles: List[str]) -> str:
        doc = self._get_doc(link_type_handle, self.COLL_NODE_TYPES)
        salt = self._get_link_type_salt(doc["name"])
        target_handles = self._sort_link(doc["name"], target_handles)
        try:
            targets_types = [self._get_doc(target)["type"] if target != '*' else '*' for target in target_handles]
        except TypeError as e:
            raise ValueError(f"invalid target handles: {target_handles}") from e
        link = [doc["type"], *targets_types]

        if salt_ := salt is not None:
            link.insert(0, str(salt_))

        return Hasher.apply_alg("".join(link))

    def _generate_link_handle(
        self, link_type_handle: str, target_handles: List[str]
    ) -> str:
        target_type = self._generate_type_link_handle(link_type_handle, target_handles)
        handles = [target_type, link_type_handle, *target_handles]
        return Hasher.apply_alg("".join(handles))

    def _sort_link(self, link_type: str, target_handles: List[str]) -> str:
        if link_type in ["Set", "Similarity"]:
            return sorted(target_handles)
        return target_handles[:]

    def _get_link_type_salt(self, link_type: str) -> str:
        return {
            "Set": "1",
            "Similarity": "2",
        }.get(link_type, None)

    def get_matched_links(self, link_type: str, target_handles: List[str]) -> List[str]:
        atom_type_handle = self._get_type_handle(link_type)
        link_handle = self._get_matched_handle(atom_type_handle, target_handles)
        collection = self.couch_db.collection(self.C_COLL_INCOMING_NAME)
        try:
            result = collection.get(link_handle)
        except DocumentNotFoundException as e:
            return []
        return result.content

    def _get_matched_handle(self, link_type: str, target_handles: List[str]) -> str:
        target_handles = self._sort_link(link_type, target_handles)
        return Hasher.apply_alg("".join([link_type, *target_handles]))

    def get_all_nodes(self, node_type: str) -> List[str]:
        type_handle = self._get_type_handle(node_type)
        return [node['name'][len(node_type) + 2:-1] for node in self._coll_nodes.find({"type": type_handle})]
        

