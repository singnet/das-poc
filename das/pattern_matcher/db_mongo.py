from typing import Any, List, Union

from pymongo.database import Database

from das.hashing import Hasher, flatten_list
from das.helpers import keys_as_list
from das.pattern_matcher.db_interface import DBInterface, WILDCARD
from das.mongo_schema import (
    CollectionNames as MongoCollectionNames,
    FieldNames as MongoFieldNames,
)


UNORDERED_LINK_TYPES = ["Similarity", "Set"]


def build_mongo_node_name(node_type: str, node_name: str) -> str:
    return f'"{node_type}:{node_name}"'


def calc_arity(keys: List[str]) -> int:
    return len(keys) - 1


class DASMongoDB(DBInterface):
    def __init__(self, mongo_db: Database):
        self.mongo_db = mongo_db
        self.mongo_link_collection = {
            "1": self.mongo_db.get_collection(MongoCollectionNames.LINKS_ARITY_1),
            "2": self.mongo_db.get_collection(MongoCollectionNames.LINKS_ARITY_2),
            "N": self.mongo_db.get_collection(MongoCollectionNames.LINKS_ARITY_N),
        }
        self.node_handles = None
        self.node_documents = None
        self.atom_type_hash = None
        self._prefetch()

    def _prefetch(self) -> None:
        self.node_handles = {}
        self.node_documents = {}
        self.atom_type_hash = {}
        self.type_hash = {}
        collection = self.mongo_db.get_collection(MongoCollectionNames.NODES)
        for document in collection.find():
            self.node_documents[document[MongoFieldNames.ID_HASH]] = document
            self.node_handles[document[MongoFieldNames.NODE_NAME]] = document[
                MongoFieldNames.ID_HASH
            ]
        collection = self.mongo_db.get_collection(MongoCollectionNames.ATOM_TYPES)
        for document in collection.find():
            self.atom_type_hash[document[MongoFieldNames.TYPE_NAME]] = document[
                MongoFieldNames.ID_HASH
            ]
            self.type_hash[document[MongoFieldNames.ID_HASH]] = document[
                MongoFieldNames.TYPE
            ]

    def _retrieve_mongo_documents_by_type_match(self, types: List[str], arity=-1) -> List[dict]:
        mongo_filter = {
            f"{MongoFieldNames.TYPE}.{i}": type_
            for i, type_ in enumerate(types)
            if type_ != WILDCARD
        }
        if arity > 0:
            if arity == 2:
                collection = self.mongo_link_collection["2"]
            elif arity == 1:
                collection = self.mongo_link_collection["1"]
            else:
                collection = self.mongo_link_collection["N"]
            return collection.find(mongo_filter)
        for collection in [self.mongo_link_collection[key] for key in ["2", "1", "N"]]:
            document = collection.find(mongo_filter)
            if document:
                return document
        return None

    def _retrieve_mongo_document(self, handle: str, arity=-1) -> dict:
        mongo_filter = {MongoFieldNames.ID_HASH: handle}
        if arity > 0:
            if arity == 2:
                collection = self.mongo_link_collection["2"]
            elif arity == 1:
                collection = self.mongo_link_collection["1"]
            else:
                collection = self.mongo_link_collection["N"]
            return collection.find_one(mongo_filter)
        document = self.node_documents.get(handle, None)
        if document:
            return document
        for collection in [self.mongo_link_collection[key] for key in ["2", "1", "N"]]:
            document = collection.find_one(mongo_filter)
            if document:
                return document
        return None

    def _retrieve_mongo_document_by_keys(self, keys: List[str]) -> dict:
        arity = calc_arity(keys)
        if arity < 3:
            mongo_filter = {
                f"key{i}": key for i, key in enumerate(keys, start=1) if key != WILDCARD
            }
        else:
            mongo_filter = {
                f"key.{i}": key for i, key in enumerate(keys) if key != WILDCARD
            }

        if arity == 2:
            collection = self.mongo_link_collection["2"]
        elif arity == 1:
            collection = self.mongo_link_collection["1"]
        else:
            collection = self.mongo_link_collection["N"]
        return collection.find(mongo_filter)

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
                if document is None:
                    raise ValueError(f"Target handle {handle} not found")
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

    # DB interface methods

    def node_exists(self, node_type: str, node_name: str) -> bool:
        return build_mongo_node_name(node_type, node_name) in self.node_handles

    def link_exists(self, link_type: str, target_handles: List[str]) -> bool:
        try:
            link_handle = self._build_link_handle(link_type, target_handles)
        except ValueError:
            return False
        document = self._retrieve_mongo_document(link_handle, len(target_handles))
        return document is not None

    def get_node_handle(self, node_type: str, node_name: str) -> str:
        try:
            return self.node_handles[build_mongo_node_name(node_type, node_name)]
        except KeyError:
            raise ValueError(f"Invalid node: type={node_type} name={node_name}")

    def get_link_handle(self, link_type: str, target_handles: List[str]) -> str:
        link_handle = self._build_link_handle(link_type, target_handles)
        document = self._retrieve_mongo_document(link_handle, len(target_handles))
        if document is None:
            raise ValueError(f"Invalid link: type={link_type} targets={target_handles}")
        return link_handle

    def get_link_targets(self, link_handle: str) -> List[str]:
        for collection in self.mongo_link_collection.values():
            answer = collection.find_one({"_id": link_handle})
            if answer:
                return keys_as_list(answer)
        raise ValueError(f"Invalid handle: {link_handle}")

    def is_ordered(self, link_handle: str) -> bool:
        document = self._retrieve_mongo_document(link_handle)
        if document is None:
            raise ValueError(f"Invalid handle: {link_handle}")
        return document["set_from"] is None

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
        keys = [link_type_hash, *target_handles]
        return [doc[MongoFieldNames.ID_HASH] for doc in self._retrieve_mongo_document_by_keys(keys)]

    def get_all_nodes(self, node_type: str) -> List[str]:
        node_type_hash = self.atom_type_hash.get(node_type, None)
        if not node_type_hash:
            raise ValueError(f"Invalid node type: {node_type}")
        return [
            document[MongoFieldNames.ID_HASH]
            for document in self.node_documents.values()
            if document[MongoFieldNames.TYPE] == node_type_hash
        ]

    def get_matched_type_template(self, template: List[Any]) -> List[str]:
        return list(self._retrieve_mongo_documents_by_type_match(template))
