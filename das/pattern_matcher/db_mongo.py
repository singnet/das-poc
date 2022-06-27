from typing import List

from pymongo.database import Database

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
        if hasattr(self, "couch_db"):
            del self.couch_db

    def get_link_targets(self, handle: str) -> List[str]:
        for col in self.EXPRESSION_COLLS:
            res = self.mongo_db[col].find_one({"_id": handle})
            if res is not None:
                return [v for k, v in sorted(res.items()) if k.startswith("key")]
        raise ValueError(f"invalid handle: {handle}")

    def get_matched_links(self, link_type: str, target_handles: List[str]) -> List[str]:
        is_hidden_link = link_type.title() in ["Set", "List"]

        link_type_handle = self._get_type_handle(link_type)
        collection_name = {
            1: self.COLL_LINKS_1,
            2: self.COLL_LINKS_2,
            3: self.COLL_LINKS_3,
        }.get(len(target_handles) + int(is_hidden_link), self.COLL_LINKS)

        collection = self.mongo_db[collection_name]

        filter_params = {}
        key_index = 1

        if not is_hidden_link:
            filter_params.update(
                {
                    f"key{key_index}": link_type_handle,
                }
            )
            key_index += 1

        filter_params.update({
            f"key{i}": handle
            for i, handle in enumerate(target_handles, start=key_index)
            if handle != "*"
        })

        if collection_name == self.COLL_LINKS:
            raise NotImplementedError("Not implented yet")

        return [doc["_id"] for doc in collection.find(filter_params)]
