from pymongo.database import Database
from typing import List, Any
from functools import cmp_to_key

from .db_interface import WILDCARD, UNORDERED_LINK_TYPES
from das.helpers import keys_as_list
from das.mongo_schema import CollectionNames as MongoCollectionNames, FieldNames as MongoFieldNames
from .couch_mongo_db import CouchMongoDB

class MongoDB(CouchMongoDB):

    def __init__(self, mongo_db: Database): 
        super().__init__(None, mongo_db)

    def _retrieve_mongo_unordered_document_by_keys(self, keys: List[str]) -> List[dict]:
        arity = len(keys) - 1
        assert arity < 3

        if arity == 2:
            collection = self.mongo_link_collection["2"]
        elif arity == 1:
            collection = self.mongo_link_collection["1"]
        type_hash = keys[0]
        targets = [key for key in keys[1:] if key != WILDCARD]
        n = len(targets)
        answer = []
        if n == 0:
            mongo_filter = {
                f'{MongoFieldNames.KEY_PREFIX}{i}': h for i, h in enumerate([type_hash, *targets], start=1)
            }
            return collection.find(mongo_filter)
        elif n == 1:
            if arity == 2:
                mongo_filter1 = {
                    f'{MongoFieldNames.KEY_PREFIX}1': type_hash,
                    f'{MongoFieldNames.KEY_PREFIX}2': targets[0]
                }
                mongo_filter2 = {
                    f'{MongoFieldNames.KEY_PREFIX}1': type_hash,
                    f'{MongoFieldNames.KEY_PREFIX}3': targets[0]
                }
                answer = [document for document in collection.find(mongo_filter1)]
                for document in collection.find(mongo_filter2):
                    answer.append(document)
                return answer
            else:
                raise NotImplemented('Not implemented for links with arity > 2')
        else:
            raise NotImplemented('Not implemented for links with arity > 2')

    def _retrieve_mongo_ordered_document_by_keys(self, keys: List[str]) -> List[dict]:
        arity = len(keys) - 1
        if arity < 3:
            mongo_filter = {
                f'{MongoFieldNames.KEY_PREFIX}{i}': h for i, h in enumerate(keys, start=1) if h != WILDCARD
            }
            if arity == 2:
                collection = self.mongo_link_collection["2"]
            elif arity == 1:
                collection = self.mongo_link_collection["1"]
        else:
            mongo_filter = {
                f'{MongoFieldNames.KEY_PREFIX}.{i}': h for i, h in enumerate(keys) if h != WILDCARD
            }
            collection = self.mongo_link_collection["N"]
        return [document for document in collection.find(mongo_filter)]

    def _retrieve_mongo_document_by_keys(self, keys: List[str]) -> List[dict]:
        assert keys[0] != WILDCARD
        type_name = self.atom_type_hash_reverse.get(keys[0], None)
        if not type_name:
            return []
        if type_name in UNORDERED_LINK_TYPES:
            return self._retrieve_mongo_unordered_document_by_keys(keys)
        else:
            return self._retrieve_mongo_ordered_document_by_keys(keys)

    def _retrieve_mongo_documents_by_type_match(self, types: List[Any]) -> List[dict]:
        arity = len(types) - 1
        mongo_filter = {
            f"type": types
        }
        if arity == 2:
            collection = self.mongo_link_collection["2"]
        elif arity == 1:
            collection = self.mongo_link_collection["1"]
        else:
            collection = self.mongo_link_collection["N"]
        return [document for document in collection.find(mongo_filter)]

    def get_link_targets(self, link_handle: str) -> List[str]:
        for collection in self.mongo_link_collection.values():
            answer = collection.find_one({"_id": link_handle})
            if answer:
                return keys_as_list(answer)
        raise ValueError(f"Invalid handle: {link_handle}")

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
        return [{
            'handle': document[MongoFieldNames.ID_HASH],
            'targets': self._get_mongo_document_keys(document)[1:]
        } for document in self._retrieve_mongo_document_by_keys(keys)]

    def get_matched_type_template(self, template: List[Any]) -> List[str]:
        try:
            template = self._build_hash_template(template)
        except KeyError as exception:
            raise ValueError(f'{exception}\nInvalid type')
        return [{
            'handle': document[MongoFieldNames.ID_HASH],
            'targets': self._get_mongo_document_keys(document)[1:]
        } for document in self._retrieve_mongo_documents_by_type_match(template)]
