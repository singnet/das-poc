"""
Distributed Atom Space

"""

import os
import json
from time import sleep
from typing import List, Optional, Union, Tuple, Dict
from pymongo import MongoClient as MongoDBClient
from couchbase.cluster import Cluster as CouchbaseDB
from couchbase.auth import PasswordAuthenticator as CouchbasePasswordAuthenticator
from couchbase.options import LockMode as CouchbaseLockMode
from couchbase.management.collections import CollectionSpec as CouchbaseCollectionSpec
from enum import Enum, auto
from das.parser_actions import KnowledgeBaseFile, MultiThreadParsing
from das.database.couch_mongo_db import CouchMongoDB
from das.database.couchbase_schema import CollectionNames as CouchbaseCollections
from das.parser_threads import SharedData, ParserThread, FlushNonLinksToDBThread, BuildConnectivityThread, \
    BuildPatternsThread, BuildTypeTemplatesThread, PopulateMongoDBLinksThread, PopulateCouchbaseCollectionThread
from das.logger import logger
from das.database.db_interface import WILDCARD
from das.transaction import Transaction
from das.canonical_parser import CanonicalParser
from das.pattern_matcher.pattern_matcher import PatternMatchingAnswer, LogicalExpression

class QueryOutputFormat(int, Enum):
    HANDLE = auto()
    ATOM_INFO = auto()
    JSON = auto()

class DistributedAtomSpace:

    def __init__(self, **kwargs):
        self.database_name = kwargs.get("database_name", "das")
        self.db = None
        logger().info(f"New Distributed Atom Space. Database name: {self.database_name}")
        self._setup_database()
        self.pattern_black_list = []

    def _setup_database(self):
        hostname = os.environ.get('DAS_MONGODB_HOSTNAME')
        port = os.environ.get('DAS_MONGODB_PORT')
        username = os.environ.get('DAS_DATABASE_USERNAME')
        password = os.environ.get('DAS_DATABASE_PASSWORD')
        self.mongo_db = MongoDBClient(f'mongodb://{username}:{password}@{hostname}:{port}')[self.database_name]

        hostname = os.environ.get('DAS_COUCHBASE_HOSTNAME')
        self.couch_db = CouchbaseDB(
            f'couchbase://{hostname}',
            authenticator=CouchbasePasswordAuthenticator(username, password),
            lockmode=CouchbaseLockMode.WAIT).bucket(self.database_name)

        collection_manager = self.couch_db.collections()
        for entry in CouchbaseCollections:
            try:
                collection_manager.create_collection(CouchbaseCollectionSpec(entry.value))
            except Exception:
                #TODO: should we provide a warning here?
                pass

        self.db = CouchMongoDB(self.couch_db, self.mongo_db)
        self.db.prefetch()

    def _get_file_list(self, source):
        """
        Build a list of file names according to the passed parameters.
        """
        answer = []
        if os.path.isfile(source):
            answer.append(source)
        else:
            if os.path.isdir(source):
                for file_name in os.listdir(source):
                    path = "/".join([source, file_name])
                    if os.path.exists(path):
                        answer.append(path)
            else:
                raise ValueError(f"Invalid knowledge base path: {source}")
        answer = [f for f in answer if f.endswith(".metta") or f.endswith(".scm")]
        if len(answer) == 0:
            raise ValueError(f"No MeTTa files found in {source}")
        return answer

    def _to_handle_list(self, atom_list: Union[List[str], List[Dict]]) -> List[str]:
        if not atom_list:
            return []
        if isinstance(atom_list[0], str):
            return atom_list
        else:
            return [handle for handle, _ in atom_list]

    def _to_link_dict_list(self, db_answer: Union[List[str], List[Dict]]) -> List[Dict]:
        if not db_answer:
            return []
        flat_handle = isinstance(db_answer[0], str)
        answer = []
        for atom in db_answer:
            if flat_handle:
                handle = atom
                arity = -1
            else:
                handle, targets = atom
                arity = len(targets)
            answer.append(self.db.get_atom_as_dict(handle, arity))
        return answer

    def _to_json(self, db_answer: Union[List[str], List[Dict]]) -> List[Dict]:
        answer = []
        if db_answer:
            flat_handle = isinstance(db_answer[0], str)
            for atom in db_answer:
                if flat_handle:
                    handle = atom
                    arity = -1
                else:
                    handle, targets = atom
                    arity = len(targets)
                answer.append(self.db.get_atom_as_deep_representation(handle, arity))
        return json.dumps(answer, sort_keys=False, indent=4)

    def _process_parsed_data(self, shared_data: SharedData, update: bool):
        shared_data.replicate_regular_expressions()
        file_builder_threads = [
            FlushNonLinksToDBThread(self.db, shared_data, update),
            BuildConnectivityThread(shared_data),
            BuildPatternsThread(shared_data),
            BuildTypeTemplatesThread(shared_data)
        ]
        for thread in file_builder_threads:
            thread.start()
        links_uploader_to_mongo_thread = PopulateMongoDBLinksThread(self.db, shared_data, update)
        links_uploader_to_mongo_thread.start()
        for thread in file_builder_threads:
            thread.join()
        assert shared_data.build_ok_count == len(file_builder_threads)

        file_processor_threads = [
            PopulateCouchbaseCollectionThread(self.db, shared_data, CouchbaseCollections.OUTGOING_SET, False, False, update),
            PopulateCouchbaseCollectionThread(self.db, shared_data, CouchbaseCollections.INCOMING_SET, False, False, update),
            PopulateCouchbaseCollectionThread(self.db, shared_data, CouchbaseCollections.PATTERNS, True, False, update),
            PopulateCouchbaseCollectionThread(self.db, shared_data, CouchbaseCollections.TEMPLATES, True, False, update),
            PopulateCouchbaseCollectionThread(self.db, shared_data, CouchbaseCollections.NAMED_ENTITIES, False, True, update)
        ]
        for thread in file_processor_threads:
            thread.start()
        links_uploader_to_mongo_thread.join()
        assert shared_data.mongo_uploader_ok
        for thread in file_processor_threads:
            thread.join()
        assert shared_data.process_ok_count == len(file_processor_threads)
        self.db.prefetch()


    # Public API

    def clear_database(self):
        for collection_name in self.mongo_db.collection_names():
            self.mongo_db.drop_collection(collection_name)
        collection_manager = self.couch_db.collections()
        for entry in CouchbaseCollections:
            collection_manager.drop_collection(CouchbaseCollectionSpec(entry.value))
            collection_manager.create_collection(CouchbaseCollectionSpec(entry.value))

    def count_atoms(self) -> Tuple[int, int]:
        return self.db.count_atoms()

    def get_atom(self,
        handle: str,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE) -> Union[str, Dict]:
    
        if output_format == QueryOutputFormat.HANDLE or not handle:
            atom = self.db.get_atom_as_dict(handle)
            return atom["handle"] if atom else ""
        elif output_format == QueryOutputFormat.ATOM_INFO:
            return self.db.get_atom_as_dict(handle)
        elif output_format == QueryOutputFormat.JSON:
            answer = self.db.get_atom_as_deep_representation(handle)
            return json.dumps(answer, sort_keys=False, indent=4)
        else:
            raise ValueError(f"Invalid output format: '{output_format}'")

    def get_node(self,
        node_type: str,
        node_name: str,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE) -> Union[str, Dict]:
    
        node_handle = None
        try:
            node_handle = self.db.get_node_handle(node_type, node_name)
        except ValueError:
            logger().warn(f"Attempt to access an invalid Node '{node_type}:{node_name}'")
            return None
        if output_format == QueryOutputFormat.HANDLE or node_handle is None:
            return node_handle
        elif output_format == QueryOutputFormat.ATOM_INFO:
            return self.db.get_atom_as_dict(node_handle)
        elif output_format == QueryOutputFormat.JSON:
            answer = self.db.get_atom_as_deep_representation(node_handle)
            return json.dumps(answer, sort_keys=False, indent=4)
        else:
            raise ValueError(f"Invalid output format: '{output_format}'")

    def get_nodes(self,
        node_type: str,
        node_name: str = None,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE) -> Union[str, Dict]:

        if node_name is not None:
            answer = self.db.get_node_handle(node_type, node_name)
            if answer is not None:
                answer = [answer]
        else:
            answer = self.db.get_all_nodes(node_type)
        if output_format == QueryOutputFormat.HANDLE or not answer:
            return answer
        elif output_format == QueryOutputFormat.ATOM_INFO:
            return [self.db.get_atom_as_dict(handle) for handle in answer]
        elif output_format == QueryOutputFormat.JSON:
            answer = [self.db.get_atom_as_deep_representation(handle) for handle in answer]
            return json.dumps(answer, sort_keys=False, indent=4)
        else:
            raise ValueError(f"Invalid output format: '{output_format}'")

    def get_link(self,
        link_type: str,
        targets: List[str] = None,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE) -> Union[str, Dict]:
    
        link_handle = None
        try:
            link_handle = self.db.get_link_handle(link_type, targets)
        except ValueError:
            pass

        if link_handle is None or output_format == QueryOutputFormat.HANDLE:
            return link_handle
        elif output_format == QueryOutputFormat.ATOM_INFO:
            return self.db.get_atom_as_dict(link_handle, len(targets))
        elif output_format == QueryOutputFormat.JSON:
            answer = self.db.get_atom_as_deep_representation(link_handle, len(targets))
            return json.dumps(answer, sort_keys=False, indent=4)
        else:
            raise ValueError(f"Invalid output format: '{output_format}'")

    def get_links(self,
        link_type: str,
        target_types: str = None,
        targets: List[str] = None,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE) -> Union[List[str], List[Dict]]:

        if link_type is None:
            link_type = WILDCARD

        if target_types is not None and link_type != WILDCARD:
            db_answer = self.db.get_matched_type_template([link_type, *target_types])
        elif targets is not None:
            db_answer = self.db.get_matched_links(link_type, targets)
        elif link_type != WILDCARD:
            db_answer = self.db.get_matched_type(link_type)
        else:
            raise ValueError("Invalid parameters")

        if output_format == QueryOutputFormat.HANDLE:
            return self._to_handle_list(db_answer)
        elif output_format == QueryOutputFormat.ATOM_INFO:
            return self._to_link_dict_list(db_answer)
        elif output_format == QueryOutputFormat.JSON:
            return self._to_json(db_answer)
        else:
            raise ValueError(f"Invalid output format: '{output_format}'")

    def query(self,
        query: LogicalExpression,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE) -> str:

        query_answer = PatternMatchingAnswer()
        matched = query.matched(self.db, query_answer)
        tag_not = ""
        mapping = ""
        if matched:
            if query_answer.negation:
                tag_not = "NOT "
            if output_format == QueryOutputFormat.HANDLE:
                mapping = str(query_answer.assignments)
            elif output_format == QueryOutputFormat.ATOM_INFO:
                mapping = str({
                    var: self.db.get_atom_as_dict(handle)
                    for var, handle in query_answer.assignments.items()})
            elif output_format == QueryOutputFormat.JSON:
                mapping = json.dumps({
                    var: self.db.get_atom_as_deep_representation(handle)
                    for var, handle in query_answer.assignments.items()}, sort_keys=False, indent=4)
            else:
                raise ValueError(f"Invalid output format: '{output_format}'")
        return f"{tag_not}{mapping}"

    def open_transaction(self) -> Transaction:
        return Transaction()

    def commit_transaction(self, transaction: Transaction) -> None:
        shared_data = SharedData()
        parser_thread = ParserThread(
            MultiThreadParsing(self.db, transaction.metta_string(), shared_data, use_action_broker_cache=True), 
            use_action_broker_cache=True)
        parser_thread.start()
        parser_thread.join()
        assert shared_data.parse_ok_count == 1
        self._process_parsed_data(shared_data, True)

    def load_knowledge_base(self, source):
        """
        This method parses one or more files
        and feeds the databases with all MeTTa expressions.
        """
        logger().info(f"Loading knowledge base")
        knowledge_base_file_list = self._get_file_list(source)
        for file_name in knowledge_base_file_list:
            logger().info(f"Knowledge base file: {file_name}")
        shared_data = SharedData()
        shared_data.pattern_black_list = self.pattern_black_list

        parser_threads = [
            ParserThread(KnowledgeBaseFile(self.db, file_name, shared_data))
            for file_name in knowledge_base_file_list
        ]
        for i in range(len(parser_threads)):
            parser_threads[i].start()
            # Sleep to avoid concurrency harzard in yacc lib startup
            # (which is not thread safe)
            if i < (len(parser_threads) - 1):
                sleep(10)
        for thread in parser_threads:
            thread.join()
        assert shared_data.parse_ok_count == len(parser_threads)
        self._process_parsed_data(shared_data, False)

    def load_canonical_knowledge_base(self, source):
        """
        This method loads a MeTTa knowledge base under certain assumptions:

            * The DBs are empty.
            * All MeTTa files have exactly one toplevel expression per line.
            * There are no empty lines.
            * Every "named" expressions (e.g. nodes) mentioned in a given
              expression is already mentioned in a typedef (i.e. something
              like '(: "my_node_name" my_type)' previously IN THE SAME FILE).
            * Every type mentioned in a typedef is already defined IN THE SAME FILE.
            * All expressions are normalized (regarding separators, parenthesis etc)
              like '(: "my_node_name" my_type)' or
              '(Evaluation "name" (Evaluation "name" (List "name" "name")))'. No tabs,
              no double spaces, no spaces after '(', etc.
            * All typedefs appear before any regular expressions
            * Among typedefs, any terminal types (e.g. '(: "my_node_name" my_type)') appear
              after all actual type definitions (e.g. '(: Concept Type)')
            * No "(" or ")" in atom names
            * Flat type hierarchy (i.e. all types inherit from Type)

        A typycal canonical file have all type definition expressions, followed by terminal
        type definition followed by the expressions. Something like:

        (: Evaluation Type)
        (: Predicate Type)
        (: Reactome Type)
        (: Concept Type)
        (: Set Type)
        (: "Predicate:has_name" Predicate)
        (: "Reactome:R-HSA-164843" Reactome)
        (: "Reactome:R-HSA-164842" Reactome)
        (: "Concept:2-LTR circle formation" Concept)
        (Evaluation "Predicate Predicate:has_name" (Evaluation "Predicate Predicate:has_name" (List "Reactome Reactome:R-HSA-164843" "Concept Concept:2-LTR circle formation")))
        (Evaluation "Predicate Predicate:has_name" (Evaluation "Predicate Predicate:has_name" (List "Reactome Reactome:R-HSA-164842" "Concept Concept:2-LTR circle formation B")))

        Typically this method is used to load huge knowledge bases generated (or translated
        to MeTTa) by an automated tool.
        """
        logger().info(f"Loading canonical knowledge base")
        knowledge_base_file_list = self._get_file_list(source)
        for file_name in knowledge_base_file_list:
            logger().info(f"Knowledge base file: {file_name}")
        canonical_parser = CanonicalParser(self.db, False)
        canonical_parser.pattern_black_list = self.pattern_black_list
        for file_name in knowledge_base_file_list:
            canonical_parser.parse(file_name)
        #canonical_parser.populate_indexes()
