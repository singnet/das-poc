"""
Distributed Atom Space

"""

import os
from time import sleep
from pymongo import MongoClient as MongoDBClient
from couchbase.cluster import Cluster as CouchbaseDB
from couchbase.auth import PasswordAuthenticator as CouchbasePasswordAuthenticator
from couchbase.options import LockMode as CouchbaseLockMode
from couchbase.management.collections import CollectionSpec as CouchbaseCollectionSpec
from das.parser_actions import KnowledgeBaseFile
from das.database.couch_mongo_db import CouchMongoDB
from das.database.couchbase_schema import CollectionNames as CouchbaseCollections
from das.parser_threads import SharedData, ParserThread, FlushNonLinksToDBThread, BuildConnectivityThread, \
    BuildPatternsThread, BuildTypeTemplatesThread, PopulateMongoDBLinksThread, PopulateCouchbaseCollectionThread
from das.logger import logger

class DistributedAtomSpace:

    def __init__(self, **kwargs):
        self.database_name = 'das'
        logger().info(f"New Distributed Atom Space. Database name: {self.database_name}")
        self._setup_database()

    def _setup_database(self):
        hostname = os.environ.get('DAS_MONGODB_HOSTNAME')
        port = os.environ.get('DAS_MONGODB_PORT')
        username = os.environ.get('DAS_DATABASE_USERNAME')
        password = os.environ.get('DAS_DATABASE_PASSWORD')
        mongo_db = MongoDBClient(f'mongodb://{username}:{password}@{hostname}:{port}')[self.database_name]

        hostname = os.environ.get('DAS_COUCHBASE_HOSTNAME')
        couch_db = CouchbaseDB(
            f'couchbase://{hostname}',
            authenticator=CouchbasePasswordAuthenticator(username, password),
            lockmode=CouchbaseLockMode.WAIT).bucket(self.database_name)

        collection_manager = couch_db.collections()
        for entry in CouchbaseCollections:
            try:
                collection_manager.create_collection(CouchbaseCollectionSpec(entry.value))
            except Exception:
                #TODO: should we provide a warning here?
                pass

        self.db = CouchMongoDB(couch_db, mongo_db)

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
        
    def load_knowledge_base(self, source):
        """
        Called in constructor, this method parses one or more files
        and feeds the databases with all MeTTa expressions.
        """

        logger().info(f"Loading knowledge base")
        knowledge_base_file_list = self._get_file_list(source)
        for file_name in knowledge_base_file_list:
            logger().info(f"Knowledge base file: {file_name}")
        shared_data = SharedData()

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
        shared_data.replicate_regular_expressions()

        file_builder_threads = [
            FlushNonLinksToDBThread(self.db, shared_data),
            BuildConnectivityThread(shared_data),
            BuildPatternsThread(shared_data),
            BuildTypeTemplatesThread(shared_data)
        ]
        for thread in file_builder_threads:
            thread.start()
        links_uploader_to_mongo_thread = PopulateMongoDBLinksThread(self.db, shared_data)
        links_uploader_to_mongo_thread.start()
        for thread in file_builder_threads:
            thread.join()
        assert shared_data.build_ok_count == len(file_builder_threads)

        file_processor_threads = [
            PopulateCouchbaseCollectionThread(self.db, shared_data, CouchbaseCollections.OUTGOING_SET, False, False),
            PopulateCouchbaseCollectionThread(self.db, shared_data, CouchbaseCollections.INCOMING_SET, False, False),
            PopulateCouchbaseCollectionThread(self.db, shared_data, CouchbaseCollections.PATTERNS, True, False),
            PopulateCouchbaseCollectionThread(self.db, shared_data, CouchbaseCollections.TEMPLATES, True, False),
            PopulateCouchbaseCollectionThread(self.db, shared_data, CouchbaseCollections.NAMED_ENTITIES, False, True)
        ]
        for thread in file_processor_threads:
            thread.start()
        links_uploader_to_mongo_thread.join()
        assert shared_data.mongo_uploader_ok
        for thread in file_processor_threads:
            thread.join()
        assert shared_data.process_ok_count == len(file_processor_threads)
        self.db.prefetch()
