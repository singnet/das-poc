"""
Distributed Atom Space

"""

import os
import json
from time import sleep
from typing import List, Optional, Union, Tuple, Dict
from pymongo import MongoClient as MongoDBClient
from redis import Redis
from redis.cluster import RedisCluster
from enum import Enum, auto
from das.parser_actions import KnowledgeBaseFile, MultiThreadParsing
from das.parser_threads import (
    SharedData,
    ParserThread,
    FlushNonLinksToDBThread,
    BuildConnectivityThread,
    BuildPatternsThread,
    BuildTypeTemplatesThread,
    PopulateMongoDBLinksThread,
    PopulateRedisCollectionThread
)
from das.logger import logger
from das.database.redis_mongo_db import RedisMongoDB
from das.database.mongo_schema import CollectionNames as MongoCollections
from das.database.key_value_schema import CollectionNames as KeyPrefix
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
        logger().info(f"Connecting to MongoDB at {hostname}:{port}")
        self.mongo_db = MongoDBClient(f'mongodb://{username}:{password}@{hostname}:{port}')[self.database_name]

        hostname = os.environ.get('DAS_REDIS_HOSTNAME')
        port = os.environ.get('DAS_REDIS_PORT')
        #TODO fix this to use a proper parameter
        if port == "7000":
            logger().info(f"Connecting to Redis cluster at {hostname}:{port}")
            self.redis = RedisCluster(host=hostname, port=port, decode_responses=False)
        else:
            self.redis = Redis(host=hostname, port=port, decode_responses=False)
            logger().info(f"Connecting to standalone Redis at {hostname}:{port}")

        self.db = RedisMongoDB(self.redis, self.mongo_db)
        logger().info(f"Prefetching data")
        self.db.prefetch()
        logger().info(f"Database setup finished")

    def _log_mongodb_counts(self):
        tags = [
            MongoCollections.ATOM_TYPES, 
            MongoCollections.NODES, 
            MongoCollections.LINKS_ARITY_1, 
            MongoCollections.LINKS_ARITY_2, 
            MongoCollections.LINKS_ARITY_N]
        names = [
            "types",
            "nodes",
            "links with arity == 1",
            "links with arity == 2",
            "links with arity >= 3"]
        for tag, name in zip(tags, names):
            mongo_collection = self.db.mongo_db[tag]
            count = mongo_collection.count_documents({})
            logger().info(f"Number of {name} in MongoDB: {count}")
            
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
            PopulateRedisCollectionThread(self.db, shared_data, KeyPrefix.OUTGOING_SET, False, False, update),
            PopulateRedisCollectionThread(self.db, shared_data, KeyPrefix.INCOMING_SET, False, False, update),
            PopulateRedisCollectionThread(self.db, shared_data, KeyPrefix.PATTERNS, True, False, update),
            PopulateRedisCollectionThread(self.db, shared_data, KeyPrefix.TEMPLATES, True, False, update),
            PopulateRedisCollectionThread(self.db, shared_data, KeyPrefix.NAMED_ENTITIES, False, True, update)
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

    def clear_database(self) -> None:
        """
        Clear all data from the connected MongoDB and Redis databases.

        This method drops all collections in the MongoDB database and flushes all data
        from the Redis cache, effectively wiping the databases clean.
        """
        collections = self.mongo_db.list_collection_names()
        
        for collection in collections:
            self.mongo_db[collection].drop()
        
        self.redis.flushall()

    def count_atoms(self) -> Tuple[int, int]:
        """
        This method is useful for returning the count of atoms in the database.
        It's also useful for ensuring that the knowledge base load went off without problems.

        Returns:
            Tuple[int, int]: (node_count, link_count)
        """
        return self.db.count_atoms()

    def get_atom(
        self,
        handle: str,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE
    ) -> Union[str, Dict]:
        """
        Retrieve information about an Atom using its handle.

        This method retrieves information about an Atom from the database
        based on the provided handle. The retrieved atom information can be
        presented in different output formats as specified by the output_format parameter.

        Args:
            handle (str): The unique handle of the atom.
            output_format (QueryOutputFormat, optional): The desired output format.
                Defaults to QueryOutputFormat.HANDLE.

        Returns:
            Union[str, Dict]: Depending on the output_format, returns either:
                - A string representing the handle of the Atom (output_format == QueryOutputFormat.HANDLE),
                - A dictionary containing detailed Atom information (output_format == QueryOutputFormat.ATOM_INFO),
                - A JSON-formatted string representing the deep representation of the Atom (output_format == QueryOutputFormat.JSON).

        Raises:
            ValueError: If an invalid output format is provided.

        Example:
            >>> result = obj.get_atom(
                    handle="af12f10f9ae2002a1607ba0b47ba8407",
                    output_format=QueryOutputFormat.ATOM_INFO
                )
            >>> print(result)
            {
                "handle": "af12f10f9ae2002a1607ba0b47ba8407",
                "type": "Concept",
                "name": "human"
            }
        """
    
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

    def get_node(
        self,
        node_type: str,
        node_name: str,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE
    ) -> Union[str, Dict]:
        """
        Retrieve information about a Node of a specific type and name.
        
        This method retrieves information about a Node from the database
        based on its type and name. The retrieved node information can be
        presented in different output formats as specified by the output_format parameter.

        Args:
            node_type (str): The type of the node being queried.
            node_name (str): The name of the specific node being queried.
            output_format (QueryOutputFormat, optional): The desired output format.
                Defaults to QueryOutputFormat.HANDLE.

        Returns:
            Union[str, Dict]: Depending on the output_format, returns either:
                - A string representing the handle of the node (output_format == QueryOutputFormat.HANDLE),
                - A dictionary containing atom information of the node (output_format == QueryOutputFormat.ATOM_INFO),
                - A JSON-formatted string representing the deep representation of the node (output_format == QueryOutputFormat.JSON).

        Raises:
            ValueError: If an invalid output format is provided.

        Note:
            If the specified node does not exist, a warning is logged and None is returned.

        Example:
            >>> result = obj.get_node(
                    node_type='Concept',
                    node_name='human',
                    output_format=QueryOutputFormat.ATOM_INFO
                )
            >>> print(result)
            {
                "handle": "af12f10f9ae2002a1607ba0b47ba8407",
                "type": "Concept",
                "name": "human"
            }
        """
        
        node_handle = None
        
        try:
            node_handle = self.db.get_node_handle(node_type, node_name)
        except ValueError:
            logger().warning(f"Attempt to access an invalid Node '{node_type}:{node_name}'")
            return None
        
        if output_format == QueryOutputFormat.HANDLE or not node_handle:
            return node_handle
        elif output_format == QueryOutputFormat.ATOM_INFO:
            return self.db.get_atom_as_dict(node_handle)
        elif output_format == QueryOutputFormat.JSON:
            answer = self.db.get_atom_as_deep_representation(node_handle)
            return json.dumps(answer, sort_keys=False, indent=4)
        else:
            raise ValueError(f"Invalid output format: '{output_format}'")

    def get_nodes(
        self,
        node_type: str,
        node_name: str = None,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE
    ) -> Union[List[str], List[Dict]]:
        """
        Retrieve information about Nodes based on their type and optional name.

        This method retrieves information about nodes from the database based
        on its type and name (if provided). The retrieved nodes information can be
        presented in different output formats as specified by the output_format parameter.
        

        Args:
            node_type (str): The type of nodes being queried.
            node_name (str, optional): The name of the specific node being queried. Defaults to None.
            output_format (QueryOutputFormat, optional): The desired output format.
                Defaults to QueryOutputFormat.HANDLE.

        Returns:
            Union[List[str], List[Dict]]: Depending on the output_format, returns either:
                - A list of strings representing handles of the nodes (output_format == QueryOutputFormat.HANDLE),
                - A list of dictionaries containing atom information of the nodes (output_format == QueryOutputFormat.ATOM_INFO),
                - A JSON-formatted string representing the deep representation of the nodes (output_format == QueryOutputFormat.JSON).

        Raises:
            ValueError: If an invalid output format is provided.

        Note:
            If node_name is provided and the specified node does not exist, an empty list is returned.

        Example:
            >>> result = obj.get_nodes(
                    node_type='Concept',
                    output_format=QueryOutputFormat.HANDLE
                )
            >>> print(result)
            [
                'af12f10f9ae2002a1607ba0b47ba8407',
                '1cdffc6b0b89ff41d68bec237481d1e1',
                '5b34c54bee150c04f9fa584b899dc030',
                'c1db9b517073e51eb7ef6fed608ec204',
                ...
            ]
        """
        
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

    def get_link(
        self,
        link_type: str,
        targets: List[str] = None,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE
    ) -> Union[str, Dict]:
        """
        Retrieve information about a link of a specific type and its targets.

        This method retrieves information about a link from the database based on
        type with given targets. The retrieved link information can be presented in different
        output formats as specified by the output_format parameter.

        Args:
            link_type (str): The type of the link being queried.
            targets (List[str], optional): A list of target identifiers that the link is associated with.
                Defaults to None.
            output_format (QueryOutputFormat, optional): The desired output format.
                Defaults to QueryOutputFormat.HANDLE.

        Returns:
            Union[str, Dict]: Depending on the output_format, returns either:
                - A string representing the handle of the link (output_format == QueryOutputFormat.HANDLE),
                - A dictionary containing atom information of the link (output_format == QueryOutputFormat.ATOM_INFO),
                - A JSON-formatted string representing the deep representation of the link (output_format == QueryOutputFormat.JSON).

        Raises:
            ValueError: If an invalid output format is provided.

        Note:
            If the specified link or targets do not exist, the method returns None.

        Example:
            >>> result = obj.get_link(
                    link_type='Similarity',
                    targets=['human', 'monkey'],
                    output_format=QueryOutputFormat.HANDLE
                )
            >>> print(result)
            '2931276cb5bb4fc0c2c48a6720fc9a84'
        """
        link_handle = None

        # TODO: Is there any exception action?
        try:
            link_handle = self.db.get_link_handle(link_type, targets)
        except ValueError:
            pass

        if output_format == QueryOutputFormat.HANDLE or link_handle is None:
            return link_handle
        elif output_format == QueryOutputFormat.ATOM_INFO:
            return self.db.get_atom_as_dict(link_handle, len(targets))
        elif output_format == QueryOutputFormat.JSON:
            answer = self.db.get_atom_as_deep_representation(link_handle, len(targets))
            return json.dumps(answer, sort_keys=False, indent=4)
        else:
            raise ValueError(f"Invalid output format: '{output_format}'")

    def get_links(
        self,
        link_type: str,
        target_types: str = None,
        targets: List[str] = None,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE
    ) -> Union[List[str], List[Dict]]:
        """
        Retrieve information about Links based on specified criteria.

        This method retrieves information about links from the database based on the provided criteria.
        The criteria includes the link type, and can include target types and specific target identifiers.
        The retrieved links information can be presented in different output formats as specified
        by the output_format parameter.

        Args:
            link_type (str): The type of links being queried.
            target_types (str, optional): The type(s) of targets being queried. Defaults to None.
            targets (List[str], optional): A list of target identifiers that the links are associated with.
                Defaults to None.
            output_format (QueryOutputFormat, optional): The desired output format.
                Defaults to QueryOutputFormat.HANDLE.

        Returns:
            Union[List[str], List[Dict]]: Depending on the output_format, returns either:
                - A list of strings representing handles of the links (output_format == QueryOutputFormat.HANDLE),
                - A list of dictionaries containing detailed information of the links (output_format == QueryOutputFormat.ATOM_INFO),
                - A JSON-formatted string representing the deep representation of the links (output_format == QueryOutputFormat.JSON).

        Raises:
            ValueError: If an invalid output format is provided or if the provided parameters are invalid.

        Example:
            >>> result = obj.get_links(
                    link_type='Similarity',
                    target_types=['Concept', 'Concept'],
                    output_format=QueryOutputFormat.ATOM_INFO
                )
            >>> print(result)
            [
                {
                    'handle': 'a45af31b43ee5ea271214338a5a5bd61',
                    'type': 'Similarity',
                    'template': ['Similarity', 'Concept', 'Concept'],
                    'targets': [...]
                },
                {
                    'handle': '2d7abd27644a9c08a7ca2c8d68338579',
                    'type': 'Similarity',
                    'template': ['Similarity', 'Concept', 'Concept'],
                    'targets': [...]
                },
                ...
            ]
        """
        
        # TODO: Delete this If. This conditional will never happen
        if link_type is None:
            link_type = WILDCARD

        if target_types is not None and link_type != WILDCARD:
            db_answer = self.db.get_matched_type_template([link_type, *target_types])
        elif targets is not None:
            db_answer = self.db.get_matched_links(link_type, targets)
        elif link_type != WILDCARD:
            db_answer = self.db.get_matched_type(link_type)
        else:
            # TODO: Improve this message error. What is invalid?
            raise ValueError("Invalid parameters")

        if output_format == QueryOutputFormat.HANDLE:
            return self._to_handle_list(db_answer)
        elif output_format == QueryOutputFormat.ATOM_INFO:
            return self._to_link_dict_list(db_answer)
        elif output_format == QueryOutputFormat.JSON:
            return self._to_json(db_answer)
        else:
            raise ValueError(f"Invalid output format: '{output_format}'")

    def get_link_type(self, link_handle: str) -> str:
        """
        Get the type of a link.

        This method retrieves the type of a link based on its handle.

        Args:
            link_handle (str): The handle of the link.

        Returns:
            str: The type of the link.
        
        Example:
            >>> human = obj.get_node('Concept', 'human')
            >>> monkey = obj.get_node('Concept', 'monkey')
            >>> link_handle = obj.get_link('Similarity', [human, monkey])
            >>> result = obj.get_link_type(link_handle=link_handle)
            >>> print(result)
            'Similarity'            
        """
        try:
            resp = self.db.get_link_type(link_handle)
            return resp
        # TODO: Find out what specific exceptions might happen
        except Exception as e:
            logger().warning(f"An error occurred during the query. Detail:'{str(e)}'")
            raise e

    def get_link_targets(self, link_handle: str) -> List[str]:
        """
        Get the targets of a link.

        This method retrieves the targets of a link based on its handle.

        Args:
            link_handle (str): The handle of the link.

        Returns:
            List[str]: A list of target handles.
        
        Example:
            >>> human = obj.get_node('Concept', 'human')
            >>> monkey = obj.get_node('Concept', 'monkey')
            >>> link_handle = obj.get_link('Similarity', [human, monkey])
            >>> result = obj.get_link_targets(link_handle=link_handle)
            >>> print(result)
            [
                '80aff30094874e75028033a38ce677bb',
                '4e8e26e3276af8a5c2ac2cc2dc95c6d2'
            ] 
        """
        try:
            resp = self.db.get_link_targets(link_handle)
            return resp
        # TODO: Find out what specific exceptions might happen
        except Exception as e:
            logger().warning(f"An error occurred during the query. Detail:'{str(e)}'")
            raise e

    def get_node_type(self, node_handle: str) -> str:
        """
        Get the type of a node.

        This method retrieves the type of a node based on its handle.

        Args:
            node_handle (str): The handle of the node.

        Returns:
            str: The type of the node.

        Example:
            >>> human = obj.get_node('Concept', 'human')
            >>> result = obj.get_node_type(node_handle=human)
            >>> print(result)
            'Concept'
        """
        try:
            resp = self.db.get_node_type(node_handle)
            return resp
        # TODO: Find out what specific exceptions might happen
        except Exception as e:
            logger().warning(f"An error occurred during the query. Detail:'{str(e)}'")
            raise e

    def get_node_name(self, node_handle: str) -> str:
        """
        Get the name of a node.

        This method retrieves the name of a node based on its handle.

        Args:
            node_handle (str): The handle of the node.

        Returns:
            str: The name of the node.

        Example:
            >>> animal = obj.get_node('Concept', 'animal')
            >>> result = obj.get_node_name(node_handle=animal)
            >>> print(result)
            'animal'
        """
        try:
            resp = self.db.get_node_name(node_handle)
            return resp
        # TODO: Find out what specific exceptions might happen
        except Exception as e:
            logger().warning(f"An error occurred during the query. Detail:'{str(e)}'")
            raise e

    def query(
        self,
        query: LogicalExpression,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE
    ) -> str:
        """
        Perform a query on the knowledge base using a logical expression.
        
        This method allows you to query the knowledge base using a logical expression
        to find patterns or relationships among atoms. The query result is returned
        in the specified output format.

        Args:
            query (LogicalExpression): As instance of a LogicalExpression. representing the query.
            output_format (QueryOutputFormat, optional): The desired output format for the query result
                Defaults to QueryOutputFormat.HANDLE.
        
        Returns:
            str: The result of the query in the specified output format.
            
        Raises:
            ValueError: If an invalid output format is provided.
            
        Notes:
            - Each query is a LogicalExpression object that may or may not be a combination of
            logical operators like `And`, `Or`, and `Not`, as well as atomic expressions like
            `Node`, `Link`, and `Variable`.
            
            - If no match is found for the query, an empty string is returned.
            
        Example:
            You can use this method to perform complex or simple queries, like the following:
            
            In this example we want to search the knowledge base for two inheritance links
            that connect 3 nodes such that V1 -> V2 -> V3.
            
            >>> V1 = Variable("V1")
            >>> V2 = Variable("V2")
            >>> V3 = Variable("V3")
        
            >>> logical_expression = And([
                Link("Inheritance", ordered=True, targets=[V1, V2]),
                Link("Inheritance", ordered=True, targets=[V2, V3])
            ])
        
            >>> result = obj.query(query=logical_expression)
            
            >>> print(result)
            {
                {'V1': '305e7d502a0ce80b94374ff0d79a6464', 'V2': '98870929d76a80c618e70a0393055b31', 'V3': '81ec21b0f1b03e18c55e056a56179fef'},
                {'V1': 'bd497eb24420dd50fed5f3d2e6cdd7c1', 'V2': '98870929d76a80c618e70a0393055b31', 'V3': '81ec21b0f1b03e18c55e056a56179fef'},
                {'V1': 'e2d9b15ab3461228d75502e754137caa', 'V2': 'c90242e2dbece101813762cc2a83d726', 'V3': '81ec21b0f1b03e18c55e056a56179fef'},
                ...
            }


        """
        query_answer = PatternMatchingAnswer()
        
        matched = query.matched(self.db, query_answer)
        
        if not matched:
            return ""        
        
        tag_not = ""
        mapping = ""

        if query_answer.negation:
            tag_not = "NOT "
        
        if output_format == QueryOutputFormat.HANDLE:
            mapping = str(query_answer.assignments)
        elif output_format == QueryOutputFormat.ATOM_INFO:
            mapping = str({
                var: self.db.get_atom_as_dict(handle)
                for var, handle in query_answer.assignments.items()
            })
        elif output_format == QueryOutputFormat.JSON:
            mapping = json.dumps(
                {var: self.db.get_atom_as_deep_representation(handle)
                for var, handle in query_answer.assignments.items()},
                sort_keys=False, indent=4
            )
        else:
            raise ValueError(f"Invalid output format: '{output_format}'")
        
        return f"{tag_not}{mapping}"

    def open_transaction(self) -> Transaction:
        return Transaction()

    def commit_transaction(self, transaction: Transaction) -> None:
        shared_data = SharedData()
        parser_thread = ParserThread(
            MultiThreadParsing(
                self.db,
                transaction.metta_string(),
                shared_data,
                use_action_broker_cache=True
            ), 
            use_action_broker_cache=True
        )
        parser_thread.start()
        parser_thread.join()
        assert shared_data.parse_ok_count == 1
        self._process_parsed_data(shared_data, True)

    def load_knowledge_base(self, source: str) -> None:
        """
        This method parses one or more files
        and feeds the databases with all MeTTa expressions.
        
        Note:
            This method can be used for `.scm` or `.metta` files(s)
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
        logger().info(f"Finished loading knowledge base")
        self._log_mongodb_counts()

    def load_canonical_knowledge_base(self, source: str) -> None:
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
        
        Note:
            This method can be used only for `.metta` files
        """
        logger().info(f"Loading canonical knowledge base")
        knowledge_base_file_list = sorted(self._get_file_list(source), reverse=True)
        for file_name in knowledge_base_file_list:
            logger().info(f"Knowledge base file: {file_name}")
        canonical_parser = CanonicalParser(self.db, True)
        canonical_parser.pattern_black_list = self.pattern_black_list
        for file_name in knowledge_base_file_list:
            canonical_parser.parse(file_name)
        canonical_parser.populate_indexes()
        logger().info(f"Finished loading canonical knowledge base")
        self._log_mongodb_counts()
