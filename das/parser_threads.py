import os
import datetime
import time
import pickle
from threading import Thread, Lock
from das.expression import Expression
from das.database.mongo_schema import CollectionNames as MongoCollections
from das.database.key_value_schema import CollectionNames as KeyPrefix, build_redis_key
from das.metta_yacc import MettaYacc
from das.atomese_yacc import AtomeseYacc
from das.database.db_interface import DBInterface
from das.database.db_interface import DBInterface, WILDCARD
from das.logger import logger
from das.key_value_file import write_key_value, key_value_generator, key_value_targets_generator

class SharedData():
    def __init__(self):
        self.regular_expressions = set()
        self.lock_regular_expressions = Lock()

        self.typedef_expressions = set()
        self.lock_typedef_expressions = Lock()

        self.terminals = set()
        self.lock_terminals = Lock()

        self.parse_ok_count = 0
        self.lock_parse_ok_count = Lock()

        self.build_ok_count = 0
        self.lock_build_ok_count = Lock()

        self.process_ok_count = 0
        self.lock_process_ok_count = Lock()

        self.mongo_uploader_ok = False

        self.temporary_file_name = {
            s.value: f"/tmp/parser_{s.value}.txt" for s in KeyPrefix
        }
        self.pattern_black_list = []

    def add_regular_expression(self, expression: Expression) -> None:
        self.lock_regular_expressions.acquire()
        self.regular_expressions.add(expression)
        self.lock_regular_expressions.release()

    def replicate_regular_expressions(self) -> None:
        self.lock_regular_expressions.acquire()
        self.regular_expressions_list = [expression for expression in self.regular_expressions]
        self.lock_regular_expressions.release()
        
    def add_typedef_expression(self, expression: Expression) -> None:
        self.lock_typedef_expressions.acquire()
        self.typedef_expressions.add(expression)
        self.lock_typedef_expressions.release()
        
    def add_terminal(self, terminal: Expression) -> None:
        self.lock_terminals.acquire()
        self.terminals.add(terminal)
        self.lock_terminals.release()

    def parse_ok(self):
        self.lock_parse_ok_count.acquire()
        self.parse_ok_count += 1
        self.lock_parse_ok_count.release()

    def build_ok(self):
        self.lock_build_ok_count.acquire()
        self.build_ok_count += 1
        self.lock_build_ok_count.release()

    def process_ok(self):
        self.lock_process_ok_count.acquire()
        self.process_ok_count += 1
        self.lock_process_ok_count.release()
        
class ParserThread(Thread):

    def __init__(self, parser_actions_broker: "ParserActions", use_action_broker_cache: bool = False):
        super().__init__()
        self.parser_actions_broker = parser_actions_broker
        self.use_action_broker_cache = use_action_broker_cache

    def run(self):
        logger().info(f"Parser thread {self.name} (TID {self.native_id}) started. " + \
            f"Parsing {self.parser_actions_broker.file_path}")
        stopwatch_start = time.perf_counter()
        if self.parser_actions_broker.file_path.endswith(".scm"):
            parser = AtomeseYacc(action_broker=self.parser_actions_broker)
        else:
            parser = MettaYacc(
                action_broker=self.parser_actions_broker, 
                use_action_broker_cache=self.use_action_broker_cache)
        parser.parse_action_broker_input()
        self.parser_actions_broker.shared_data.parse_ok()
        elapsed = (time.perf_counter() - stopwatch_start) // 60
        logger().info(f"Parser thread {self.name} (TID {self.native_id}) Finished. " + \
            f"{elapsed:.0f} minutes.")

class FlushNonLinksToDBThread(Thread):

    def __init__(self, db: DBInterface, shared_data: SharedData, allow_duplicates: bool):
        super().__init__()
        self.db = db
        self.shared_data = shared_data
        self.allow_duplicates = allow_duplicates

    def _insert_many(self, collection, bulk_insertion):
        try:
            collection.insert_many(bulk_insertion, ordered=False)
        except Exception as e:
            if not self.allow_duplicates:
                logger().error(str(e))

    def run(self):
        logger().info(f"Flush thread {self.name} (TID {self.native_id}) started.")
        stopwatch_start = time.perf_counter()
        bulk_insertion = []
        while self.shared_data.typedef_expressions:
            bulk_insertion.append(self.shared_data.typedef_expressions.pop().to_dict())
        if bulk_insertion:
            mongo_collection = self.db.mongo_db[MongoCollections.ATOM_TYPES]
            self._insert_many(mongo_collection, bulk_insertion)

        named_entities = open(self.shared_data.temporary_file_name[KeyPrefix.NAMED_ENTITIES], "w")
        bulk_insertion = []
        while self.shared_data.terminals:
            terminal = self.shared_data.terminals.pop()
            bulk_insertion.append(terminal.to_dict())
            write_key_value(named_entities, terminal.hash_code, terminal.terminal_name)
        named_entities.close()
        if bulk_insertion:
            mongo_collection = self.db.mongo_db[MongoCollections.NODES]
            self._insert_many(mongo_collection, bulk_insertion)
        named_entities.close()
        self.shared_data.build_ok()
        elapsed = (time.perf_counter() - stopwatch_start) // 60
        logger().info(f"Flush thread {self.name} (TID {self.native_id}) finished. {elapsed:.0f} minutes.")

class BuildConnectivityThread(Thread):

    def __init__(self, shared_data: SharedData):
        super().__init__()
        self.shared_data = shared_data

    def run(self):
        outgoing_file_name = self.shared_data.temporary_file_name[KeyPrefix.OUTGOING_SET]
        incoming_file_name = self.shared_data.temporary_file_name[KeyPrefix.INCOMING_SET]
        logger().info(f"Temporary file builder thread {self.name} (TID {self.native_id}) started. Building " + \
            f"{outgoing_file_name} and {incoming_file_name}")
        stopwatch_start = time.perf_counter()
        outgoing = open(outgoing_file_name, "w")
        incoming = open(incoming_file_name, "w")
        for i in range(len(self.shared_data.regular_expressions_list)):
            expression = self.shared_data.regular_expressions_list[i]
            for element in expression.elements:
                write_key_value(outgoing, expression.hash_code, element)
                write_key_value(incoming, element, expression.hash_code)
        outgoing.close()
        incoming.close()
        os.system(f"sort -t , -k 1,1 {outgoing_file_name} > {outgoing_file_name}.sorted")
        os.rename(f"{outgoing_file_name}.sorted", outgoing_file_name)
        os.system(f"sort -t , -k 1,1 {incoming_file_name} > {incoming_file_name}.sorted")
        os.rename(f"{incoming_file_name}.sorted", incoming_file_name)
        self.shared_data.build_ok()
        elapsed = (time.perf_counter() - stopwatch_start) // 60
        logger().info(f"Temporary file builder thread {self.name} (TID {self.native_id}) finished. " + \
            f"{elapsed:.0f} minutes.")

class BuildPatternsThread(Thread):

    def __init__(self, shared_data: SharedData):
        super().__init__()
        self.shared_data = shared_data

    def run(self):
        file_name = self.shared_data.temporary_file_name[KeyPrefix.PATTERNS]
        logger().info(f"Temporary file builder thread {self.name} (TID {self.native_id}) started. " + \
            f"Building {file_name}")
        stopwatch_start = time.perf_counter()
        patterns = open(file_name, "w")
        for i in range(len(self.shared_data.regular_expressions_list)):
            expression = self.shared_data.regular_expressions_list[i]
            if expression.named_type not in self.shared_data.pattern_black_list:
                arity = len(expression.elements)
                type_hash = expression.named_type_hash
                keys = []
                keys.append([WILDCARD, *expression.elements])
                if arity == 1:
                    keys.append([type_hash, WILDCARD])
                    keys.append([WILDCARD, expression.elements[0]])
                    keys.append([WILDCARD, WILDCARD])
                elif arity == 2:
                    keys.append([type_hash, expression.elements[0], WILDCARD])
                    keys.append([type_hash, WILDCARD, expression.elements[1]])
                    keys.append([type_hash, WILDCARD, WILDCARD])
                    keys.append([WILDCARD, expression.elements[0], expression.elements[1]])
                    keys.append([WILDCARD, expression.elements[0], WILDCARD])
                    keys.append([WILDCARD, WILDCARD, expression.elements[1]])
                    keys.append([WILDCARD, WILDCARD, WILDCARD])
                elif arity == 3:
                    keys.append([type_hash, expression.elements[0], expression.elements[1], WILDCARD])
                    keys.append([type_hash, expression.elements[0], WILDCARD, expression.elements[2]])
                    keys.append([type_hash, WILDCARD, expression.elements[1], expression.elements[2]])
                    keys.append([type_hash, expression.elements[0], WILDCARD, WILDCARD])
                    keys.append([type_hash, WILDCARD, expression.elements[1], WILDCARD])
                    keys.append([type_hash, WILDCARD, WILDCARD, expression.elements[2]])
                    keys.append([type_hash, WILDCARD, WILDCARD, WILDCARD])
                    keys.append([WILDCARD, expression.elements[0], expression.elements[1], expression.elements[2]])
                    keys.append([WILDCARD, expression.elements[0], expression.elements[1], WILDCARD])
                    keys.append([WILDCARD, expression.elements[0], WILDCARD, expression.elements[2]])
                    keys.append([WILDCARD, WILDCARD, expression.elements[1], expression.elements[2]])
                    keys.append([WILDCARD, expression.elements[0], WILDCARD, WILDCARD])
                    keys.append([WILDCARD, WILDCARD, expression.elements[1], WILDCARD])
                    keys.append([WILDCARD, WILDCARD, WILDCARD, expression.elements[2]])
                    keys.append([WILDCARD, WILDCARD, WILDCARD, WILDCARD])
            for key in keys:
                write_key_value(patterns, key, [expression.hash_code, *expression.elements])
        patterns.close()
        os.system(f"sort -t , -k 1,1 {file_name} > {file_name}.sorted")
        os.rename(f"{file_name}.sorted", file_name)
        self.shared_data.build_ok()
        elapsed = (time.perf_counter() - stopwatch_start) // 60
        logger().info(f"Temporary file builder thread {self.name} (TID {self.native_id}) finished. {elapsed:.0f} minutes.")

class BuildTypeTemplatesThread(Thread):

    def __init__(self, shared_data: SharedData):
        super().__init__()
        self.shared_data = shared_data

    def run(self):
        file_name = self.shared_data.temporary_file_name[KeyPrefix.TEMPLATES]
        logger().info(f"Temporary file builder thread {self.name} (TID {self.native_id}) started. Building {file_name}")
        stopwatch_start = time.perf_counter()
        template = open(file_name, "w")
        for i in range(len(self.shared_data.regular_expressions_list)):
            expression = self.shared_data.regular_expressions_list[i]
            write_key_value(
                template,
                expression.composite_type_hash, 
                [expression.hash_code, *expression.elements])
            write_key_value(
                template,
                expression.named_type_hash,
                [expression.hash_code, *expression.elements])
        template.close()
        os.system(f"sort -t , -k 1,1 {file_name} > {file_name}.sorted")
        os.rename(f"{file_name}.sorted", file_name)
        self.shared_data.build_ok()
        elapsed = (time.perf_counter() - stopwatch_start) // 60
        logger().info(f"Temporary file builder thread {self.name} (TID {self.native_id}) finished. {elapsed:.0f} minutes.")

class PopulateMongoDBLinksThread(Thread):

    def __init__(self, db: DBInterface, shared_data: SharedData, allow_duplicates: bool):
        super().__init__()
        self.db = db
        self.shared_data = shared_data
        self.allow_duplicates = allow_duplicates

    def _insert_many(self, collection, bulk_insertion):
        try:
            collection.insert_many(bulk_insertion, ordered=False)
        except Exception as e:
            if not self.allow_duplicates:
                logger().error(str(e))

    def run(self):
        logger().info(f"MongoDB links uploader thread {self.name} (TID {self.native_id}) started.")
        duplicates = 0
        stopwatch_start = time.perf_counter()
        bulk_insertion_1 = []
        bulk_insertion_2 = []
        bulk_insertion_N = []
        for i in range(len(self.shared_data.regular_expressions_list)):
            expression = self.shared_data.regular_expressions_list[i]
            arity = len(expression.elements)
            if arity == 1:
                bulk_insertion_1.append(expression.to_dict())
            elif arity == 2:
                bulk_insertion_2.append(expression.to_dict())
            else:
                bulk_insertion_N.append(expression.to_dict())
        if bulk_insertion_1:
            mongo_collection = self.db.mongo_db[MongoCollections.LINKS_ARITY_1]
            self._insert_many(mongo_collection, bulk_insertion_1)
        if bulk_insertion_2:
            mongo_collection = self.db.mongo_db[MongoCollections.LINKS_ARITY_2]
            self._insert_many(mongo_collection, bulk_insertion_2)
        if bulk_insertion_N:
            mongo_collection = self.db.mongo_db[MongoCollections.LINKS_ARITY_N]
            self._insert_many(mongo_collection, bulk_insertion_N)
        self.shared_data.mongo_uploader_ok = True
        elapsed = (time.perf_counter() - stopwatch_start) // 60
        logger().info(f"MongoDB links uploader thread {self.name} (TID {self.native_id}) finished. " + \
            f"{duplicates} hash colisions. {elapsed:.0f} minutes.")

class PopulateRedisCollectionThread(Thread):
    
    def __init__(
        self, 
        db: DBInterface, 
        shared_data: SharedData, 
        collection_name: str,
        use_targets: bool,
        merge_rest: bool,
        update: bool):

        super().__init__()
        self.db = db
        self.shared_data = shared_data
        self.collection_name = collection_name
        self.use_targets = use_targets
        self.merge_rest = merge_rest
        self.update = update

    def run(self):
        file_name = self.shared_data.temporary_file_name[self.collection_name]
        logger().info(f"Redis collection uploader thread {self.name} (TID {self.native_id}) started. " + \
            f"Uploading {self.collection_name}")
        stopwatch_start = time.perf_counter()
        generator = key_value_targets_generator if self.use_targets else key_value_generator
        for key, value, block_count in generator(file_name, merge_rest=self.merge_rest):
            assert block_count == 0
            #print(f"file_name = {file_name} type(value) = {type(value)} type(value[0]) = {type(value[0])} value = {value}")
            if self.use_targets:
                self.db.redis.sadd(build_redis_key(self.collection_name, key), *[pickle.dumps(v) for v in value])
            else:
                self.db.redis.sadd(build_redis_key(self.collection_name, key), *value)
        elapsed = (time.perf_counter() - stopwatch_start) // 60
        self.shared_data.process_ok()
        logger().info(f"Redis collection uploader thread {self.name} (TID {self.native_id}) finished. " + \
            f"{elapsed:.0f} minutes.")
