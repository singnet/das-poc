import os
import datetime
import time
from threading import Thread, Lock
from das.expression import Expression
from das.database.mongo_schema import CollectionNames as MongoCollections
from das.database.couchbase_schema import CollectionNames as CouchbaseCollections
from das.expression_hasher import ExpressionHasher
from das.metta_yacc import MettaYacc
from das.database.db_interface import DBInterface
from das.database.db_interface import DBInterface, WILDCARD

# There is a Couchbase limitation for long values (max: 20Mb)
# So we set the it to ~15Mb, if this max size is reached
# we create a new key to store the next 15Mb batch and so on.
# TODO: move this constant to a proper place
MAX_COUCHBASE_BLOCK_SIZE = 500000

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
            s.value: f"/tmp/MultiFileKnowledgeBase_{s.value}.txt" for s in CouchbaseCollections
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
        
def _write_key_value(file, key, value):
    if isinstance(key, list):
        key = ExpressionHasher.composite_hash(key)
    if isinstance(value, list):
        value = ",".join(value)
    line = f"{key},{value}"
    file.write(line)
    file.write("\n")

def _key_value_generator(input_filename, *, block_size=MAX_COUCHBASE_BLOCK_SIZE, merge_rest=False):
    last_key = ''
    last_list = []
    block_count = 0
    with open(input_filename, 'r') as fh:
        for line in fh:
            line = line.strip()
            if line == '':
                continue
            if merge_rest:
                v = line.split(",")
                key = v[0]
                value = ",".join(v[1:])
            else:
                key, value = line.split(",")
            if last_key == key:
                last_list.append(value)
                if len(last_list) >= block_size:
                    yield last_key, last_list, block_count
                    block_count += 1
                    last_list = []
            else:
                if last_key != '':
                    yield last_key, last_list, block_count
                block_count = 0
                last_key = key
                last_list = [value]
    if last_key != '':
        yield last_key, last_list, block_count

def _key_value_targets_generator(input_filename, *, block_size=MAX_COUCHBASE_BLOCK_SIZE/4, merge_rest=False):
    last_key = ''
    last_list = []
    block_count = 0
    with open(input_filename, 'r') as fh:
        for line in fh:
            line = line.strip()
            if line == '':
                continue
            key, value, *targets = line.split(",")
            if last_key == key:
                last_list.append({'handle':value, 'targets': targets})
                if len(last_list) >= block_size:
                    yield last_key, last_list, block_count
                    block_count += 1
                    last_list = []
            else:
                if last_key != '':
                    yield last_key, last_list, block_count
                block_count = 0
                last_key = key
                last_list = [value, *targets]
                last_list = [{'handle':value, 'targets': targets}]
    if last_key != '':
        yield last_key, last_list, block_count

class ParserThread(Thread):

    def __init__(self, parser_actions_broker: "MettaParserActions"):
        super().__init__()
        self.parser_actions_broker = parser_actions_broker

    def run(self):
        print(f"Parser thread {self.name} (TID {self.native_id}) started. Parsing {self.parser_actions_broker.file_path}")
        stopwatch_start = time.perf_counter()
        parser = MettaYacc(action_broker=self.parser_actions_broker)
        parser.parse_action_broker_input()
        self.parser_actions_broker.shared_data.parse_ok()
        elapsed = (time.perf_counter() - stopwatch_start) // 60
        print(f"Parser thread {self.name} (TID {self.native_id}) Finished. {elapsed:.0f} minutes.")

class FlushNonLinksToDBThread(Thread):

    def __init__(self, db: DBInterface, shared_data: SharedData):
        super().__init__()
        self.db = db
        self.shared_data = shared_data

    def run(self):
        print(f"Flush thread {self.name} (TID {self.native_id}) started.")
        stopwatch_start = time.perf_counter()
        bulk_insertion = []
        while self.shared_data.typedef_expressions:
            bulk_insertion.append(self.shared_data.typedef_expressions.pop().to_dict())
        if bulk_insertion:
            mongo_collection = self.db.mongo_db[MongoCollections.ATOM_TYPES]
            mongo_collection.insert_many(bulk_insertion)

        named_entities = open(self.shared_data.temporary_file_name[CouchbaseCollections.NAMED_ENTITIES], "w")
        bulk_insertion = []
        while self.shared_data.terminals:
            terminal = self.shared_data.terminals.pop()
            bulk_insertion.append(terminal.to_dict())
            _write_key_value(named_entities, terminal.hash_code, terminal.terminal_name)
        named_entities.close()
        if bulk_insertion:
            mongo_collection = self.db.mongo_db[MongoCollections.NODES]
            mongo_collection.insert_many(bulk_insertion)
        named_entities.close()
        self.shared_data.build_ok()
        elapsed = (time.perf_counter() - stopwatch_start) // 60
        print(f"Flush thread {self.name} (TID {self.native_id}) finished. {elapsed:.0f} minutes.")

class BuildConnectivityThread(Thread):

    def __init__(self, shared_data: SharedData):
        super().__init__()
        self.shared_data = shared_data

    def run(self):
        outgoing_file_name = self.shared_data.temporary_file_name[CouchbaseCollections.OUTGOING_SET]
        incoming_file_name = self.shared_data.temporary_file_name[CouchbaseCollections.INCOMING_SET]
        print(f"Temporary file builder thread {self.name} (TID {self.native_id}) started. Building {outgoing_file_name} and {incoming_file_name}")
        stopwatch_start = time.perf_counter()
        outgoing = open(outgoing_file_name, "w")
        incoming = open(incoming_file_name, "w")
        for i in range(len(self.shared_data.regular_expressions_list)):
            expression = self.shared_data.regular_expressions_list[i]
            for element in expression.elements:
                _write_key_value(outgoing, expression.hash_code, element)
                _write_key_value(incoming, element, expression.hash_code)
        outgoing.close()
        incoming.close()
        os.system(f"sort -t , -k 1,1 {outgoing_file_name} > {outgoing_file_name}.sorted")
        os.rename(f"{outgoing_file_name}.sorted", outgoing_file_name)
        os.system(f"sort -t , -k 1,1 {incoming_file_name} > {incoming_file_name}.sorted")
        os.rename(f"{incoming_file_name}.sorted", incoming_file_name)
        self.shared_data.build_ok()
        elapsed = (time.perf_counter() - stopwatch_start) // 60
        print(f"Temporary file builder thread {self.name} (TID {self.native_id}) finished. {elapsed:.0f} minutes.")

class BuildPatternsThread(Thread):

    def __init__(self, shared_data: SharedData):
        super().__init__()
        self.shared_data = shared_data

    def run(self):
        file_name = self.shared_data.temporary_file_name[CouchbaseCollections.PATTERNS]
        print(f"Temporary file builder thread {self.name} (TID {self.native_id}) started. Building {file_name}")
        stopwatch_start = time.perf_counter()
        patterns = open(file_name, "w")
        for i in range(len(self.shared_data.regular_expressions_list)):
            expression = self.shared_data.regular_expressions_list[i]
            if expression.named_type not in self.shared_data.pattern_black_list:
                arity = len(expression.elements)
                keys = []
                if arity == 1:
                    keys.append([expression.named_type_hash, WILDCARD])
                elif arity == 2:
                    keys.append([expression.named_type_hash, expression.elements[0], WILDCARD])
                    keys.append([expression.named_type_hash, WILDCARD, expression.elements[1]])
                    keys.append([expression.named_type_hash, WILDCARD, WILDCARD])
                elif arity == 3:
                    keys.append([expression.named_type_hash, expression.elements[0], expression.elements[1], WILDCARD])
                    keys.append([expression.named_type_hash, expression.elements[0], WILDCARD, expression.elements[2]])
                    keys.append([expression.named_type_hash, WILDCARD, expression.elements[1], expression.elements[2]])
                    keys.append([expression.named_type_hash, expression.elements[0], WILDCARD, WILDCARD])
                    keys.append([expression.named_type_hash, WILDCARD, expression.elements[1], WILDCARD])
                    keys.append([expression.named_type_hash, WILDCARD, WILDCARD, expression.elements[2]])
                    keys.append([expression.named_type_hash, WILDCARD, WILDCARD, WILDCARD])
            for key in keys:
                _write_key_value(patterns, key, [expression.hash_code, *expression.elements])
        patterns.close()
        os.system(f"sort -t , -k 1,1 {file_name} > {file_name}.sorted")
        os.rename(f"{file_name}.sorted", file_name)
        self.shared_data.build_ok()
        elapsed = (time.perf_counter() - stopwatch_start) // 60
        print(f"Temporary file builder thread {self.name} (TID {self.native_id}) finished. {elapsed:.0f} minutes.")

class BuildTypeTemplatesThread(Thread):

    def __init__(self, shared_data: SharedData):
        super().__init__()
        self.shared_data = shared_data

    def run(self):
        file_name = self.shared_data.temporary_file_name[CouchbaseCollections.TEMPLATES]
        print(f"Temporary file builder thread {self.name} (TID {self.native_id}) started. Building {file_name}")
        stopwatch_start = time.perf_counter()
        template = open(file_name, "w")
        for i in range(len(self.shared_data.regular_expressions_list)):
            expression = self.shared_data.regular_expressions_list[i]
            _write_key_value(
                template,
                expression.composite_type_hash, 
                [expression.hash_code, *expression.elements])
        template.close()
        os.system(f"sort -t , -k 1,1 {file_name} > {file_name}.sorted")
        os.rename(f"{file_name}.sorted", file_name)
        self.shared_data.build_ok()
        elapsed = (time.perf_counter() - stopwatch_start) // 60
        print(f"Temporary file builder thread {self.name} (TID {self.native_id}) finished. {elapsed:.0f} minutes.")

class PopulateMongoDBLinksThread(Thread):

    def __init__(self, db: DBInterface, shared_data: SharedData):
        super().__init__()
        self.db = db
        self.shared_data = shared_data

    def run(self):
        print(f"MongoDB links uploader thread {self.name} (TID {self.native_id}) started.")
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
            mongo_collection.insert_many(bulk_insertion_1)
        if bulk_insertion_2:
            mongo_collection = self.db.mongo_db[MongoCollections.LINKS_ARITY_2]
            mongo_collection.insert_many(bulk_insertion_2)
        if bulk_insertion_N:
            mongo_collection = self.db.mongo_db[MongoCollections.LINKS_ARITY_N]
            mongo_collection.insert_many(bulk_insertion_N)
        self.shared_data.mongo_uploader_ok = True
        elapsed = (time.perf_counter() - stopwatch_start) // 60
        print(f"MongoDB links uploader thread {self.name} (TID {self.native_id}) finished. {elapsed:.0f} minutes.")

class PopulateCouchbaseCollectionThread(Thread):
    
    def __init__(
        self, 
        db: DBInterface, 
        shared_data: SharedData, 
        collection_name: str,
        use_targets: bool,
        merge_rest: bool):

        super().__init__()
        self.db = db
        self.shared_data = shared_data
        self.collection_name = collection_name
        self.use_targets = use_targets
        self.merge_rest = merge_rest

    def run(self):
        file_name = self.shared_data.temporary_file_name[self.collection_name]
        couchbase_collection = self.db.couch_db.collection(self.collection_name)
        print(f"Couchbase collection uploader thread {self.name} (TID {self.native_id}) started. Uploading {self.collection_name}")
        stopwatch_start = time.perf_counter()
        generator = _key_value_targets_generator if self.use_targets else _key_value_generator
        for key, value, block_count in generator(file_name, merge_rest=self.merge_rest):
            if block_count == 0:
                couchbase_collection.upsert(key, value, timeout=datetime.timedelta(seconds=100))
            else:
                if block_count == 1:
                    first_block = couchbase_collection.get(key)
                    couchbase_collection.upsert(f"{key}_0", first_block.content, timeout=datetime.timedelta(seconds=100))
                couchbase_collection.upsert(key, block_count + 1)
                couchbase_collection.upsert(f"{key}_{block_count}", v, timeout=datetime.timedelta(seconds=100))
        elapsed = (time.perf_counter() - stopwatch_start) // 60
        self.shared_data.process_ok()
        print(f"Couchbase collection uploader thread {self.name} (TID {self.native_id}) finished. {elapsed:.0f} minutes.")
