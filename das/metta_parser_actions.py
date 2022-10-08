import tempfile
import datetime
import time
import os
from typing import List, Tuple
from abc import ABC, abstractmethod
from das.expression import Expression
from das.expression_hasher import ExpressionHasher
from das.database.db_interface import DBInterface, WILDCARD
from das.database.mongo_schema import CollectionNames as MongoCollections
from das.database.couchbase_schema import CollectionNames as CouchbaseCollections

PATTERN_BLACK_LIST = []
# There is a Couchbase limitation for long values (max: 20Mb)
# So we set the it to ~15Mb, if this max size is reached
# we create a new key to store the next 15Mb batch and so on.
MAX_COUCHBASE_BLOCK_SIZE = 500000


class MettaParserActions(ABC):

    @abstractmethod
    def next_input_chunk(self) -> Tuple[str, str]:
        pass

    @abstractmethod
    def new_top_level_expression(self, expression: Expression):
        pass

    @abstractmethod
    def new_expression(self, expression: Expression):
        pass

    @abstractmethod
    def new_terminal(self, expression: Expression):
        pass

    @abstractmethod
    def new_top_level_typedef_expression(self, expression: Expression):
        pass

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

class MultiFileKnowledgeBase(MettaParserActions):

    def __init__(self, db: DBInterface, file_list: List[str], show_progress_bar=True):
        self.db = db
        self.file_list = [f for f in file_list]
        self.finished = False
        self.regular_expressions = set()
        self.typedef_expressions = set()
        self.terminals = set()
        self.temporary_file_name = {
            s.value: f"/tmp/MultiFileKnowledgeBase_{s.value}.txt" for s in CouchbaseCollections
        }
        self.show_progress_bar = show_progress_bar
        self.stopwatch_start = None


    def _build_dict_list(self, expressions):
        answer = []
        while expressions:
            answer.append(expressions.pop().to_dict())
        return answer

    def _flush_typedef(self):
        bulk_insertion = []
        while self.typedef_expressions:
            bulk_insertion.append(self.typedef_expressions.pop().to_dict())
        if bulk_insertion:
            mongo_collection = self.db.mongo_db[MongoCollections.ATOM_TYPES]
            mongo_collection.insert_many(bulk_insertion)

    def _flush_nodes(self):
        named_entities = open(self.temporary_file_name[CouchbaseCollections.NAMED_ENTITIES], "w")
        bulk_insertion = []
        while self.terminals:
            terminal = self.terminals.pop()
            bulk_insertion.append(terminal.to_dict())
            _write_key_value(named_entities, terminal.hash_code, terminal.terminal_name)
        named_entities.close()
        if bulk_insertion:
            mongo_collection = self.db.mongo_db[MongoCollections.NODES]
            mongo_collection.insert_many(bulk_insertion)

    def _print_progress_bar(self, done, total, length=50):
        filled_length = int(length * done // total)
        previous = int(length * (done - 1) // total)
        if done == 1 or filled_length > previous or done == total:
            percent = ("{0:.0f}").format(100 * (done / float(total)))
            fill='█'
            bar = fill * filled_length + '-' * (length - filled_length)
            elapsed = (time.perf_counter() - self.stopwatch_start) / 60
            print(f'\rProgress: |{bar}| {percent}% complete ({done}/{total}) {elapsed:.0f} minutes', end = '\r')
            if done == total: 
                print()
    
    def _sort_temporary_files(self):
        for file_name in self.temporary_file_name.values():
            os.system(f"sort -t , -k 1,1 {file_name} > {file_name}.sorted")
            os.rename(f"{file_name}.sorted", file_name)

    def _build_temporary_files(self):

        outgoing = open(self.temporary_file_name[CouchbaseCollections.OUTGOING_SET], "w")
        incoming = open(self.temporary_file_name[CouchbaseCollections.INCOMING_SET], "w")
        patterns = open(self.temporary_file_name[CouchbaseCollections.PATTERNS], "w")
        template = open(self.temporary_file_name[CouchbaseCollections.TEMPLATES], "w")

        if self.show_progress_bar:
            self.stopwatch_start = time.perf_counter()
            total_entries = len(self.regular_expressions)
            done = 0

        for expression in self.regular_expressions:
            # Outgoing and incoming sets
            for element in expression.elements:
                _write_key_value(outgoing, expression.hash_code, element)
                _write_key_value(incoming, element, expression.hash_code)
            # Patterns
            if expression.named_type not in PATTERN_BLACK_LIST:
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
            # Type templates
            _write_key_value(
                template,
                expression.composite_type_hash, 
                [expression.hash_code, *expression.elements])
            if self.show_progress_bar:
                done += 1
                if done % 1000 == 0 or done >= total_entries:
                    self._print_progress_bar(done, total_entries)

        outgoing.close()
        incoming.close()
        patterns.close()
        template.close()

    def _process_temporary_file(self, collection_name, use_targets=False, merge_rest=False):
        file_name = self.temporary_file_name[collection_name]
        if self.show_progress_bar:
            self.stopwatch_start = time.perf_counter()
            with open(file_name, 'r') as f:
                total_entries = len(f.readlines())
            i = 0
            done = 0
            print(f"Processing {collection_name}...")
        couchbase_collection = self.db.couch_db.collection(collection_name)
        generator = _key_value_targets_generator if use_targets else _key_value_generator
        for key, value, block_count in generator(file_name, merge_rest=merge_rest):
            if block_count == 0:
                couchbase_collection.upsert(key, value, timeout=datetime.timedelta(seconds=100))
            else:
                assert False
                if block_count == 1:
                    first_block = couchbase_collection.get(key)
                    couchbase_collection.upsert(f"{key}_0", first_block.content, timeout=datetime.timedelta(seconds=100))
                couchbase_collection.upsert(key, block_count + 1)
                couchbase_collection.upsert(f"{key}_{block_count}", v, timeout=datetime.timedelta(seconds=100))
            if self.show_progress_bar:
                i += 1
                done += len(value)
                if i % 100000 == 0 or done >= total_entries:
                    self._print_progress_bar(done, total_entries)
        
    def _flush_links(self):

        # Build temporary files to be used to populate Couchbase
        print("(2/5) Pre-processing links...")
        self._build_temporary_files()
        print("(3/5) Pre-processing links a bit more...")
        self._sort_temporary_files()

        # Populates MongoDB
        print("(4/5) Adding links to MongoDB...")
        bulk_insertion_1 = []
        bulk_insertion_2 = []
        bulk_insertion_N = []
        while self.regular_expressions:
            expression = self.regular_expressions.pop()
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

        print("(5/5) Creating Couchbase tables...")
        # Populates Couchbase using temporary files
        self._process_temporary_file(CouchbaseCollections.OUTGOING_SET, use_targets=False, merge_rest=False)
        self._process_temporary_file(CouchbaseCollections.INCOMING_SET, use_targets=False, merge_rest=False)
        self._process_temporary_file(CouchbaseCollections.PATTERNS, use_targets=True, merge_rest=False)
        self._process_temporary_file(CouchbaseCollections.TEMPLATES, use_targets=True, merge_rest=False)
        self._process_temporary_file(CouchbaseCollections.NAMED_ENTITIES, use_targets=False, merge_rest=True)
            
    def _flush_expressions_to_db(self):
        print("Adding data to DB (5 steps)")
        print("(1/5) Processing types and nodes...")
        self._flush_typedef()
        self._flush_nodes()
        self._flush_links()
        print("Adding data to DB - DONE")

    def next_input_chunk(self) -> Tuple[str, str]:
        file_path = self.file_list.pop(0) if self.file_list else None
        if file_path is None:
            print("NO MORE FILES")
            self._flush_expressions_to_db()
            self.finished = True
            return (None, None)
        with open(file_path, "r") as file_handle:
            text = file_handle.read()
        print(f"Parsing file: {file_path}")
        return (text, file_path)

    def new_top_level_expression(self, expression: Expression):
        #print(f"TOPLEVEL EXPRESSION: <{expression}>")
        #print(expression.to_json())
        self.regular_expressions.add(expression)

    def new_expression(self, expression: Expression):
        #print(f"EXPRESSION: <{expression}>")
        #print(expression.to_json())
        self.regular_expressions.add(expression)

    def new_terminal(self, expression: Expression):
        #print(f"TERMINAL: <{expression}>")
        #print(expression.to_json())
        self.terminals.add(expression)

    def new_top_level_typedef_expression(self, expression: Expression):
        #print(f"TYPEDEF: <{expression}>")
        #print(expression.to_json())
        self.typedef_expressions.add(expression)
