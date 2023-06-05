from enum import Enum, auto
import datetime
import subprocess
import pickle
import sys
from das.logger import logger
from das.expression_hasher import ExpressionHasher
from das.database.key_value_schema import CollectionNames as KeyPrefix, build_redis_key
from das.database.mongo_schema import CollectionNames as MongoCollections
from das.key_value_file import write_key_value, key_value_generator, key_value_targets_generator, sort_file
import das.key_value_file
from das.database.db_interface import WILDCARD

class State(str, Enum):
    READING_TYPES = auto()
    READING_TERMINALS = auto()
    READING_EXPRESSIONS = auto()


def _file_line_count(file_name):
    output = subprocess.run(["wc", "-l", file_name], stdout=subprocess.PIPE)
    return int(output.stdout.split()[0])

EXPRESSIONS_CHUNK_SIZE = 10000000
HINT_FILE_SIZE = None
TMP_DIR = '/tmp'
#TMP_DIR = '/mnt/HD10T/nfs_share/work/tmp'
SKIP_KEY_VALUE_FILES_GENERATION = False
SKIP_PARSER = SKIP_KEY_VALUE_FILES_GENERATION or False

class CanonicalParser:

    def __init__(self, db, allow_duplicates):
        self.current_line_count = None
        self.mongo_typedef = []
        self.mongo_terminal = []
        self.mongo_expression = []
        self.typedef_mark_hash = ExpressionHasher.named_type_hash(":")
        self.base_type_hash = ExpressionHasher.named_type_hash("Type")
        self.db = db
        self.allow_duplicates = allow_duplicates
        self.temporary_file_name = {
            s.value: f"{TMP_DIR}/parser_{s.value}.txt" for s in KeyPrefix
        }
        self.pattern_black_list = None


    def _add_typedef(self, name, stype):
        stype_hash = ExpressionHasher.named_type_hash(stype)
        name_hash = ExpressionHasher.named_type_hash(name)
        composite_type = [self.typedef_mark_hash, stype_hash, self.base_type_hash]
        composite_type_hash = ExpressionHasher.composite_hash(composite_type)
        id_hash = ExpressionHasher.expression_hash(self.typedef_mark_hash, [name_hash, stype_hash])
        self.mongo_typedef.append({
            "_id": id_hash,
            "composite_type_hash": composite_type_hash,
            "named_type": name,
            "named_type_hash": name_hash,
        })
            
    def _add_terminal(self, name, stype):
        stype_hash = ExpressionHasher.named_type_hash(stype)
        name_hash = ExpressionHasher.named_type_hash(name)
        id_hash = ExpressionHasher.terminal_hash(stype, name)
        self.mongo_terminal.append({
            "_id": id_hash,
            "composite_type_hash": stype_hash,
            "name": name,
            "named_type": stype,
        })

    def _flush_mongo_expressions(self):
        logger().info(f"Populating MongoDB link tables")
        self._populate_mongo_links()
        self.mongo_expression = []

    def _add_expression(self, expression, composite_type, toplevel, named_type, composite_type_hash):
        named_type_hash = ExpressionHasher.named_type_hash(named_type)
        id_hash = ExpressionHasher.expression_hash(named_type_hash, expression[1:])
        document = {
            "_id": id_hash,
            "composite_type_hash": composite_type_hash,
            "is_toplevel": toplevel,
            "composite_type": composite_type,
            "named_type": named_type,
            "named_type_hash": named_type_hash,
        }
        for i in range(1, len(expression)):
            document[f"key_{i - 1}"] = expression[i]
        self.mongo_expression.append(document)
        if len(self.mongo_expression) >= EXPRESSIONS_CHUNK_SIZE:
            logger().info(f"Expression chunk size reached.")
            self._flush_mongo_expressions()

    def _mongo_insert_many(self, collection, bulk_insertion_raw):
        bulk_insertion = [d for d in bulk_insertion_raw if ("name" not in d) or sys.getsizeof(d["name"]) < 16000000]
        if len(bulk_insertion_raw) != len(bulk_insertion):
            logger().error(f"Striped {len(bulk_insertion_raw) - len(bulk_insertion)} too large documents")
        try:
            collection.insert_many(bulk_insertion, ordered=False)
        except Exception as e:
            if not self.allow_duplicates:
                logger().error(str(e))

    def _flush_terminals(self):
        logger().info(f"Flushing terminals")
        if self.mongo_typedef:
            mongo_collection = self.db.mongo_db[MongoCollections.ATOM_TYPES]
            self._mongo_insert_many(mongo_collection, self.mongo_typedef)
        if self.mongo_terminal:
            mongo_collection = self.db.mongo_db[MongoCollections.NODES]
            self._mongo_insert_many(mongo_collection, self.mongo_terminal)
        with open(self.temporary_file_name[KeyPrefix.NAMED_ENTITIES], "w") as named_entities:
            for document in self.mongo_terminal:
                write_key_value(named_entities, document["_id"], document["name"])
        self.mongo_typedef = []
        self.mongo_terminal = []
        logger().info(f"Terminals flushed")

    def _sort_files(self):
        sort_file(self.temporary_file_name[KeyPrefix.OUTGOING_SET])
        sort_file(self.temporary_file_name[KeyPrefix.INCOMING_SET])
        sort_file(self.temporary_file_name[KeyPrefix.PATTERNS])
        sort_file(self.temporary_file_name[KeyPrefix.TEMPLATES])

    def _build_key_value_files(self):
        outgoing = open(self.temporary_file_name[KeyPrefix.OUTGOING_SET], "w")
        incoming = open(self.temporary_file_name[KeyPrefix.INCOMING_SET], "w")
        patterns = open(self.temporary_file_name[KeyPrefix.PATTERNS], "w")
        template = open(self.temporary_file_name[KeyPrefix.TEMPLATES], "w")
        for tag in [MongoCollections.LINKS_ARITY_1, MongoCollections.LINKS_ARITY_2, MongoCollections.LINKS_ARITY_N]:
            mongo_collection = self.db.mongo_db[tag]
            for expression in mongo_collection.find():
                elements = [expression[k] for k in expression.keys() if k.startswith("key")]
                for target in elements:
                    write_key_value(outgoing, expression["_id"], target)
                    write_key_value(incoming, target, expression["_id"])
                if expression["named_type"] not in self.pattern_black_list:
                    arity = len(elements)
                    type_hash = expression["named_type_hash"]
                    keys = []
                    keys.append([WILDCARD, *elements])
                    if arity == 1:
                        keys.append([type_hash, WILDCARD])
                        keys.append([WILDCARD, elements[0]])
                        keys.append([WILDCARD, WILDCARD])
                    elif arity == 2:
                        keys.append([type_hash, elements[0], WILDCARD])
                        keys.append([type_hash, WILDCARD, elements[1]])
                        keys.append([type_hash, WILDCARD, WILDCARD])
                        keys.append([WILDCARD, elements[0], elements[1]])
                        keys.append([WILDCARD, elements[0], WILDCARD])
                        keys.append([WILDCARD, WILDCARD, elements[1]])
                        keys.append([WILDCARD, WILDCARD, WILDCARD])
                    elif arity == 3:
                        keys.append([type_hash, elements[0], elements[1], WILDCARD])
                        keys.append([type_hash, elements[0], WILDCARD, elements[2]])
                        keys.append([type_hash, WILDCARD, elements[1], elements[2]])
                        keys.append([type_hash, elements[0], WILDCARD, WILDCARD])
                        keys.append([type_hash, WILDCARD, elements[1], WILDCARD])
                        keys.append([type_hash, WILDCARD, WILDCARD, elements[2]])
                        keys.append([type_hash, WILDCARD, WILDCARD, WILDCARD])
                        keys.append([WILDCARD, elements[0], elements[1], elements[2]])
                        keys.append([WILDCARD, elements[0], elements[1], WILDCARD])
                        keys.append([WILDCARD, elements[0], WILDCARD, elements[2]])
                        keys.append([WILDCARD, WILDCARD, elements[1], elements[2]])
                        keys.append([WILDCARD, elements[0], WILDCARD, WILDCARD])
                        keys.append([WILDCARD, WILDCARD, elements[1], WILDCARD])
                        keys.append([WILDCARD, WILDCARD, WILDCARD, elements[2]])
                        keys.append([WILDCARD, WILDCARD, WILDCARD, WILDCARD])
                for key in keys:
                    write_key_value(patterns, key, [expression["_id"], *elements])
                write_key_value(template, expression["composite_type_hash"], [expression["_id"], *elements])
                write_key_value(template, expression["named_type_hash"], [expression["_id"], *elements])
        for file in [outgoing, incoming, patterns, template]:
            file.close()
        self._sort_files()

    def _populate_mongo_links(self):
        bulk_insertion_1 = []
        bulk_insertion_2 = []
        bulk_insertion_N = []
        for expression in self.mongo_expression:
            elements = [expression[k] for k in expression.keys() if k.startswith("key")]
            arity = len(elements)
            if arity == 1:
                bulk_insertion_1.append(expression)
            elif arity == 2:
                bulk_insertion_2.append(expression)
            else:
                bulk_insertion_N.append(expression)
        if bulk_insertion_1:
            mongo_collection = self.db.mongo_db[MongoCollections.LINKS_ARITY_1]
            self._mongo_insert_many(mongo_collection, bulk_insertion_1)
        if bulk_insertion_2:
            mongo_collection = self.db.mongo_db[MongoCollections.LINKS_ARITY_2]
            self._mongo_insert_many(mongo_collection, bulk_insertion_2)
        if bulk_insertion_N:
            mongo_collection = self.db.mongo_db[MongoCollections.LINKS_ARITY_N]
            self._mongo_insert_many(mongo_collection, bulk_insertion_N)

    def _populate_redis_collection(self, collection_name, use_targets, merge_rest, update):
        logger().info(f"Populating collection {collection_name}")
        file_name = self.temporary_file_name[collection_name]
        generator = key_value_targets_generator if use_targets else key_value_generator
        key_count = 0
        for key, value, block_count in generator(file_name, block_size=100000, merge_rest=merge_rest):
            key_count += 1
            if key_count % 100000 == 0:
                logger().info(f"Added {key_count} keys (line count = {das.key_value_file.KEY_VALUE_LINE_COUNTER})")
            try:
                if use_targets:
                    self.db.redis.sadd(build_redis_key(collection_name, key), *[pickle.dumps(v) for v in value])
                else:
                    self.db.redis.sadd(build_redis_key(collection_name, key), *value)
            except:
                logger().error(f"Error in key-value file {collection_name} on line {das.key_value_file.KEY_VALUE_LINE_COUNTER}")
                assert False

    def _populate_redis(self):
        self._populate_redis_collection(KeyPrefix.OUTGOING_SET, False, False, False),
        self._populate_redis_collection(KeyPrefix.INCOMING_SET, False, False, False),
        self._populate_redis_collection(KeyPrefix.PATTERNS, True, False, False),
        self._populate_redis_collection(KeyPrefix.TEMPLATES, True, False, False),
        self._populate_redis_collection(KeyPrefix.NAMED_ENTITIES, False, True, False)

    def _process_key_value_files(self):
        logger().info(f"Populating Redis")
        if not SKIP_KEY_VALUE_FILES_GENERATION:
            logger().info(f"Building key-value files")
            self._build_key_value_files()
        logger().info(f"Processing key-value files")
        self._populate_redis()
        logger().info(f"Redis is up to date")

    def _parse_expression(self, expression):
        slist = []
        stack = []
        composite_type_stack = []
        composite_type_hash_stack = []
        named_type_stack = []
        state = 0
        for c in expression:
            if state == 0:
                if c == '(':
                   stack.append("(")
                elif c == ' ':
                    if slist:
                        s = "".join(slist)
                        stack.append(s)
                        named_type_stack.append(s)
                        named_type_hash = ExpressionHasher.named_type_hash(s)
                        composite_type_stack.append(named_type_hash)
                        composite_type_hash_stack.append(named_type_hash)
                        slist = []
                elif c == ')':
                    expression = []
                    composite_type = []
                    term = stack.pop(-1)
                    hash_list = []
                    while term != "(":
                        expression.append(term)
                        composite_type.append(composite_type_stack.pop())
                        hash_list.append(composite_type_hash_stack.pop())
                        named_type = named_type_stack.pop()
                        term = stack.pop(-1)
                    expression.reverse()
                    composite_type.reverse()
                    hash_list.reverse()
                    composite_type_hash = ExpressionHasher.composite_hash(hash_list)
                    if stack:
                        self._add_expression(expression, composite_type, False, named_type, composite_type_hash)
                        id_hash = ExpressionHasher.expression_hash(ExpressionHasher.named_type_hash(named_type), expression[1:])
                        stack.append(id_hash)
                        named_type_stack.append(":")
                        composite_type_stack.append(composite_type)
                        composite_type_hash_stack.append(composite_type_hash)
                    else:
                        self._add_expression(expression, composite_type, True, named_type, composite_type_hash)
                elif c == '"':
                    state = 1
                else:
                    slist.append(c)
            elif state == 1:
                if c == '"' and previous != '\\':
                    s = "".join(slist).split()
                    stype = s[0]
                    name = " ".join(s[1:])
                    stack.append(ExpressionHasher.terminal_hash(stype, name))
                    named_type_stack.append(stype)
                    named_type_hash = ExpressionHasher.named_type_hash(stype)
                    composite_type_stack.append(named_type_hash)
                    composite_type_hash_stack.append(named_type_hash)
                    slist = []
                    state = 0
                else:
                    slist.append(c)
            previous = c
        self._check(len(stack) == 0)
            
    def _check(self, flag):
        if not flag:
            print(f"({self.current_state.name}) Line #{self.current_line_count}: {self.current_line}")
            assert False
        
    def populate_indexes(self):
        self._process_key_value_files()

    def parse(self, path):
        logger().info(f"Parsing {path}")
        if SKIP_PARSER:
            logger().info(f"Skipping parser")
            return
        logger().info(f"Computing file size")
        self.current_line_count = 1
        HINT_FILE_SIZE = _file_line_count(path)
        logger().info(f"Parsing types")
        self.current_state = State.READING_TYPES
        if HINT_FILE_SIZE is not None:
            progress_count = 1
            progress_bound = HINT_FILE_SIZE // 100
        with open(path, "r") as file:
            for line in file:
                if HINT_FILE_SIZE is not None:
                    if progress_count >= progress_bound:
                        percent = ("{0:.0f}").format(100 * (self.current_line_count / float(HINT_FILE_SIZE)))
                        logger().info(f"Parsed {self.current_line_count}/{HINT_FILE_SIZE} ({percent}%)")
                        progress_count = 1
                    else:
                        progress_count += 1
                self.current_line = line.strip()
                self.current_line_count += 1
                expression = self.current_line.split()
                if self.current_state == State.READING_TYPES:
                    self._check(expression[0] == "(:")
                    if expression[1].startswith("\""):
                        self.current_state = State.READING_TERMINALS
                        logger().info(f"Parsing terminals")
                    else:
                        self._check(len(expression) == 3)
                        type_name = expression[1]
                        stype = expression[-1].rstrip(")")
                        self._add_typedef(type_name, stype)
                if self.current_state == State.READING_TERMINALS:
                    if expression[0] == "(:":
                        terminal_name = " ".join(expression[1:-1]).strip("\"")
                        stype = expression[-1].rstrip(")")
                        self._add_terminal(terminal_name, stype)
                    else:
                        self.current_state = State.READING_EXPRESSIONS
                        self._flush_terminals()
                        logger().info(f"Parsing expressions")
                if self.current_state == State.READING_EXPRESSIONS:
                    self._check(expression[0] != "(:")
                    self._check(self.current_line.startswith("("))
                    self._check(self.current_line.endswith(")"))
                    self._parse_expression(self.current_line)
        logger().info(f"Finished parsing file.")
        self._flush_mongo_expressions()
