from simple_ddl_parser import parse_from_file, DDLParser
from pathlib import Path
import os, shutil
import subprocess
from enum import Enum, auto
from precomputed_tables import PrecomputedTables
import sqlparse

#SQL_LINES_PER_CHUNK = 3000000000
#SQL_LINES_PER_CHUNK = 3000000
#EXPRESSIONS_PER_CHUNK = 70000000
EXPRESSIONS_PER_CHUNK = 150000000
SQL_FILE = "/opt/das/data/flybase/2023_02/FB2023_02.sql"
#SQL_FILE = "/mnt/HD10T/nfs_share/work/datasets/flybase/auto_download/2023_02/FB2023_02.sql"
#SQL_FILE = "/mnt/HD10T/nfs_share/work/datasets/flybase/FB2022_05.sql"
#SQL_FILE = "/tmp/cut.sql"
#SQL_FILE = "/tmp/hedra/genes.sql"
#PRECOMPUTED_DIR = None
PRECOMPUTED_DIR = "/opt/das/data/flybase/2023_02/precomputed"
#PRECOMPUTED_DIR = "/mnt/HD10T/nfs_share/work/datasets/flybase/auto_download/2023_02/precomputed"
#PRECOMPUTED_DIR = "/mnt/HD10T/nfs_share/work/datasets/flybase/precomputed/FB2022_05"
#PRECOMPUTED_DIR = "/tmp/tsv"
OUTPUT_DIR = "/opt/das/data/flybase_metta"
#OUTPUT_DIR = "/mnt/HD10T/nfs_share/work/datasets/flybase_metta"
#OUTPUT_DIR = "/tmp/flybase"
#OUTPUT_DIR = "/tmp/cut"
#OUTPUT_DIR = "/tmp/hedra"
SHOW_PROGRESS = True
FILE_SIZE = 0

SCHEMA_ONLY = False
SKIP_PRECOMPUTED_MATCH_BUILD = True
USE_PRECOMPUTED_NEAR_MATCHES = False
PRINT_PRECOMPUTED_NEAR_MATCHES = False
SKIP_SQL_JOIN = False
SKIP_FKEY_FOLLOWING = SKIP_SQL_JOIN or False

def _file_line_count(file_name):
    output = subprocess.run(["wc", "-l", file_name], stdout=subprocess.PIPE)
    return int(output.stdout.split()[0])

if SHOW_PROGRESS:
    print("Checking SQL file size...")
    FILE_SIZE = _file_line_count(SQL_FILE)

class AtomTypes(str, Enum):
    CONCEPT = "Concept"
    SCHEMA = "Schema"
    NUMBER = "Number"
    VERBATIM = "Verbatim"
    INHERITANCE = "Inheritance"
    EXECUTION = "Execution"
    LIST = "List"

TYPED_NAME = [AtomTypes.CONCEPT, AtomTypes.SCHEMA]

CREATE_TABLE_PREFIX = "CREATE TABLE "
CREATE_TABLE_SUFFIX = ");"
ADD_CONSTRAINT_PREFIX = "ADD CONSTRAINT "
PRIMARY_KEY = " PRIMARY KEY "
FOREIGN_KEY = " FOREIGN KEY "
COPY_PREFIX = "COPY "
COPY_SUFFIX = "\."

class State(int, Enum):
    WAIT_KNOWN_COMMAND = auto()
    READING_CREATE_TABLE = auto()
    READING_COPY = auto()

def non_mapped_column(column):
    return column.startswith("time") or "timestamp" in column

def filter_field(line):
    return  \
        "timestamp" in line or \
        "CONSTRAINT" in line

def _compose_name(str_list):
    return "_".join(str_list)

def short_name(long_table_name):
    return long_table_name.split(".")[1] if long_table_name is not None else None

class LazyParser():

    def __init__(self, sql_file_name, precomputed = None):
        self.sql_file_name = sql_file_name
        self.table_schema = {}
        self.current_table = None
        self.current_table_header = None
        self.current_output_file_number = 1
        base_name = sql_file_name.split("/")[-1].split(".")[0]
        self.target_dir = f"/{OUTPUT_DIR}/{base_name}"
        self.current_output_file = None
        self.error_file_name = f"{OUTPUT_DIR}/{base_name}_errors.txt"
        self.error_file = None
        self.schema_file_name = f"{OUTPUT_DIR}/{base_name}_schema.txt"
        self.precomputed_mapping_file_name = f"{OUTPUT_DIR}/{base_name}_precomputed_tables_mapping.txt"
        self.errors = False
        self.current_node_set = set()
        self.current_typedef_set = set()
        self.current_link_list = []
        self.all_types = set()
        self.current_field_types = {}
        self.discarded_tables = []
        self.line_count = None
        self.precomputed = precomputed
        self.relevant_tables = None
        self.expression_chunk_count = 0
        self.all_precomputed_nodes = set()
        self.all_precomputed_node_names = set()
        self.log_precomputed_nodes = None
        self.relevant_fkeys = {}

        Path(self.target_dir).mkdir(parents=True, exist_ok=True)
        for filename in os.listdir(self.target_dir):
            file_path = os.path.join(self.target_dir, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(e)

    def _print_progress_bar(self, iteration, total, length, step, max_step):
        filled_length = int(length * iteration // total)
        previous = int(length * (iteration - 1) // total)
        if iteration == 1 or filled_length > previous or iteration >= total:
            percent = ("{0:.0f}").format(100 * (iteration / float(total)))
            fill='█'
            bar = fill * filled_length + '-' * (length - filled_length)
            print(f'\r STEP {step}/{max_step} Progress: |{bar}| {percent}% complete ({iteration}/{total})', end = '\r')
            if iteration >= total: 
                print()

    def _table_info(self, table_name):
        answer = [table_name]
        table = self.table_schema[table_name]
        for column in table['columns']:
            prefix = "  "
            suffix = ""
            if column['name'] == table['primary_key']:
                prefix = "PK"
            elif column['name'] in table['foreign_keys']:
                prefix = "FK"
                referenced_table, referenced_field = table['foreign_key'][column['name']]
                suffix = f"-> {referenced_table} {referenced_field}"
            answer.append(f"    {prefix} {column['type']} {column['name']} {suffix}")
        return "\n".join(answer)

    def _error(self, message):
        self.error_file.write(message)
        self.error_file.write("\n")
        self.errors = True

    def _emit_file_header(self):
        #metta
        for t in AtomTypes:
            self.current_output_file.write(f"(: {t.value} Type)\n")

    def _open_new_output_file(self):
        if self.current_output_file_number > 1:
            self.current_output_file.close()
        fname = f"{self.target_dir}/file_{str(self.current_output_file_number).zfill(3)}.metta"
        self.current_output_file_number += 1
        self.current_output_file = open(fname, "w")
        self._emit_file_header()

    def _emit_precomputed_tables(self, output_file):
        self.log_precomputed_nodes = True
        table_count = 0
        for table in self.precomputed.all_tables:
            self._print_progress_bar(table_count, len(self.precomputed.all_tables), 50, 3, 5)
            #print(table)
            for row in table.rows:
                #print(row)
                for key1, value1 in zip(table.header, row):
                    if key1 not in table.mapped_fields:
                        #print(f"key1: {key1} not in table.mapped_fields")
                        continue
                    sql_table1, sql_field1 = table.mapping[key1]
                    node1 = self._add_value_node(short_name(sql_table1), self._get_type(sql_table1, sql_field1), value1)
                    #print("1:", node1)
                    for key2, value2 in zip(table.header, row):
                        if key2 != key1:
                            sql_table2, sql_field2 = table.mapping[key2] if key2 in table.mapping else (None, None)
                            node2 = self._add_value_node(short_name(sql_table2), self._get_type(sql_table2, sql_field2), value2)
                            #print("2:", node2)
                            schema = self._add_node(AtomTypes.SCHEMA, _compose_name(["preref", table.name, key2]))
                            self._add_execution(schema, node1, node2)
                for key1, value1 in zip(table.header, row):
                    for key2, value2 in zip(table.header, row):
                        if key2 != key1:
                            schema = self._add_node(AtomTypes.SCHEMA, _compose_name(["pre", table.name, key2]))
                            node1 = self._add_node(AtomTypes.VERBATIM, value1)
                            node2 = self._add_node(AtomTypes.VERBATIM, value2)
                            self._add_execution(schema, node1, node2)
            table_count += 1
            self._print_progress_bar(table_count, len(self.precomputed.all_tables), 50, 3, 5)
        self.log_precomputed_nodes = False

    def _checkpoint(self, create_new, use_precomputed_filter=False):
        if SCHEMA_ONLY:
            return
        for metta_string in self.current_typedef_set:
            self.current_output_file.write(metta_string)
            self.current_output_file.write("\n")
        for metta_string in self.current_node_set:
            if not use_precomputed_filter or metta_string in self.all_precomputed_nodes:
                self.current_output_file.write(metta_string)
                self.current_output_file.write("\n")
        for metta_string in self.current_link_list:
            self.current_output_file.write(metta_string)
            self.current_output_file.write("\n")
        self.current_node_set = set()
        self.current_typedef_set = set()
        self.current_link_list = []
        self.expression_chunk_count = 0
        if create_new:
            self._open_new_output_file()

    def _setup(self):
        self._open_new_output_file()
        self.error_file = open(self.error_file_name, "w")

    def _tear_down(self):
        self.current_output_file.close()
        self.error_file.close()

    def _create_table(self, text):
        parsed = DDLParser(text).run()
        full_name = f"{parsed[0]['schema']}.{parsed[0]['table_name']}"
        self.table_schema[full_name] = parsed[0]
        assert len(parsed[0]['primary_key']) <= 1
        parsed[0]['primary_key'] = None
        parsed[0]['foreign_key'] = {}
        parsed[0]['foreign_keys'] = []
        parsed[0]['fields'] = [column['name'] for column in parsed[0]['columns']]
        parsed[0]['types'] = [column['type'] for column in parsed[0]['columns']]
        for column in parsed[0]['columns']:
            self.all_types.add(f"{column['type']} {column['size']}")

    def _start_copy(self, line):
        self.current_table = line.split(" ")[1]
        if SCHEMA_ONLY or self.current_table in self.discarded_tables or\
           (self.relevant_tables is not None and self.current_table not in self.relevant_tables):
            return False
        columns = line.split("(")[1].split(")")[0].split(",")
        columns = [s.strip() for s in columns]
        schema_columns = [column['name'] for column in self.table_schema[self.current_table]['columns']]
        assert all(column in schema_columns or non_mapped_column(column) for column in columns)
        self.current_table_header = columns
        self.current_field_types = {}
        table = self.table_schema[self.current_table]
        for name, ctype in zip(table['fields'], table['types']):
            self.current_field_types[name] = ctype
        return True

    def _get_type(self, table_name, field):
        if table_name is not None:
            table = self.table_schema[table_name]
            for name, ctype in zip(table['fields'], table['types']):
                if name == field:
                    if name == table['primary_key']:
                        return "pk"
                    else:
                        return ctype
        return "text"

    def _build_node(self, node_type, node_name):
        node_name = node_name.replace("(", "[")
        node_name = node_name.replace(")", "]")
        node_name = node_name.replace('"', "")
        if node_type in TYPED_NAME:
            quoted_node_name = f'"{node_type}:{node_name}"'
            quoted_canonical_node_name = f'"{node_type} {node_type}:{node_name}"'
        else:
            quoted_node_name = f'"{node_name}"'
            quoted_canonical_node_name = f'"{node_type} {node_name}"'
        return tuple([quoted_canonical_node_name, f"(: {quoted_node_name} {node_type})"])

    def _add_node_to_internal_sets(self, quoted_canonical_node_name, node):
        self.current_node_set.add(node)
        if self.log_precomputed_nodes:
            self.all_precomputed_nodes.add(node)
            self.all_precomputed_node_names.add(quoted_canonical_node_name)
        node_type = quoted_canonical_node_name.strip('"').split()[0]
        self.current_typedef_set.add(f"(: {node_type} Type)")
        self.expression_chunk_count += 1

    def _add_node(self, node_type, node_name):
        # metta
        #print(f"add_node {node_type} {node_name}")
        quoted_canonical_node_name, node = self._build_node(node_type, node_name)
        self._add_node_to_internal_sets(quoted_canonical_node_name, node)
        return quoted_canonical_node_name

    def _add_inheritance(self, node1, node2):
        # metta
        #print(f"add_inheritance {node1} {node2}")
        if node1 and node2:
            self.current_link_list.append(f"({AtomTypes.INHERITANCE} {node1} {node2})")
        self.expression_chunk_count += 1

    #def _add_evaluation(self, predicate, node1, node2):
    #    # metta
    #    #print(f"add_evaluation {predicate} {node1} {node2}")
    #    if predicate and node1 and node2:
    #        self.current_link_list.append(f"({AtomTypes.EVALUATION} {predicate} ({AtomTypes.LIST} {node1} {node2}))")
    #    self.expression_chunk_count += 1

    def _add_execution(self, schema, node1, node2):
        # metta
        #print(f"add_execution {schema} {node1} {node2}")
        if schema and node1 and node2:
            self.current_link_list.append(f"({AtomTypes.EXECUTION} {schema} {node1} {node2})")
        self.expression_chunk_count += 1

    def _add_value_node(self, table_short_name, field_type, value, build_only=False):
        if value == "\\N":
            return None
        if field_type == "pk":
            assert table_short_name is not None
            if build_only:
                return self._build_node(table_short_name, value)
            else:
                return self._add_node(table_short_name, value)
        elif field_type == "boolean":
            if build_only:
                return self._build_node(AtomTypes.CONCEPT, "True" if value.lower() == "t" else "False")
            else:
                return self._add_node(AtomTypes.CONCEPT, "True" if value.lower() == "t" else "False")
        elif field_type in ["bigint", "integer", "smallint", "double precision"]:
            if build_only:
                return self._build_node(AtomTypes.NUMBER, value)
            else:
                return self._add_node(AtomTypes.NUMBER, value)
        elif "character" in field_type or field_type in ["date", "text"]:
            if build_only:
                return self._build_node(AtomTypes.VERBATIM, value)
            else:
                return self._add_node(AtomTypes.VERBATIM, value)
        elif field_type in ["jsonb"]:
            return None
        else:
            assert False

    def _new_row_precomputed(self, line):
        if SCHEMA_ONLY:
            return
        table = self.table_schema[self.current_table]
        fkeys = table['foreign_keys']
        data = line.split("\t")
        if len(self.current_table_header) != len(data):
            self._error(f"Invalid row at line {self.line_count} Table: {self.current_table} Header: {self.current_table_header} Raw line: <{line}>")
            return
        for name, value in zip(self.current_table_header, data):
            if (not non_mapped_column(name)) and (name not in fkeys):
                self.precomputed.check_field_value(self.current_table, name, value)

    def _new_row_relevant_fkeys(self, line):
        if SCHEMA_ONLY or (self.current_table not in self.relevant_fkeys):
            return
        table = self.table_schema[self.current_table]
        table_short_name = short_name(self.current_table)
        pkey = table['primary_key']
        fkeys = table['foreign_keys']
        assert pkey,f"self.current_table = {self.current_table} pkey = {pkey} \n{table}"
        data = line.split("\t")
        if len(self.current_table_header) != len(data):
            self._error(f"Invalid row at line {self.line_count} Table: {self.current_table} Header: {self.current_table_header} Raw line: <{line}>")
            return
        pkey_node = None
        for name, value in zip(self.current_table_header, data):
            if name == pkey:
                if value not in self.relevant_fkeys[self.current_table]:
                    return
                pkey_node = self._add_node(table_short_name, value)
                break
        assert pkey_node is not None
        for name, value in zip(self.current_table_header, data):
            if non_mapped_column(name):
                continue
            if name in fkeys:
                referenced_table, referenced_field = table['foreign_key'][name]
                schema_node = self._add_node(AtomTypes.SCHEMA, referenced_table)
                fkey_node = self._add_node(AtomTypes.CONCEPT, _compose_name(["fwfk", referenced_table, value]))
                self._add_execution(schema_node, pkey_node, fkey_node)
            elif name != pkey:
                ftype = self.current_field_types.get(name, None)
                if not ftype:
                    continue
                value_node = self._add_value_node(table_short_name, ftype, value)
                if not value_node:
                    continue
                schema_node = self._add_node(AtomTypes.SCHEMA, _compose_name(["fw", table_short_name, name]))
                self._add_execution(schema_node, pkey_node, value_node)

    def _new_row(self, line):
        if SCHEMA_ONLY or (self.relevant_tables is not None and self.current_table not in self.relevant_tables):
            return
        table = self.table_schema[self.current_table]
        table_short_name = short_name(self.current_table)
        pkey = table['primary_key']
        fkeys = table['foreign_keys']
        assert pkey,f"self.current_table = {self.current_table} pkey = {pkey} \n{table}"
        data = line.split("\t")
        if len(self.current_table_header) != len(data):
            self._error(f"Invalid row at line {self.line_count} Table: {self.current_table} Header: {self.current_table_header} Raw line: <{line}>")
            return
        relevant_row = False
        for name, value in zip(self.current_table_header, data):
            for precomputed_table in self.precomputed.all_tables:
                for key in precomputed_table.mapping:
                    sql_table, sql_field = precomputed_table.mapping[key]
                    if sql_table == self.current_table and sql_field == name and value in precomputed_table.values[key]:
                        relevant_row = True
                        break
                if relevant_row:
                    break
            if relevant_row:
                break
        if not relevant_row:
            return
        pkey_node = None
        for name, value in zip(self.current_table_header, data):
            if name == pkey:
                pkey_node = self._add_node(table_short_name, value)
                break
        assert pkey_node is not None
        for name, value in zip(self.current_table_header, data):
            if non_mapped_column(name):
                continue
            if name in fkeys:
                referenced_table, referenced_field = table['foreign_key'][name]
                if not SKIP_FKEY_FOLLOWING:
                    if referenced_table not in self.relevant_fkeys:
                        self.relevant_fkeys[referenced_table] = set()
                    self.relevant_fkeys[referenced_table].add(value)
                schema_node = self._add_node(AtomTypes.SCHEMA, _compose_name(["sqlfk", referenced_table]))
                fkey_node = self._add_node(AtomTypes.CONCEPT, _compose_name([referenced_table, value]))
                self._add_execution(schema_node, pkey_node, fkey_node)
            elif name != pkey:
                ftype = self.current_field_types.get(name, None)
                if not ftype:
                    continue
                value_node = self._add_value_node(table_short_name, ftype, value)
                if not value_node:
                    continue
                schema_node = self._add_node(AtomTypes.SCHEMA, _compose_name(["sql", table_short_name, name]))
                self._add_execution(schema_node, pkey_node, value_node)

    def _primary_key(self, first_line, second_line):
        line = first_line.split()
        table = line[2] if line[2] != "ONLY" else line[3]
        line = second_line.split()
        field = line[-1][1:-2]
        assert not self.table_schema[table]['primary_key']
        assert field in self.table_schema[table]['fields']
        self.table_schema[table]['primary_key'] = field
        if self.precomputed:
            self.precomputed.set_sql_primary_key(table, field)

    def _foreign_key(self, first_line, second_line):
        line = first_line.split()
        table = line[2] if line[2] != "ONLY" else line[3]
        line = second_line.split()
        field = line[5][1:-1]
        reference = line[7].split("(")
        referenced_table = reference[0]
        referenced_field = reference[1].split(")")[0]
        assert field in self.table_schema[table]['fields']
        assert referenced_field in self.table_schema[referenced_table]['fields']
        self.table_schema[table]['foreign_key'][field] = tuple([referenced_table, referenced_field])
        self.table_schema[table]['foreign_keys'].append(field)

    def _parse_step_1(self):

        text = ""
        self.line_count = 0
        file_size = FILE_SIZE

        state = State.WAIT_KNOWN_COMMAND
        with open(self.sql_file_name, 'r') as file:
            line = file.readline()
            previous_line = None
            while line:
                self.line_count += 1
                if SHOW_PROGRESS:
                    self._print_progress_bar(self.line_count, file_size, 50, 1, 5)
                line = line.replace('\n', '').strip()
                if state == State.WAIT_KNOWN_COMMAND:
                    if line.startswith(CREATE_TABLE_PREFIX):
                        text = line
                        state = State.READING_CREATE_TABLE
                    elif line.startswith(ADD_CONSTRAINT_PREFIX) and PRIMARY_KEY in line:
                        self._primary_key(previous_line, line)
                    elif line.startswith(ADD_CONSTRAINT_PREFIX) and FOREIGN_KEY in line:
                        self._foreign_key(previous_line, line)
                elif state == State.READING_CREATE_TABLE:
                    if not filter_field(line):
                        text = f"{text}\n{line}"
                    if line.startswith(CREATE_TABLE_SUFFIX):
                        self._create_table(text)
                        state = State.WAIT_KNOWN_COMMAND
                        text = ""
                else:
                    print(f"Invalid state {state}")
                    assert False
                previous_line = line
                line = file.readline()

    def _parse_step_2(self):

        text = ""
        self.line_count = 0
        file_size = FILE_SIZE

        for key,table in self.table_schema.items():
            if not table['primary_key']:
                self.discarded_tables.append(key)
                self._error(f"Discarded table {key}. No PRIMARY KEY defined.")
                
        state = State.WAIT_KNOWN_COMMAND
        with open(self.sql_file_name, 'r') as file:
            line = file.readline()
            while line:
                self.line_count += 1
                if SHOW_PROGRESS:
                    self._print_progress_bar(self.line_count, file_size, 50, 2, 5)
                if not self.precomputed.all_tables_mapped():
                    line = line.replace('\n', '').strip()
                    if state == State.WAIT_KNOWN_COMMAND:
                        if line.startswith(COPY_PREFIX):
                            if self._start_copy(line):
                                state = State.READING_COPY
                    elif state == State.READING_COPY:
                        if line.startswith(COPY_SUFFIX):
                            state = State.WAIT_KNOWN_COMMAND
                        elif not SKIP_PRECOMPUTED_MATCH_BUILD:
                            self._new_row_precomputed(line)
                    else:
                        print(f"Invalid state {state}")
                        assert False
                line = file.readline()
            if USE_PRECOMPUTED_NEAR_MATCHES:
                self.precomputed.check_nearly_matched_tables()
            if PRINT_PRECOMPUTED_NEAR_MATCHES:
                self.precomputed.print_matched_tables()
            self._emit_precomputed_tables(self.current_output_file)
            self._checkpoint(True, use_precomputed_filter=True)
        self.relevant_tables = self.precomputed.get_relevant_sql_tables()

    def _parse_step_3(self):

        text = ""
        self.line_count = 0
        file_size = FILE_SIZE

        if not self.precomputed:
            for key,table in self.table_schema.items():
                if not table['primary_key']:
                    self.discarded_tables.append(key)
                    self._error(f"Discarded table {key}. No PRIMARY KEY defined.")
                
        state = State.WAIT_KNOWN_COMMAND
        with open(self.sql_file_name, 'r') as file:
            line = file.readline()
            while line:
                self.line_count += 1
                if self.expression_chunk_count >= EXPRESSIONS_PER_CHUNK:
                    self._checkpoint(True)
                if SHOW_PROGRESS:
                    self._print_progress_bar(self.line_count, file_size, 50, 4, 5)
                line = line.replace('\n', '').strip()
                if state == State.WAIT_KNOWN_COMMAND:
                    if line.startswith(COPY_PREFIX):
                        if self._start_copy(line):
                            state = State.READING_COPY
                elif state == State.READING_COPY:
                    if line.startswith(COPY_SUFFIX):
                        state = State.WAIT_KNOWN_COMMAND
                    else:
                        if not SKIP_SQL_JOIN:
                            self._new_row(line)
                else:
                    print(f"Invalid state {state}")
                    assert False
                line = file.readline()
            self._checkpoint(False)

    def _parse_step_4(self):

        text = ""
        self.line_count = 0
        file_size = FILE_SIZE

        if not self.precomputed:
            for key,table in self.table_schema.items():
                if not table['primary_key']:
                    self.discarded_tables.append(key)
                    self._error(f"Discarded table {key}. No PRIMARY KEY defined.")
                
        state = State.WAIT_KNOWN_COMMAND
        with open(self.sql_file_name, 'r') as file:
            line = file.readline()
            while line:
                self.line_count += 1
                if self.expression_chunk_count >= EXPRESSIONS_PER_CHUNK:
                    self._checkpoint(True)
                if SHOW_PROGRESS:
                    self._print_progress_bar(self.line_count, file_size, 50, 5, 5)
                line = line.replace('\n', '').strip()
                if state == State.WAIT_KNOWN_COMMAND:
                    if line.startswith(COPY_PREFIX):
                        if self._start_copy(line):
                            state = State.READING_COPY
                elif state == State.READING_COPY:
                    if line.startswith(COPY_SUFFIX):
                        state = State.WAIT_KNOWN_COMMAND
                    else:
                        if not SKIP_SQL_JOIN:
                            self._new_row_relevant_fkeys(line)
                else:
                    print(f"Invalid state {state}")
                    assert False
                line = file.readline()
            self._checkpoint(False)

    def parse(self):
        self._setup()
        self._parse_step_1()
        if self.precomputed:
            self._parse_step_2()
            f = open(self.precomputed_mapping_file_name, "w")
            f.write(self.precomputed.mappings_str())
            f.close()
        f = open(self.schema_file_name, "w")
        for table in self.table_schema:
            if self.relevant_tables is None or table in self.relevant_tables:
                f.write(self._table_info(table))
                f.write("\n\n")
        f.close()
        self._parse_step_3()
        if not SKIP_FKEY_FOLLOWING:
            self._parse_step_4()
        if self.errors:
            print(f"Errors occured while processing this SQL file. See them in {self.error_file_name}")
        self._tear_down()

def main():
    precomputed = PrecomputedTables(PRECOMPUTED_DIR) if PRECOMPUTED_DIR else None
    parser = LazyParser(SQL_FILE, precomputed)
    parser.parse()
    if precomputed:
        pass

if __name__ == "__main__":
    main()
