from simple_ddl_parser import parse_from_file, DDLParser
from pathlib import Path
import os, shutil
from enum import Enum, auto
from flybase2metta.precomputed_tables import PrecomputedTables
import sqlparse

#SQL_LINES_PER_CHUNK = 3000000000
SQL_LINES_PER_CHUNK = 3000000
#SQL_FILE = "/mnt/HD10T/nfs_share/work/datasets/flybase/FB2022_05.sql"
#SQL_FILE = "/tmp/cut.sql"
SQL_FILE = "/tmp/hedra/genes.sql"
#PRECOMPUTED_DIR = "/mnt/HD10T/nfs_share/work/datasets/flybase/precomputed/FB2022_05"
PRECOMPUTED_DIR = "/tmp/tsv"
#OUTPUT_DIR = "/mnt/HD10T/nfs_share/work/datasets/flybase_metta"
#OUTPUT_DIR = "/tmp/cut"
OUTPUT_DIR = "/tmp/hedra"
SCHEMA_ONLY = False
SHOW_PROGRESS = True
#SHOW_PROGRESS = False
FILE_SIZE = 1778012

class AtomTypes(str, Enum):
    CONCEPT = "Concept"
    PREDICATE = "Predicate"
    SCHEMA = "Schema"
    NUMBER = "Number"
    VERBATIM = "Verbatim"
    INHERITANCE = "Inheritance"
    EVALUATION = "Evaluation"
    LIST = "List"

TYPED_NAME = [AtomTypes.CONCEPT, AtomTypes.PREDICATE, AtomTypes.SCHEMA]

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

def _compose_name(name1, name2):
    return f"{name1}_{name2}"

class LazyParser():

    def __init__(self, sql_file_name):
        self.sql_file_name = sql_file_name
        self.parse_step = None
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
        self.schema_file = None
        self.errors = False
        self.current_table_node = None
        self.current_node_set = set()
        self.current_link_list = []
        self.all_types = set()
        self.current_field_types = {}
        self.discarded_tables = []
        self.line_count = None

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

    def _print_progress_bar(self, iteration, total, length=50):
        filled_length = int(length * iteration // total)
        previous = int(length * (iteration - 1) // total)
        if iteration == 1 or filled_length > previous or iteration >= total:
            percent = ("{0:.0f}").format(100 * (iteration / float(total)))
            fill='█'
            bar = fill * filled_length + '-' * (length - filled_length)
            print(f'\r STEP {self.parse_step}/2 Progress: |{bar}| {percent}% complete ({iteration}/{total})', end = '\r')
            if iteration >= total: 
                print()

    def _print_table_info(self, table):
        print(table)
        table = self.table_schema[table]
        for column in table['columns']:
            prefix = "  "
            suffix = ""
            if column['name'] == table['primary_key']:
                prefix = "PK"
            elif column['name'] in table['foreign_keys']:
                prefix = "FK"
                referenced_table, referenced_field = table['foreign_key'][column['name']]
                suffix = f"-> {referenced_table} {referenced_field}"
            print(f"    {prefix} {column['type']} {column['name']} {suffix}")

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

    def _checkpoint(self, create_new):
        if SCHEMA_ONLY:
            return
        for metta_string in self.current_node_set:
            self.current_output_file.write(metta_string)
            self.current_output_file.write("\n")
        for metta_string in self.current_link_list:
            self.current_output_file.write(metta_string)
            self.current_output_file.write("\n")
        self.current_node_set = set()
        self.current_link_list = []
        if create_new:
            self._open_new_output_file()

    def _setup(self):
        self._open_new_output_file()
        self.error_file = open(self.error_file_name, "w")
        self.schema_file = open(self.schema_file_name, "w")

    def _tear_down(self):
        self.current_output_file.close()
        self.error_file.close()
        self.schema_file.close()

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
        self.schema_file.write(text)
        self.schema_file.write("\n\n")

    def _start_copy(self, line):
        self.current_table = line.split(" ")[1]
        if SCHEMA_ONLY or self.current_table in self.discarded_tables:
            return False
        columns = line.split("(")[1].split(")")[0].split(",")
        columns = [s.strip() for s in columns]
        schema_columns = [column['name'] for column in self.table_schema[self.current_table]['columns']]
        assert all(column in schema_columns or non_mapped_column(column) for column in columns)
        self.current_table_header = columns
        self.current_table_node = self._add_node(AtomTypes.CONCEPT, self.current_table.split(".")[1])
        #self._add_inheritance(self.current_table_node, self.table_node)
        self.current_field_types = {}
        table = self.table_schema[self.current_table]
        for name, ctype in zip(table['fields'], table['types']):
            self.current_field_types[name] = ctype
        return True

    def _get_type(self, field):
        table = self.table_schema[self.current_table]
        for name, ctype in zip(table['fields'], table['types']):
            if name == field:
                return ctype
        assert False

    def _add_node(self, node_type, node_name):
        # metta
        #print(f"add_node {node_type} {node_name}")
        node_name = node_name.replace("(", "[")
        node_name = node_name.replace(")", "]")
        if node_type in TYPED_NAME:
            quoted_node_name = f'"{node_type}:{node_name}"'
            quoted_canonical_node_name = f'"{node_type} {node_type}:{node_name}"'
        else:
            quoted_node_name = f'"{node_name}"'
            quoted_canonical_node_name = f'"{node_type} {node_name}"'
        self.current_node_set.add(f"(: {quoted_node_name} {node_type})")
        return quoted_canonical_node_name

    def _add_inheritance(self, node1, node2):
        # metta
        #print(f"add_inheritance {node1} {node2}")
        if node1 and node2:
            self.current_link_list.append(f"({AtomTypes.INHERITANCE} {node1} {node2})")

    def _add_evaluation(self, predicate, node1, node2):
        # metta
        #print(f"add_evaluation {predicate} {node1} {node2}")
        if predicate and node1 and node2:
            self.current_link_list.append(f"({AtomTypes.EVALUATION} {predicate} ({AtomTypes.LIST} {node1} {node2}))")

    def _add_execution(self, schema, node1, node2):
        # metta
        #print(f"add_execution {schema} {node1} {node2}")
        if schema and node1 and node2:
            self.current_link_list.append(f"({AtomTypes.SCHEMA} {schema} {node1} {node2})")

    def _add_value_node(self, field_type, value):
        if value == "\\N":
            return None
        if field_type == "boolean":
            return self._add_node(AtomTypes.CONCEPT, "True" if value.lower() == "t" else "False")
        elif field_type in ["bigint", "integer", "smallint", "double precision"]:
            return self._add_node(AtomTypes.NUMBER, value)
        elif "character" in field_type or field_type in ["date", "text"]:
            return self._add_node(AtomTypes.VERBATIM, value)
        elif field_type in ["jsonb"]:
            return None
        else:
            assert False

    def _new_row(self, line):
        if SCHEMA_ONLY:
            return
        table = self.table_schema[self.current_table]
        table_short_name = self.current_table.split(".")[1]
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
                pkey_node = self._add_node(AtomTypes.CONCEPT, _compose_name(table_short_name, value))
                self._add_inheritance(pkey_node, self.current_table_node)
                break
        assert pkey_node is not None
        for name, value in zip(self.current_table_header, data):
            if non_mapped_column(name):
                continue
            if name in fkeys:
                referenced_table, referenced_field = table['foreign_key'][name]
                predicate_node = self._add_node(AtomTypes.PREDICATE, referenced_table)
                fkey_node = self._add_node(AtomTypes.CONCEPT, _compose_name(referenced_table, value))
                self._add_evaluation(predicate_node, pkey_node, fkey_node)
            elif name != pkey:
                ftype = self.current_field_types.get(name, None)
                if not ftype:
                    continue
                value_node = self._add_value_node(ftype, value)
                if not value_node:
                    continue
                schema_node = self._add_node(AtomTypes.SCHEMA, _compose_name(table_short_name, name))
                self._add_execution(schema_node, pkey_node, value_node)

    def _primary_key(self, first_line, second_line):
        line = first_line.split()
        table = line[2] if line[2] != "ONLY" else line[3]
        line = second_line.split()
        field = line[-1][1:-2]
        assert not self.table_schema[table]['primary_key']
        assert field in self.table_schema[table]['fields']
        self.table_schema[table]['primary_key'] = field

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

        CREATE_TABLE_PREFIX = "CREATE TABLE "
        CREATE_TABLE_SUFFIX = ");"
        ADD_CONSTRAINT_PREFIX = "ADD CONSTRAINT "
        PRIMARY_KEY = " PRIMARY KEY "
        FOREIGN_KEY = " FOREIGN KEY "
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
                    self._print_progress_bar(self.line_count, file_size, length=50)
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

        COPY_PREFIX = "COPY "
        COPY_SUFFIX = "\."
        text = ""
        self.line_count = 0
        chunk_count = 0
        file_size = FILE_SIZE

        for key,table in self.table_schema.items():
            if not table['primary_key']:
                self.discarded_tables.append(key)
                self._error(f"Discarded table {key}. No PRIMARY KEY defined.")
                
        state = State.WAIT_KNOWN_COMMAND
        #self.table_node = self._add_node(AtomTypes.CONCEPT, "SQLTable")
        with open(self.sql_file_name, 'r') as file:
            line = file.readline()
            while line:
                self.line_count += 1
                chunk_count += 1
                if chunk_count == SQL_LINES_PER_CHUNK:
                    self._checkpoint(True)
                    chunk_count = 0
                if SHOW_PROGRESS:
                    self._print_progress_bar(self.line_count, file_size, length=50)
                line = line.replace('\n', '').strip()
                if state == State.WAIT_KNOWN_COMMAND:
                    if line.startswith(COPY_PREFIX):
                        if self._start_copy(line):
                            state = State.READING_COPY
                elif state == State.READING_COPY:
                    if line.startswith(COPY_SUFFIX):
                        state = State.WAIT_KNOWN_COMMAND
                    else:
                        self._new_row(line)
                else:
                    print(f"Invalid state {state}")
                    assert False
                line = file.readline()
            self._checkpoint(False)

    def parse(self):
        self._setup()
        self.parse_step = 1
        self._parse_step_1()
        self.parse_step = 2
        self._parse_step_2()
        if self.errors:
            print(f"Errors occured while processing this SQL file. See them in {self.error_file_name}")
        self._tear_down()

def main():
    #schema = parse_from_file(SQL_FILE)
    #print(schema)

    #with open(SQL_FILE, 'r') as file:
    #    sql_string = file.read().replace('\n', '')
    #statements = sqlparse.split(sql_string)
    #print("---------------")
    #for statement in statements:
    #    #print(sqlparse.format(statement, reindent=True, keyword_case='upper'))
    #    print(type(statement))
    #    print(statement)
    #print("---------------")

    #precomputed = PrecomputedTables(PRECOMPUTED_DIR)
    parser = LazyParser(SQL_FILE)
    parser.parse()

    #for t in sorted(parser.all_types):
    #    print(t)

    #parser._print_table_info("gene.gene")
    #print("")
    #parser._print_table_info("gene.allele")

    #for key in parser.table_schema:
    #    print(key)

if __name__ == "__main__":
    main()
