import os
import sys
import time
import traceback
from concurrent import futures
import string
import random
import grpc
from enum import Enum
import tempfile
from threading import Lock, Condition, Thread
sys.path.append(os.path.join(os.path.dirname(__file__), "service_spec"))
import das_pb2 as pb2
import das_pb2_grpc as pb2_grpc
from das.distributed_atom_space import DistributedAtomSpace, QueryOutputFormat
from das.database.db_interface import UNORDERED_LINK_TYPES
from das.pattern_matcher.pattern_matcher import Node, Link, And, Or, Not, Variable

SERVICE_PORT = 7025
COUCHBASE_SETUP_DIR = os.environ['COUCHBASE_SETUP_DIR']

def build_random_string(length):
    return ''.join(random.choice(string.ascii_lowercase) for i in range(length))

class AtomSpaceStatus(str, Enum):
    READY = "Ready"
    LOADING = "Loading knowledge base"

class OutputFormat(str, Enum):
    HANDLE = "HANDLE"
    DICT = "DICT"
    JSON = "JSON"

def _parse_query(query_str: str):
    current_state = 0
    nodes = {}
    stack = []
    for chunk in query_str.split(","):
        chunk = chunk.strip().split()
        head = chunk[0]
        if current_state == 0:
            if head == 'Node':
                if len(chunk) != 4:
                    return None
                nodes[chunk[1]] = Node(chunk[2], chunk[3])
            else:
                current_state = 1
        if current_state == 1:
            if head == 'Link':
                if len(chunk) < 3:
                    return None
                link_type = chunk[1]
                args_str = chunk[2:]
                args = []
                for arg in args_str:
                    if arg.startswith("$"):
                        args.append(Variable(arg))
                    else:
                        node = nodes.get(arg, None)
                        if node is None:
                            return None
                        args.append(node)
                ordered = not link_type in UNORDERED_LINK_TYPES
                stack.append(Link(link_type, args, ordered))
            else:
                if not stack:
                    return None
                if head == 'AND':
                    new_logic_operation = And(stack)
                    stack = [new_logic_operation]
                elif head == 'OR':
                    new_logic_operation = Or(stack)
                    stack = [new_logic_operation]
                elif head == 'NOT':
                    new_logic_operation = Not(stack.pop())
                    stack.append(new_logic_operation)
                else:
                    return None
    if len(stack) != 1:
        return None
    return stack[0]

class KnowledgeBaseLoader(Thread):

    def __init__(self, service: "ServiceDefinition", das_key: str, url: str):
        super().__init__()
        self.service = service
        self.das_key = das_key
        self.url = url

    def run(self):
        #TODO: make this block thread-safe
        temp_dir = tempfile.mkdtemp()
        if self.url.startswith("file:///"):
            self.url = self.url[7:]
            os.system(f"cp -f {self.url} {temp_dir}")
        else:
            os.system(f"wget -P {temp_dir} {self.url}")
        if self.url.endswith(".tgz"):
            os.system(f"tar -xzf {temp_dir}/*.tgz -C {temp_dir}")
        elif self.url.endswith(".tar"):
            os.system(f"unzip {temp_dir}/*.zip -d {temp_dir}")
        das = self.service._get_das(self.das_key)
        das.load_knowledge_base(temp_dir)
        os.system(f"rm -rf {temp_dir}")
        self.service._set_das_status(self.das_key, AtomSpaceStatus.READY)

# SERVICE_API
class ServiceDefinition(pb2_grpc.ServiceDefinitionServicer):
    
    def __init__(self):
        self.atom_spaces = {}
        self.atom_space_status = {}
        self.lock = Lock()
        self.locked_scope = Condition(self.lock)
        self.query_output_map = {
            OutputFormat.HANDLE: QueryOutputFormat.HANDLE,
            OutputFormat.DICT: QueryOutputFormat.ATOM_INFO,
            OutputFormat.JSON: QueryOutputFormat.JSON
        }

    def _get_das(self, key: str):
        das = self.atom_spaces[key]
        return das

    def _set_das_status(self, key: str, status: str):
        self.atom_space_status[key] = status

    def _error(self, error_message: str):
        return pb2.Status(success=False, msg=error_message)
        
    def _success(self, message=AtomSpaceStatus.READY):
        return pb2.Status(success=True, msg=message)
        
    def create(self, request, context):
        with self.locked_scope:
            name = request.name
            if any(das.database_name == name for das in self.atom_spaces.values()):
                return self._error(f"DAS named '{name}' already exists")
            while True:
                token = build_random_string(20)
                if token not in self.atom_spaces:
                    break
            #TODO Remove hardwired folder reference
            os.system(f"touch {COUCHBASE_SETUP_DIR}/new_das/{name}.das")
            time.sleep(5)
            das = DistributedAtomSpace(database_name=name)
            self.atom_spaces[token] = das
            self.atom_space_status[token] = AtomSpaceStatus.READY
            return self._success(token)
        
    def reconnect(self, request, context):
        with self.locked_scope:
            name = request.name
            if any(das.database_name == name for das in self.atom_spaces.values()):
                return self._error(f"DAS named '{name}' already exists")
            while True:
                token = build_random_string(20)
                if token not in self.atom_spaces:
                    break
            das = DistributedAtomSpace(database_name=name)
            self.atom_spaces[token] = das
            self.atom_space_status[token] = AtomSpaceStatus.READY
            return self._success(token)

    def _check_das_key(self, key):
        if key not in self.atom_spaces:
            return self._error("Invalid DAS key")
        if self.atom_space_status[key] != AtomSpaceStatus.READY:
            return self._error(f"DAS {key} is busy")
        return None
        
    def load_knowledge_base(self, request, context):
        with self.locked_scope:
            key = request.key
            url = request.url
            check = self._check_das_key(key)
            if check:
                return check
            self.atom_space_status[key] = AtomSpaceStatus.LOADING
            thread = KnowledgeBaseLoader(self, key, url)
            thread.start()
            return self._success(AtomSpaceStatus.LOADING)

    def check_das_status(self, request, context):
        with self.locked_scope:
            key = request.key
            if key not in self.atom_spaces:
                return self._error("Invalid DAS key")
            das_status = self.atom_space_status[key]
            return self._success(das_status)

    def _basic_das_call(self, key, method, args):
        check = self._check_das_key(key)
        if check:
            return check
        das = self.atom_spaces[key]
        try:
            callable_method = getattr(DistributedAtomSpace, method)
            answer = callable_method(*[das, *args])
        except Exception as exception:
            formatted_lines = traceback.format_exc().splitlines()
            return self._error(str(exception) + " " + str(formatted_lines))
        return self._success(str(answer))
        
    def clear(self, request, context):
        with self.locked_scope:
            return self._basic_das_call(request.key, "clear_database", [])

    def count(self, request, context):
        with self.locked_scope:
            return self._basic_das_call(request.key, "count_atoms", [])

    def get_atom(self, request, context):
        with self.locked_scope:
            handle = request.handle
            output_format = self.query_output_map[request.output_format]
            return self._basic_das_call(request.key, "get_atom", [handle, output_format])

    def search_nodes(self, request, context):
        with self.locked_scope:
            node_type = request.node_type if request.node_type else None
            node_name = request.node_name if request.node_name else None
            output_format = self.query_output_map[request.output_format]
            return self._basic_das_call(request.key, "get_nodes", 
                [node_type, node_name, output_format])

    def search_links(self, request, context):
        with self.locked_scope:
            link_type = request.link_type if request.link_type else None
            target_types = request.target_types if request.target_types else None
            targets = request.targets if request.targets else None
            output_format = self.query_output_map[request.output_format]
            return self._basic_das_call(request.key, "get_links", 
                [link_type, target_types, targets, output_format])

    def query(self, request, context):
        with self.locked_scope:
            key = request.key
            query_str = request.query
            output_format = self.query_output_map[request.output_format]
            query = _parse_query(query_str)
            if query is None:
                return self._error(f"Invalid query")
            return self._basic_das_call(request.key, "query", [query, output_format])

def main():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_ServiceDefinitionServicer_to_server(ServiceDefinition(), server)
    server.add_insecure_port(f"[::]:{SERVICE_PORT}")
    server.start()
    print(f"Server listening on 0.0.0.0:{SERVICE_PORT}")
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    main()
