import os
import sys
import time
from concurrent import futures
import string
import random
import grpc
from enum import Enum
import tempfile
from threading import Lock, Thread
sys.path.append(os.path.join(os.path.dirname(__file__), "service_spec"))
import das_pb2 as pb2
import das_pb2_grpc as pb2_grpc
from das.distributed_atom_space import DistributedAtomSpace, QueryOutputFormat

SERVICE_PORT = 7025

def build_random_string(length):
    return ''.join(random.choice(string.ascii_lowercase) for i in range(length))

class AtomSpaceStatus(str, Enum):
    READY = "Ready"
    LOADING = "Loading knowledge base"

class OutputFormat(str, Enum):
    HANDLE = "HANDLE"
    DICT = "DICT"
    JSON = "JSON"

class KnowledgeBaseLoader(Thread):

    def __init__(self, service: "ServiceDefinition", das_key: str, url: str):
        super().__init__()
        self.service = service
        self.das_key = das_key
        self.url = url

    def run(self):
        temp_dir = tempfile.mkdtemp()
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
        self.query_output_map = {
            OutputFormat.HANDLE: QueryOutputFormat.HANDLE,
            OutputFormat.DICT: QueryOutputFormat.ATOM_INFO,
            OutputFormat.JSON: QueryOutputFormat.JSON
        }

    def _get_das(self, key: str):
        self.lock.acquire()
        das = self.atom_spaces[key]
        self.lock.release()
        return das

    def _set_das_status(self, key: str, status: str):
        self.lock.acquire()
        self.atom_space_status[key] = status
        self.lock.release()

    def _error(self, error_message: str):
        return pb2.Status(success=False, msg=error_message)
        
    def _success(self, message=AtomSpaceStatus.READY):
        return pb2.Status(success=True, msg=message)
        
    def create(self, request, context):
        name = request.name
        self.lock.acquire()
        if any(das.database_name == name for das in self.atom_spaces.values()):
            self.lock.release()
            return self._error(f"DAS named '{name}' already exists")
        while True:
            token = build_random_string(20)
            if token not in self.atom_spaces:
                break
        das = DistributedAtomSpace(database_name=name)
        self.atom_spaces[token] = das
        self.atom_space_status[token] = AtomSpaceStatus.READY
        self.lock.release()
        return self._success(token)

    def load_knowledge_base(self, request, context):
        key = request.das_key
        url = request.url
        self.lock.acquire()
        if self.atom_space_status[key] != AtomSpaceStatus.READY:
            self.lock.release()
            return self._error(f"DAS {key} is busy")
        else:
            self.atom_space_status[key] = AtomSpaceStatus.LOADING
            thread = KnowledgeBaseLoader(self, key, url)
            thread.start()
        self.lock.release()
        return self._success(AtomSpaceStatus.LOADING)

    def check_das_status(self, request, context):
        self.lock.acquire()
        das_status = self.atom_space_status[request.key]
        self.lock.release()
        return self._success(das_status)
        
    def clear(self, request, context):
        key = request.key
        self.lock.acquire()
        if self.atom_space_status[key] != AtomSpaceStatus.READY:
            self.lock.release()
            return self._error(f"DAS {key} is busy")
        else:
            das = self.atom_spaces[key]
            try:
                das.clear_database()
            except Exception as exception:
                self.lock.release()
                return self._error(str(exception))
        self.lock.release()
        return self._success()

    def count(self, request, context):
        key = request.key
        self.lock.acquire()
        if self.atom_space_status[key] != AtomSpaceStatus.READY:
            self.lock.release()
            return self._error(f"DAS {key} is busy")
        else:
            das = self.atom_spaces[key]
            try:
                node_count, link_count = das.count_atoms()
            except Exception as exception:
                self.lock.release()
                return self._error(str(exception))
        self.lock.release()
        return self._success(f"{node_count} {link_count}")

    def search_nodes(self, request, context):
        key = request.das_key
        node_type = request.node_type if request.node_type else None
        node_name = request.node_name if request.node_name else None
        output_format = self.query_output_map[request.output_format]
        self.lock.acquire()
        if self.atom_space_status[key] != AtomSpaceStatus.READY:
            self.lock.release()
            return self._error(f"DAS {key} is busy")
        else:
            das = self.atom_spaces[key]
            try:
                answer = das.get_nodes(node_type, node_name, output_format)
            except Exception as exception:
                self.lock.release()
                return self._error(str(exception))
        self.lock.release()
        return self._success(f"{answer}")

    def search_links(self, request, context):
        key = request.das_key
        link_type = request.link_type if request.link_type else None
        target_types = request.target_types if request.target_types else None
        targets = request.targets if request.targets else None
        output_format = self.query_output_map[request.output_format]
        self.lock.acquire()
        if self.atom_space_status[key] != AtomSpaceStatus.READY:
            self.lock.release()
            return self._error(f"DAS {key} is busy")
        else:
            das = self.atom_spaces[key]
            try:
                answer = das.get_links(link_type, target_types, targets, output_format)
            except Exception as exception:
                self.lock.release()
                return self._error(str(exception))
        self.lock.release()
        return self._success(f"{answer}")

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
