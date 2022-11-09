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
from das.distributed_atom_space import DistributedAtomSpace

SERVICE_PORT = 7025

def build_random_string(length):
    return ''.join(random.choice(string.ascii_lowercase) for i in range(length))

class AtomSpaceStatus(str, Enum):
    READY = "Ready"
    LOADING = "Loading knowledge base"

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

    def _add_new_das(self, name: str):
        self.lock.acquire()
        if any(das.database_name == name for das in self.atom_spaces.values()):
            return None
        while True:
            token = build_random_string(20)
            if token not in self.atom_spaces:
                break
        das = DistributedAtomSpace(database_name=name)
        self.atom_spaces[token] = das
        self.atom_space_status[token] = AtomSpaceStatus.READY
        self.lock.release()
        return token

    def _load_knowledge_base(self, key: str, url: str):
        self.lock.acquire()
        if self.atom_space_status[key] != AtomSpaceStatus.READY:
            self.lock.release()
            return f"DAS {key} is busy"
        else:
            self.atom_space_status[key] = AtomSpaceStatus.LOADING
            thread = KnowledgeBaseLoader(self, key, url)
            thread.start()
        self.lock.release()
        return None

    def _clear_knowledge_base(self, key: str):
        self.lock.acquire()
        if self.atom_space_status[key] != AtomSpaceStatus.READY:
            self.lock.release()
            return f"DAS {key} is busy"
        else:
            das = self.atom_spaces[key]
            das.clear_database()
        self.lock.release()
        return None

    def _get_das(self, key: str):
        self.lock.acquire()
        das = self.atom_spaces[key]
        self.lock.release()
        return das

    def _get_das_status(self, key: str):
        self.lock.acquire()
        das_status = self.atom_space_status[key]
        self.lock.release()
        return das_status

    def _set_das_status(self, key: str, status: str):
        self.lock.acquire()
        self.atom_space_status[key] = status
        self.lock.release()
        
    def create(self, request, context):
        knowledge_base_name = request.name
        token = self._add_new_das(knowledge_base_name)
        return pb2.DASKey(key=token)

    def load_knowledge_base(self, request, context):
        key = request.das_key
        url = request.url
        error_message = self._load_knowledge_base(key, url)
        if error_message is None:
            return pb2.Status(success=True, msg=AtomSpaceStatus.LOADING)
        else:
            return pb2.Status(success=False, msg=error_message)

    def check_das_status(self, request, context):
        key = request.key
        msg = self._get_das_status(key)
        return pb2.Status(success=True, msg=msg)

    def clear(self, request, context):
        key = request.key
        error_message = self._clear_knowledge_base(key)
        if error_message is None:
            return pb2.Status(success=True, msg=AtomSpaceStatus.READY)
        else:
            return pb2.Status(success=False, msg=error_message)

    #def count(self, request, context):
    #    key = request.key
    #    node_count, link_count = self._knowledge_base_count(key)
    #    if error_message is None:
    #        return pb2.Status(success=True, msg=AtomSpaceStatus.READY)
    #    else:
    #        return pb2.Status(success=False, msg=error_message)

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
