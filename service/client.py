import os
import time
import sys
import grpc
sys.path.append(os.path.join(os.path.dirname(__file__), "service_spec"))
import das_pb2 as pb2
import das_pb2_grpc as pb2_grpc
from server import SERVICE_PORT, AtomSpaceStatus


def main():
    with grpc.insecure_channel(f"localhost:{SERVICE_PORT}") as channel:
        stub = pb2_grpc.ServiceDefinitionStub(channel)
        service_input = pb2.CreationRequest(name="das")
        response = stub.create(service_input)
        key = response.key
        das_key = pb2.DASKey(key=key)
        print(response)
        service_input = pb2.LoadRequest(das_key=key, url="https://raw.githubusercontent.com/singnet/das/main/data/samples/animals.metta")
        response = stub.load_knowledge_base(service_input)
        print(response)
        while True:
            response = stub.check_das_status(das_key)
            print(response)
            if response.msg == AtomSpaceStatus.READY:
                break
            else:
                time.sleep(1)
        response = stub.clear(das_key)
        print(response)


if __name__ == "__main__":
    main()
