from concurrent import futures
import time
import grpc
import person_pb2
import person_pb2_grpc
from grpc_service import PersonServicer 

#server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
#person_pb2_grpc.add_PersonServiceServicer_to_server(PersonServicer(), server)
#server.add_insecure_port('[::]:5001')
#server.start()
#server.wait_for_termination()

# Initialize gRPC server
server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
person_pb2_grpc.add_PersonServiceServicer_to_server(PersonServicer(), server)


print("Server starting on port 5002...")
server.add_insecure_port("[::]:5002")
server.start()
# Keep thread alive
try:
    while True:
        time.sleep(86400)
except KeyboardInterrupt:
    server.stop(0)