import grpc
import location_pb2
import location_pb2_grpc
from google.protobuf.timestamp_pb2 import Timestamp

"""
Sample implementation of a writer that can be used to write messages to gRPC.
"""

print("Sending sample payload...")

channel = grpc.insecure_channel("localhost:5005")
stub = location_pb2_grpc.LocationServiceStub(channel)

creation_time = Timestamp()
creation_time.GetCurrentTime()
# Update this with desired payload
location = location_pb2.LocationMessage(    
    person_id=1,
    longitude="105.123456",
    latitude="-2.123456",
    creation_time = creation_time
)


response = stub.Create(location)