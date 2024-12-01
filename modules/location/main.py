import time
from concurrent import futures
import logging

import grpc
import location_pb2
import location_pb2_grpc

from kafka import KafkaProducer
import json
from datetime import datetime
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("udaconnect-location")

#TOPIC_NAME = 'UDACONNECT_LOCATION'
#KAFKA_SERVER = 'kafka:9092'
KAFKA_SERVER = os.environ["KAFKA_SERVER"]
TOPIC_NAME = os.environ["TOPIC_NAME"]

class LocationServicer(location_pb2_grpc.LocationServiceServicer):
    def Create(self, request, context):
        print("Received a message!")
        logger.info("start receving message")
        creation_time_str = datetime.fromtimestamp(request.creation_time.seconds).isoformat()
        request_value = {
            "person_id": request.person_id,
            "longitude": request.longitude,
            "latitude": request.latitude,
            "creation_time": creation_time_str
        }
        print(request_value)
        logger.info("Request data: %s", request_value)
        logger.info("SENDING TO KAFKA WITH: SERVER - %s - TOPIC: %s", KAFKA_SERVER, TOPIC_NAME)

        producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        producer.send(TOPIC_NAME, request_value)
        producer.flush()
        logger.info("AFTER SENDING")
        return location_pb2.LocationMessage(person_id=request.person_id,
            longitude=request.longitude,
            latitude=request.latitude,
            creation_time=request.creation_time)

# Initialize gRPC server

server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
location_pb2_grpc.add_LocationServiceServicer_to_server(LocationServicer(), server)


print("Server starting on port 5005...")
server.add_insecure_port("[::]:5005")
server.start()
# Keep thread alive
try:
    while True:
        time.sleep(86400)
except KeyboardInterrupt:
    server.stop(0)