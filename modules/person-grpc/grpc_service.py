from concurrent import futures
import time
import grpc
import person_pb2
import person_pb2_grpc
from app.udaconnect.services import PersonService

class PersonServicer(person_pb2_grpc.PersonServiceServicer):
    def Get(self, request, context):
        persons = PersonService.retrieve_all()  # Gọi hàm retrieve_all từ PersonService
        person_messages = []

        for person in persons:
            person_message = person_pb2.PersonMessage(
                id=person.id,
                first_name=person.first_name,
                last_name=person.last_name,
                company_name=person.company_name
            )
            person_messages.append(person_message)

        return person_pb2.PersonMessageList(orders=person_messages)
    
# Initialize gRPC server
server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
person_pb2_grpc.add_PersonServiceServicer_to_server(PersonServicer(), server)


print("Server starting on port 5001...")
server.add_insecure_port("[::]:5001")
server.start()
# Keep thread alive
try:
    while True:
        time.sleep(86400)
except KeyboardInterrupt:
    server.stop(0)