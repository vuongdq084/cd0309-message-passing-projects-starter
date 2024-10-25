from concurrent import futures
import time
import grpc
import person_pb2
import person_pb2_grpc

class PersonServicer(person_pb2_grpc.PersonServiceServicer):
    def Get(self, request, context):
        from app.udaconnect.services import PersonService
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
    
