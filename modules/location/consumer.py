from kafka import KafkaConsumer


TOPIC_NAME = 'UDACONNECT_LOCATION'

consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers='localhost:9092')
for message in consumer:
    print (message)