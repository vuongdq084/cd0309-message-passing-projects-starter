from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
import json
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("location-conumser")

#TOPIC_NAME = 'UDACONNECT_LOCATION'
#KAFKA_SERVER = 'kafka:9092'
KAFKA_SERVER = os.environ["KAFKA_SERVER"]
TOPIC_NAME = os.environ["TOPIC_NAME"]

DB_USERNAME = os.environ["DB_USERNAME"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = os.environ["DB_NAME"]

# Set up the database engine
DATABASE_URI = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URI)

# Initialize Kafka consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# SQL query for inserting location data into the database
insert_query = """
    INSERT INTO location (person_id, coordinate, creation_time)
    VALUES (:person_id, ST_Point(:longitude, :latitude), :creation_time)
"""

logger.info(f"STARTING LOCATION CONSUMER")
# Consume messages from Kafka and insert into DB
for message in consumer:
    location_data = message.value
    logger.info(f"Consumed message: {location_data}")

    #Prepare data for insertion
    data = {
       "person_id": location_data["person_id"],
       "latitude": location_data["latitude"],
       "longitude": location_data["longitude"],
       "creation_time": location_data["creation_time"]
    }

    # Execute the insert query
    with engine.connect() as conn:
       conn.execute(text(insert_query), **data)

    logger.info(f"Location saved to DB: {data}")
