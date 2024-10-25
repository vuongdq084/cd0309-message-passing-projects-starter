from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
import json
import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("location-conumser")

TOPIC_NAME = 'UDACONNECT_LOCATION'
KAFKA_SERVER = 'kafka:9092'

# Set up the database engine
DATABASE_URI = 'postgresql://ct_admin:Abc12345@postgres:5432/geoconnections'
engine = create_engine(DATABASE_URI)

# Initialize Kafka consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=['kafka:9092']
)

# SQL query for inserting location data into the database
insert_query = """
    INSERT INTO location (person_id, coordinate, creation_time)
    VALUES (:person_id, ST_Point(:latitude, :longitude), :creation_time)
"""

# Consume messages from Kafka and insert into DB
for message in consumer:
    location_data = message.value
    logger.info(f"Consumed message: {location_data}")

    # Prepare data for insertion
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
