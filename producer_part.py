# Simulate one building sensor and publish its readings to Kafka.
# Run this script multiple times to emulate multiple sensors; each process keeps
# one stable sensor ID for its entire lifetime.

from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random
import os

# Serialize message keys and values as UTF-8 JSON.
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Keep the topic naming convention consistent with the admin and consumer scripts.
my_name = "oksana"
topic_name = f'{my_name}_building_sensors'

# Reuse an explicit ID when supplied; otherwise generate one ID for this process.
SENSOR_ID = os.getenv("SENSOR_ID") or str(random.randint(100000, 999999))


for i in range(130):
    # Generate one reading and wait until Kafka acknowledges all buffered records.
    try:
        data = {
            "timestamp": time.time(),  # Unix timestamp of the simulated reading.
            "temperature": random.randint(25, 45),
            "humidity": random.randint(15, 85)
        }
        producer.send(topic_name, key=SENSOR_ID, value=data)
        producer.flush()  # Make delivery errors visible during the simulation.
        print(f"Message {i} sent {SENSOR_ID} to topic {topic_name} data {data} successfully")
    except Exception as e:
        print(f"An error occurred: {e}")

producer.close()  # Release network resources after the simulation.
