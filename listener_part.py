# Display temperature and humidity alerts produced by the processing service.

from kafka import KafkaConsumer
from configs import kafka_config
import json

# Deserialize both message keys and values from JSON.
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Replay alerts when the group has no offset.
    enable_auto_commit=True,       # Persist progress for subsequent runs.
    group_id='my_consumer_group_1' # Keep listener offsets separate from processing.
)

# Listen to both derived alert streams.
my_name = "oksana"
temperature_topic_name = f'{my_name}_temperature_alerts'
humidity_topic_name = f'{my_name}_humidity_alerts'

consumer.subscribe([temperature_topic_name, humidity_topic_name])

print(f"Subscribed to topics {humidity_topic_name}, {temperature_topic_name}")

# Print alerts continuously and always close the consumer cleanly.
try:
    for message in consumer:
        print(f"Received message: {message.value} from {message.key}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Release the broker connection.
