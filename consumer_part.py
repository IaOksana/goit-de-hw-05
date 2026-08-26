# Consume sensor readings and route out-of-range values to alert topics.
# Temperature alerts are produced above 40 C. Humidity alerts are produced
# below 20 percent or above 80 percent.

from kafka import KafkaConsumer
from kafka import KafkaProducer
from configs import kafka_config
import json

# Read all available input events and commit offsets automatically.
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Start at the beginning when no offset exists.
    enable_auto_commit=True,       # Commit successfully read offsets automatically.
    group_id='my_consumer_group_3' # Keep processing state for this consumer group.
)

# Use a producer in the same process to publish derived alerts.
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Derive every topic name from the same owner prefix.
my_name = "oksana"
topic_name = f'{my_name}_building_sensors'
temperature_topic_name = f'{my_name}_temperature_alerts'
humidity_topic_name = f'{my_name}_humidity_alerts'

# Subscribe only after the consumer configuration is complete.
consumer.subscribe([topic_name])

print(f"Subscribed to topic '{topic_name}'")

# Process messages continuously until the consumer stops or an error occurs.
try:
    for message in consumer:
        print(f"Received message: {message.value} from {message.key}")

        rec = message.value
        timestamp = str(rec.get("timestamp"))
        temperature = int(rec.get("temperature"))
        humidity = int(rec.get("humidity"))

        # Route high-temperature readings without altering the original event.
        if temperature > 40:
            data = {
                "timestamp": timestamp,  # Preserve the source event timestamp.
                "temperature": temperature,
                "message": "out of range"
            }
            producer.send(temperature_topic_name, key=message.key, value=data)
            producer.flush()  # Ensure the alert is delivered before continuing.
            print(f"sent {message.key} to topic {temperature_topic_name} data {data} successfully . Температура перевищила поріг")

        # Route both low- and high-humidity readings to the same alert stream.
        if humidity < 20 or humidity > 80:
            data = {
                "timestamp": timestamp,  # Preserve the source event timestamp.
                "humidity": humidity,
                "message": "out of range"
            }
            producer.send(humidity_topic_name, key=message.key, value=data)
            producer.flush()  # Ensure the alert is delivered before continuing.
            print(f"sent {message.key} to topic {humidity_topic_name} data {data} successfully. Вологість вийшла за межі")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    producer.close()
    consumer.close()  # Close both clients even when processing fails.
