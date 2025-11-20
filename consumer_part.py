# 3. Обробка даних:
# Напишіть Python-скрипт, який підписується на топік building_sensors, зчитує повідомлення і перевіряє отримані дані:
# - якщо температура перевищує 40°C, генерує сповіщення і відправляє його в топік temperature_alerts;
# - якщо вологість перевищує 80% або сягає менше 20%, генерує сповіщення і відправляє його в топік humidity_alerts.
# Сповіщення повинні містити ідентифікатор датчика, значення показників, час та
# повідомлення про перевищення порогового значення.

from kafka import KafkaConsumer
from kafka import KafkaProducer
from configs import kafka_config
import json

# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='my_consumer_group_3'   # Ідентифікатор групи споживачів
)

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Назва топіку
my_name = "oksana"
topic_name = f'{my_name}_building_sensors'
temperature_topic_name = f'{my_name}_temperature_alerts'
humidity_topic_name = f'{my_name}_humidity_alerts'

# Підписка на тему
consumer.subscribe([topic_name])

print(f"Subscribed to topic '{topic_name}'")

# Обробка повідомлень з топіку
try:
    for message in consumer:
        print(f"Received message: {message.value} from {message.key}")

        rec = message.value
        timestamp = str(rec.get("timestamp"))
        temperature = int(rec.get("temperature"))
        humidity = int(rec.get("humidity"))

        if temperature > 40:
            data = {
                "timestamp": timestamp,  # Часова мітка
                "temperature": temperature,
                "message": "out of range"
            }
            producer.send(temperature_topic_name, key=message.key, value=data)
            producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
            print(f"sent {message.key} to topic {temperature_topic_name} data {data} successfully . Температура перевищила поріг")

        if humidity < 20 or humidity > 80:
            data = {
                "timestamp": timestamp,  # Часова мітка
                "humidity": humidity,
                "message": "out of range"
            }
            producer.send(humidity_topic_name, key=message.key, value=data)
            producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
            print(f"sent {message.key} to topic {humidity_topic_name} data {data} successfully. Вологість вийшла за межі")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    producer.close()
    consumer.close()  # Закриття consumer
