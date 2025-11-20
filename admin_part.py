# Домашнє завдання до теми «Apache Kafka»

from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# 1. Створення топіків в Kafka:
# Створіть три топіки в Kafka:
# _building_sensors — для зберігання даних з усіх датчиків,
# _temperature_alerts — для зберігання сповіщень про перевищення допустимого рівня температури,
# _humidity_alerts — для зберігання сповіщень про вихід рівня вологості за допустимі рамки.
my_name = "oksana"
general_topic_name = f'{my_name}_building_sensors'
temperature_topic_name = f'{my_name}_temperature_alerts'
humidity_topic_name = f'{my_name}_humidity_alerts'
num_partitions = 2
replication_factor = 1

general_topic = NewTopic(name=general_topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
temperature_topic = NewTopic(name=temperature_topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
humidity_topic = NewTopic(name=humidity_topic_name, num_partitions=num_partitions, replication_factor=replication_factor)

# Створення нового топіку
try:
    admin_client.create_topics(new_topics=[general_topic], validate_only=False)
    print(f"Topic '{general_topic_name}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e} with topic '{general_topic_name}'.")
try:
    admin_client.create_topics(new_topics=[temperature_topic], validate_only=False)
    print(f"Topic '{temperature_topic_name}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e} with topic '{temperature_topic_name}'.")
try:
    admin_client.create_topics(new_topics=[humidity_topic], validate_only=False)
    print(f"Topic '{humidity_topic_name}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e} with topic '{humidity_topic_name}'.")

# Перевіряємо список існуючих топіків
print(admin_client.list_topics())

# Закриття зв'язку з клієнтом
admin_client.close()
