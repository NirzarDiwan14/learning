import config
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({"bootstrap.servers": config.BOOTSTRAP_SERVERS})

topics = [
    NewTopic(config.ORDERS_TOPIC, num_partitions=3, replication_factor=1),
    NewTopic(config.PAYMENTS_TOPIC, num_partitions=3, replication_factor=1),
    NewTopic(config.ORDERS_DLQ_TOPIC, num_partitions=1, replication_factor=1),
]

for name, future in admin.create_topics(topics).items():
    try:
        future.result()
        print(f"Topic '{name}' created")
    except KafkaException as e:
        print(f"Could not create topic '{name}': {e}")
