from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({"bootstrap.servers": "localhost:9092"})

topics = [
    NewTopic("payment-requests", num_partitions=3, replication_factor=1),
    # Dead Letter Topic: messages we could not process end up here
    NewTopic("payment-requests.dlq", num_partitions=1, replication_factor=1),
]

for name, future in admin.create_topics(topics).items():
    try:
        future.result()
        print(f"Topic '{name}' created")
    except KafkaException as e:
        print(f"Could not create topic '{name}': {e}")
