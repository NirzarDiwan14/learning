from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({"bootstrap.servers": "localhost:9092"})

futures = admin.create_topics([NewTopic("order-events", num_partitions=3, replication_factor=1)])

for name, future in futures.items():
    try:
        future.result()
        print(f"Topic '{name}' created")
    except KafkaException as e:
        print(f"Could not create topic '{name}': {e}")
