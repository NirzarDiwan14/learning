from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({"bootstrap.servers": "localhost:9092"})

# 1 partition keeps offsets easy to follow in this lesson
futures = admin.create_topics([NewTopic("payments", num_partitions=1, replication_factor=1)])

for name, future in futures.items():
    try:
        future.result()
        print(f"Topic '{name}' created")
    except KafkaException as e:
        print(f"Could not create topic '{name}': {e}")
