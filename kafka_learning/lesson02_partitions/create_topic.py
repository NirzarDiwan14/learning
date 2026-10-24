from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({"bootstrap.servers": "localhost:9092"})

# 3 partitions = the topic is split into 3 independent, ordered logs.
# replication_factor=1 because we only have 1 broker.
topic = NewTopic("orders", num_partitions=3, replication_factor=1)

# create_topics() is async too: it returns {topic_name: future}
futures = admin.create_topics([topic])

for name, future in futures.items():
    try:
        future.result()
        print(f"Topic '{name}' created with 3 partitions")
    except KafkaException as e:
        print(f"Could not create topic '{name}': {e}")
