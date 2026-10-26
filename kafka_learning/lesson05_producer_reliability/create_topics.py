from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({"bootstrap.servers": "localhost:9092"})

# One topic per compression type, so we can compare their size on disk
names = ["reliability", "bench-none", "bench-gzip", "bench-lz4", "bench-zstd"]
futures = admin.create_topics([NewTopic(n, num_partitions=3, replication_factor=1) for n in names])

for name, future in futures.items():
    try:
        future.result()
        print(f"Topic '{name}' created")
    except KafkaException as e:
        print(f"Could not create topic '{name}': {e}")
