from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({"bootstrap.servers": "localhost:9092"})

topics = [
    NewTopic("orders-placed", num_partitions=3, replication_factor=1),  # input
    # outputs of processor_naive.py
    NewTopic("naive-invoices", num_partitions=3, replication_factor=1),
    NewTopic("naive-reservations", num_partitions=3, replication_factor=1),
    # outputs of processor_tx.py
    NewTopic("tx-invoices", num_partitions=3, replication_factor=1),
    NewTopic("tx-reservations", num_partitions=3, replication_factor=1),
]

for name, future in admin.create_topics(topics).items():
    try:
        future.result()
        print(f"Topic '{name}' created")
    except KafkaException as e:
        print(f"Could not create topic '{name}': {e}")
