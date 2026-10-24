import sys

from confluent_kafka import Consumer, TopicPartition

# Usage: python inspect_offsets.py <group.id>
# Computes LAG yourself, the same number kafka-consumer-groups.sh shows.
group = sys.argv[1] if len(sys.argv) > 1 else "payments-manual"
topic = "payments"

# A consumer that never subscribes: we only use it to ask questions
consumer = Consumer({"bootstrap.servers": "localhost:9092", "group.id": group})

partitions = [
    TopicPartition(topic, p)
    for p in consumer.list_topics(topic).topics[topic].partitions
]

for tp in consumer.committed(partitions, timeout=5):
    low, high = consumer.get_watermark_offsets(tp, timeout=5)
    # tp.offset is negative (-1001) when the group never committed anything
    committed = tp.offset if tp.offset >= 0 else None
    lag = high - (committed if committed is not None else low)
    print(
        f"group={group} partition={tp.partition} "
        f"earliest={low} latest={high} committed={committed} LAG={lag}"
    )

consumer.close()
