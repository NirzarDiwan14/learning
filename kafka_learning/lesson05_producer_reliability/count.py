import sys

from confluent_kafka import Consumer, TopicPartition

# Usage: python count.py [topic]   -> how many messages are stored in the topic
topic = sys.argv[1] if len(sys.argv) > 1 else "reliability"

consumer = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "counter"})
partitions = consumer.list_topics(topic).topics[topic].partitions

total = 0
for p in partitions:
    low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=5)
    total += high - low

print(f"topic '{topic}' holds {total} messages")
consumer.close()
