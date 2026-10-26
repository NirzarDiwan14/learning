import json
import subprocess

from confluent_kafka import Consumer, TopicPartition

# Compares the bench-* topics: size on the broker's disk per message stored.
# (Per message, because you may run some benchmarks more often than others.)
topics = ["bench-none", "bench-gzip", "bench-lz4", "bench-zstd"]

# Ask the broker for its on-disk sizes (this is the same tool as kafka-log-dirs.sh)
out = subprocess.run(
    [
        "docker", "exec", "kafka", "/opt/kafka/bin/kafka-log-dirs.sh",
        "--bootstrap-server", "localhost:9092", "--describe", "--topic-list", ",".join(topics),
    ],
    capture_output=True, text=True, check=True,
).stdout
report = json.loads(next(line for line in out.splitlines() if line.startswith("{")))

size = dict.fromkeys(topics, 0)
for broker in report["brokers"]:
    for log_dir in broker["logDirs"]:
        for p in log_dir["partitions"]:
            size[p["partition"].rsplit("-", 1)[0]] += p["size"]

consumer = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "counter"})
for topic in topics:
    partitions = consumer.list_topics(topic).topics[topic].partitions
    count = sum(
        high - low
        for low, high in (consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=5) for p in partitions)
    )
    per_msg = size[topic] / count if count else 0
    print(f"{topic:<11} {count:>9,} msgs  {size[topic] / 1e6:7.1f} MB on disk  {per_msg:6.1f} bytes/msg")
consumer.close()
