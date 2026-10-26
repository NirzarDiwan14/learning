import sys
from collections import Counter

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

# Usage:
#   python reader.py naive              -> outputs of processor_naive.py
#   python reader.py tx                 -> outputs of processor_tx.py, as a read_committed consumer sees them
#   python reader.py tx uncommitted     -> same, but read_uncommitted (also sees aborted / open transactions)
prefix = sys.argv[1] if len(sys.argv) > 1 else "tx"
level = sys.argv[2] if len(sys.argv) > 2 else "committed"

consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": "reader",  # required by the client, but we never join the group or commit
        "isolation.level": f"read_{level}",
        "enable.auto.commit": False,
    }
)

topics = [f"{prefix}-invoices", f"{prefix}-reservations"]
# assign() instead of subscribe(): read every partition from the start, no group, no commits
consumer.assign(
    [
        TopicPartition(t, p, OFFSET_BEGINNING)
        for t in topics
        for p in consumer.list_topics(t).topics[t].partitions
    ]
)

counts = {t: Counter() for t in topics}
while True:
    msg = consumer.poll(3.0)
    if msg is None:  # nothing new for 3 s: we've read everything visible
        break
    if msg.error():
        print(f"Error: {msg.error()}")
        continue
    counts[msg.topic()][msg.key().decode()] += 1
consumer.close()

print(f"isolation.level=read_{level}")
for topic in topics:
    total = sum(counts[topic].values())
    dupes = {k: n for k, n in counts[topic].items() if n > 1}
    print(f"  {topic:<18} {total:>3} messages  for {len(counts[topic])} orders   duplicates: {dupes or 'none'}")
missing = set(counts[topics[0]]) ^ set(counts[topics[1]])
print(f"  orders with an invoice but no reservation (or the reverse): {sorted(missing) or 'none'}")
