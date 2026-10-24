import sys

from confluent_kafka import OFFSET_BEGINNING, Consumer

# Usage:
#   python replay.py            -> re-read payments from the very beginning
#   python replay.py 6          -> re-read payments starting at offset 6
start = int(sys.argv[1]) if len(sys.argv) > 1 else OFFSET_BEGINNING

consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": "payments-manual",  # same group as consumer_manual_commit.py
        "enable.auto.commit": False,
    }
)


# on_assign runs before fetching starts: changing the offsets here
# overrides whatever the group had committed.
def on_assign(consumer, partitions):
    for p in partitions:
        p.offset = start
    print(f"Assigned {[p.partition for p in partitions]}, starting at {start}")
    consumer.assign(partitions)


consumer.subscribe(["payments"], on_assign=on_assign)

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            break
        print(f"offset {msg.offset()}: replayed {msg.value().decode('utf-8')}")
        # No commit: replaying doesn't change the group's real position
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
