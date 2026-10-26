import time

from confluent_kafka import Consumer, KafkaException

# A consumer whose processing takes LONGER than max.poll.interval.ms.
config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "slow-processor",
    "auto.offset.reset": "earliest",
    "enable.auto.offset.store": False,
    "session.timeout.ms": 6000,
    # If poll() isn't called again within 10 s, the consumer is considered stuck and is
    # thrown out of the group (default: 300 s = 5 min)
    "max.poll.interval.ms": 10_000,
}

consumer = Consumer(config)


def on_assign(c, partitions):
    print(f"ASSIGNED {[p.partition for p in partitions]}")


def on_revoke(c, partitions):
    print(f"REVOKED {[p.partition for p in partitions]}")


consumer.subscribe(["payment-requests"], on_assign=on_assign, on_revoke=on_revoke)

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        print(f"p{msg.partition()} off{msg.offset()}: processing (takes 12 s)...")
        time.sleep(12)  # e.g. 3 retries with long backoff, or a very slow API
        try:
            consumer.store_offsets(message=msg)
            print(f"p{msg.partition()} off{msg.offset()}: done, offset stored locally (not committed yet!)")
        except KafkaException as e:
            print(f"p{msg.partition()} off{msg.offset()}: done, but could NOT store offset: {e}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
