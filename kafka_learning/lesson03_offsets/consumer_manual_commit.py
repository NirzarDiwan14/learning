import os
import sys
import time

from confluent_kafka import Consumer

# Usage:
#   python consumer_manual_commit.py crash    -> crashes on payment-3
#   python consumer_manual_commit.py          -> no crash: where does it restart?
crash = "crash" in sys.argv[1:]

CRASH_AT = 3

config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "payments-manual",
    "auto.offset.reset": "earliest",
    # We decide WHEN an offset is committed: only after the work is done
    "enable.auto.commit": False,
    "session.timeout.ms": 6000,
}

consumer = Consumer(config)
consumer.subscribe(["payments"])

print(f"crash={crash}, group={config['group.id']}, manual commit")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            break

        value = msg.value().decode("utf-8")
        print(f"offset {msg.offset()}: processing {value} ...")
        time.sleep(1)

        if crash and msg.offset() == CRASH_AT:
            print("💥 CRASH (os._exit = like kill -9: no finally, no close)")
            os._exit(1)

        # Commit AFTER processing. Commits offset msg.offset()+1 = "next one to read".
        # asynchronous=False blocks until the broker confirms.
        consumer.commit(message=msg, asynchronous=False)
        print(f"offset {msg.offset()}: done {value} (committed {msg.offset() + 1})")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
