import os
import sys
import time

from confluent_kafka import Consumer

# Usage:
#   python consumer_auto_commit.py dupes crash   -> commits rarely, crashes on payment-3
#   python consumer_auto_commit.py dupes         -> same group, no crash: where does it restart?
#   python consumer_auto_commit.py loss crash    -> commits often, crashes on payment-3
#   python consumer_auto_commit.py loss          -> same group, no crash: where does it restart?
mode = sys.argv[1] if len(sys.argv) > 1 else "dupes"
crash = "crash" in sys.argv[2:]

CRASH_AT = 3  # crash while processing payment-3

commit_interval = {"dupes": 10_000, "loss": 100}[mode]

config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": f"payments-auto-{mode}",
    "auto.offset.reset": "earliest",
    # Default behaviour: a background timer commits "what poll() returned"
    # every auto.commit.interval.ms, whether you finished processing it or not.
    "enable.auto.commit": True,
    "auto.commit.interval.ms": commit_interval,
    # Lowest allowed value; so a crashed consumer is noticed in 6s, not 45s
    "session.timeout.ms": 6000,
}

consumer = Consumer(config)
consumer.subscribe(["payments"])

print(f"mode={mode}, crash={crash}, group={config['group.id']}, auto.commit.interval.ms={commit_interval}")

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
        time.sleep(1)  # pretend this is slow work (e.g. charging a card)

        if crash and msg.offset() == CRASH_AT:
            print("💥 CRASH (os._exit = like kill -9: no finally, no close)")
            os._exit(1)

        print(f"offset {msg.offset()}: done {value}")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
