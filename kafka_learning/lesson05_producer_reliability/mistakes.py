import sys

from confluent_kafka import Producer

# Usage:
#   python mistakes.py no-flush   -> produce 1000 messages and exit WITHOUT flush()
#   python mistakes.py no-poll    -> produce in a loop WITHOUT ever calling poll()
mode = sys.argv[1] if len(sys.argv) > 1 else "no-flush"

producer = Producer(
    {
        "bootstrap.servers": "localhost:9092",
        # Wait up to 1 s to fill a batch (default is 5 ms). Makes the no-flush mistake obvious.
        "linger.ms": 1000,
        # The local queue holds at most this many not-yet-sent messages (default 100,000)
        "queue.buffering.max.messages": 1000,
    }
)

delivered = 0


def on_delivery(err, msg):
    global delivered
    if err is None:
        delivered += 1


if mode == "no-flush":
    for i in range(1000):
        producer.produce("reliability", value=f"no-flush-{i}".encode(), on_delivery=on_delivery)
    print("produce() was called 1000 times without any error. Exiting now, no flush()...")
    # Python exits here. Whatever is still in the local queue is gone.

elif mode == "no-poll":
    for i in range(5000):
        try:
            producer.produce("reliability", value=f"no-poll-{i}".encode(), on_delivery=on_delivery)
        except BufferError as e:
            print(f"BufferError after {i} messages: {e}")
            print(f"delivery callbacks that ran so far: {delivered}")
            break
    producer.flush()
    print(f"after flush(): delivery callbacks that ran: {delivered}")
