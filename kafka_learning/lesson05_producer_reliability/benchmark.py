import argparse
import time

from confluent_kafka import Producer

# Usage examples:
#   python benchmark.py --acks all --linger 5 --compression none
#   python benchmark.py --acks 0
#   python benchmark.py --linger 0
#   python benchmark.py --compression zstd
parser = argparse.ArgumentParser()
parser.add_argument("--acks", default="all", choices=["0", "1", "all"])
parser.add_argument("--linger", type=int, default=5, help="linger.ms (librdkafka default: 5)")
parser.add_argument("--compression", default="none", choices=["none", "gzip", "lz4", "zstd"])
parser.add_argument("-n", type=int, default=200_000, help="number of messages")
args = parser.parse_args()

producer = Producer(
    {
        "bootstrap.servers": "localhost:9092",
        # How many brokers must confirm a write before it counts as delivered
        "acks": args.acks,
        # How long to wait for more messages to fill a batch before sending it
        "linger.ms": args.linger,
        # Compression is applied per BATCH, so bigger batches compress better
        "compression.type": args.compression,
    }
)

topic = f"bench-{args.compression}"
stats = {"ok": 0, "failed": 0, "last_offset": None}


def on_delivery(err, msg):
    if err is not None:
        stats["failed"] += 1
    else:
        stats["ok"] += 1
        stats["last_offset"] = msg.offset()


# A realistic-looking JSON event (~200 bytes, very repetitive, like real JSON)
template = (
    '{{"event_id": "evt-{i:08d}", "event_type": "order_created", "order_id": "order-{o}", '
    '"user_id": "user-{u}", "amount_cents": {a}, "currency": "INR", "created_at": "2026-10-26T10:00:00Z"}}'
)

start = time.perf_counter()
total_bytes = 0
for i in range(args.n):
    value = template.format(i=i, o=i % 5000, u=i % 700, a=(i * 37) % 100_000).encode()
    total_bytes += len(value)
    while True:
        try:
            producer.produce(topic, value=value, on_delivery=on_delivery)
            break
        except BufferError:
            # Local queue is full: serve delivery callbacks so it drains, then retry
            producer.poll(0.1)
    producer.poll(0)

producer.flush()
elapsed = time.perf_counter() - start

print(
    f"acks={args.acks:<3} linger.ms={args.linger:<4} compression={args.compression:<5} | "
    f"{args.n / elapsed:>9,.0f} msg/s  {total_bytes / elapsed / 1e6:6.1f} MB/s  "
    f"ok={stats['ok']} failed={stats['failed']} last offset seen={stats['last_offset']}"
)
