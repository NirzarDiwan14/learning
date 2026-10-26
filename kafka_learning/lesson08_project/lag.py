import sys
import time

import config
from confluent_kafka import Consumer, TopicPartition

# Usage: python lag.py           -> print once
#        python lag.py --watch   -> refresh every 2 s
#
# Note: on topics written with TRANSACTIONS (shop-shipments) a fully caught-up consumer shows LAG 1 per
# partition. The last offset is the transaction's commit MARKER, which is never delivered as a message.
GROUPS = {
    "payment-service": [config.ORDERS_TOPIC],
    "order-status-projector": [config.ORDERS_TOPIC, config.PAYMENTS_TOPIC, config.SHIPMENTS_TOPIC],
    "shipping-service": [config.PAYMENTS_TOPIC],
}

# One consumer per group, used only to ask questions (never subscribes)
clients = {
    group: Consumer({"bootstrap.servers": config.BOOTSTRAP_SERVERS, "group.id": group}) for group in GROUPS
}


def report():
    lines = []
    for group, topics in GROUPS.items():
        client = clients[group]
        partitions = [
            TopicPartition(t, p) for t in topics for p in client.list_topics(t, timeout=5).topics[t].partitions
        ]
        total = 0
        per_topic = {}
        for tp in client.committed(partitions, timeout=5):
            low, high = client.get_watermark_offsets(tp, timeout=5)
            lag = high - (tp.offset if tp.offset >= 0 else low)
            total += lag
            per_topic[tp.topic] = per_topic.get(tp.topic, 0) + lag
        detail = "  ".join(f"{t}={n}" for t, n in per_topic.items())
        lines.append(f"{group:<24} LAG {total:>6}   ({detail})")
    return "\n".join(lines)


try:
    while True:
        print(time.strftime("%H:%M:%S"))
        print(report())
        if "--watch" not in sys.argv:
            break
        time.sleep(2)
        print()
except KeyboardInterrupt:
    pass
finally:
    for client in clients.values():
        client.close()
