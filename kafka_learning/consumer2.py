from confluent_kafka import Consumer, KafkaError

config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "my_consumer_group_2",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(config)
consumer.subscribe(["my_topic"])

print("consumer-2.py is running...")
print("Consuming messages from topic: my_topic")


try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(
                    f"Reached end of partition {msg.partition()} at offset {msg.offset()}"
                )
                continue
            else:
                print(f"Error while consuming message: {msg.error()}")
                break

        print(
            f"Received message: {msg.value().decode('utf-8')} from topic: {msg.topic()} at offset: {msg.offset()}"
        )


except KeyboardInterrupt:
    pass
finally:
    print("Closing consumer...")
    consumer.close()
