import time 
from confluent_kafka import Producer

config = {
    'bootstrap.servers': 'localhost:9092',
}

producer = Producer(config)


topic_name = "my_topic"


def delivery_report(err,msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message ---{msg.value().decode('utf-8')}--- delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

print("Producing messages to topic:", topic_name)

def message_producer(msg: str):
    producer.produce(
        topic = topic_name,
        value = msg.encode('utf-8'),
        callback = delivery_report
    )

n = 10  # Number of messages to produce
for i in range(n):
    message = f"Message: {i}"
    message_producer(message)

producer.flush()


    
