import json
from confluent_kafka import Consumer

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-tracker",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)
consumer.subscribe(["orders"])

print("🚚 Order Tracker is running and listening for orders...")

try:
    while True:
        msg = consumer.poll(1.0)  # Timeout of 1 second
        print(f"Polled message: {msg}")
        if msg is None:
            continue
        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue

        value = msg.value().decode("utf-8")
        order = json.loads(value)
        print(f"📦 New order received: {order['quantity']} x {order['item']}")

except KeyboardInterrupt:
    print("🛑 Stopping Order Tracker...")
finally:
    consumer.close()
