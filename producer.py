from confluent_kafka import Producer
import uuid
import json
producer_config = {
    "bootstrap.servers": "localhost:9092"
    }

Producer = Producer(producer_config)


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed for Order {msg.key()}: {err}")
    else:
        print(f"✅ Order delivered {msg.value().decode("utf-8")} successfully")
        print(f"✅ Delivered to {msg.topic()}, partition: {msg.partition()}, at offset: {msg.offset()}")
        print(dir(msg))

order = {
    "order_id": str(uuid.uuid4()),  
    "user": "john",
    "item": "veg sandwich",
    "quantity": 2,
}

value = json.dumps(order).encode("utf-8")

Producer.produce(
    topic="orders",
    value=value,
    callback=delivery_report
)

Producer.flush()
