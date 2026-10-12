
import json
import time
import random
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC_NAME = "shipment-events"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    key_serializer=lambda key: key.encode("utf-8"),
)

statuses = [
    "Picked Up",
    "In Transit",
    "Out for Delivery",
    "Delivered",
]

locations = [
    "Cairo",
    "Giza",
    "Beni Suef",
    "Alexandria",
]

print(f"Streaming shipment events to '{TOPIC_NAME}'... Press Ctrl+C to stop.")

try:
    while True:
        shipment = {
            "shipment_id": f"SHP{random.randint(1001, 1010)}",
            "status": random.choice(statuses),
            "location": random.choice(locations),
            "event_time": datetime.now(timezone.utc).isoformat(),
        }

        producer.send(
            TOPIC_NAME,
            key=shipment["shipment_id"],
            value=shipment,
        )

        producer.flush()
        print(f"Sent: {shipment}")
        time.sleep(2)

except KeyboardInterrupt:
    print("\nStreaming stopped.")

finally:
    producer.close()