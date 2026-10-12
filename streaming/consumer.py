
import json
from pathlib import Path

from kafka import KafkaConsumer

TOPIC_NAME = "shipment-events"
OUTPUT = Path("data/raw/shipment_events.jsonl")
OUTPUT.parent.mkdir(parents=True, exist_ok=True)

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers="localhost:9092",
    group_id="shipping-events-consumer",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

print(f"Consuming events from '{TOPIC_NAME}'... Press Ctrl+C to stop.")

try:
    with OUTPUT.open("a", encoding="utf-8") as file:
        for message in consumer:
            shipment = message.value
            file.write(json.dumps(shipment, ensure_ascii=False) + "\n")
            file.flush()
            print(f"Saved shipment: {shipment['shipment_id']}")

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    consumer.close()
    print(f"Events saved to: {OUTPUT}")