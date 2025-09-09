"""Event processor that consumes scraping events and publishes price updates."""

import os
import json
import time
from kafka import KafkaConsumer, KafkaProducer
KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
IN_TOPIC = os.getenv("TOPIC_SCRAPING_EVENTS", "scraping-events")
OUT_TOPIC = os.getenv("TOPIC_PRICE_UPDATES", "price-updates")

consumer = KafkaConsumer(
    IN_TOPIC,
    bootstrap_servers=[KAFKA],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='processor-group'
)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5
)

def transform(scraping_event):
    """Normalize basic fields on a scraping event and add processing metadata.

    Args:
        scraping_event: Dict-like event payload produced by the scraper.

    Returns:
        A new dictionary with normalized price and processed timestamp.
    """
    # small example transformation: normalize price and add source
    price = scraping_event.get("price", "N/A")
    if isinstance(price, str) and price.startswith("$"):
        normalized = price.replace("$", "").strip()
    else:
        normalized = price
    out = {
        "product_url": scraping_event.get("product_url"),
        "timestamp": scraping_event.get("timestamp"),
        "price_raw": price,
        "price_normalized": normalized,
        "processed_at": int(time.time())
    }
    return out

if __name__ == "__main__":
    print(f"Processor listening on {IN_TOPIC}, output -> {OUT_TOPIC}")
    for msg in consumer:
        event = msg.value
        print("received:", event)
        out_event = transform(event)
        producer.send(OUT_TOPIC, value=out_event)
        producer.flush()
        print("published:", out_event)
