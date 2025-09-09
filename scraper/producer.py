import os
import time
import json
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC_SCRAPING_EVENTS", "scraping-events")
URL = os.getenv("TARGET_URL", "https://httpbin.org/html")
INTERVAL = int(os.getenv("SCRAPE_INTERVAL", "60"))

producer = KafkaProducer(
    bootstrap_servers=[KAFKA],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
)

HEADERS = {
    "User-Agent": "kafka-scraper-bot/1.0 (+https://example.com)"
}

def extract_price(html):
    # naive example: adjust for your target site's DOM
    soup = BeautifulSoup(html, "html.parser")
    # Example: find an element with class 'product-price' or fallback to body text
    el = soup.find(class_="product-price")
    if el:
        return el.get_text(strip=True)
    # fallback: demonstrate extracting some text
    body = soup.find("body")
    return body.get_text(strip=True)[:200] if body else "N/A"

def build_event(url, price):
    return {
        "timestamp": int(time.time()),
        "product_url": url,
        "price": price
    }

def scrape_and_publish():
    try:
        resp = requests.get(URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        price = extract_price(resp.text)
    except Exception as e:
        price = "ERROR: " + str(e)
    event = build_event(URL, price)
    producer.send(TOPIC, value=event)
    producer.flush()
    print("published:", event)

if __name__ == "__main__":
    print(f"Starting scraper -> {KAFKA} -> topic {TOPIC}")
    while True:
        scrape_and_publish()
        time.sleep(INTERVAL)
