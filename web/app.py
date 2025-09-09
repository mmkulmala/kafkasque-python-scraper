"""Tiny Flask web UI to display latest price updates from Kafka."""
import os
import threading
import json
from kafka import KafkaConsumer
from flask import Flask, jsonify
KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC_PRICE_UPDATES", "price-updates")

app = Flask(__name__)
latest = []        # in-memory ring buffer
MAX_ITEMS = 100

def kafka_listener():
    """Background thread that consumes price updates and stores a small buffer."""
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='web-consumer'
    )
    for msg in consumer:
        item = msg.value
        latest.insert(0, item)
        if len(latest) > MAX_ITEMS:
            latest.pop()
        print("web received:", item)

@app.route("/api/latest")
def get_latest():
    """Return the most recent price updates as JSON."""
    return jsonify(latest)

@app.route("/")
def index():
    """Serve a minimal HTML page that polls and renders the latest updates."""
    # very small front-end page
    return """
    <!doctype html>
    <html><head><title>Price updates</title></head>
    <body>
      <h2>Latest price updates</h2>
      <ul id=\"list\"></ul>
      <script>
        async function refresh(){
          const res = await fetch('/api/latest');
          const items = await res.json();
          const ul = document.getElementById('list');
          ul.innerHTML = '';
          items.slice(0,20).forEach(it => {
            const li = document.createElement('li');
            li.textContent = new Date(it.processed_at*1000).toLocaleString() + ' — ' + (it.price_normalized || it.price_raw) + ' — ' + it.product_url;
            ul.appendChild(li);
          });
        }
        setInterval(refresh, 3000);
        refresh();
      </script>
    </body>
    </html>
    """

if __name__ == "__main__":
    t = threading.Thread(target=kafka_listener, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=5000)
