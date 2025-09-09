# Kafkasque Python Scraper
This is a prototype of Python Web Scraper with Kafka running in Docker container. Includes pieces of Flask.

* docker-compose.yml (Zookeeper + Kafka + three Python services)
* scraper — scrapes a page periodically and publishes to scraping-events
* processor — consumes scraping-events, optionally transforms, and publishes price-updates
* web — Flask app that consumes price-updates and serves the latest updates (simple REST + SSE-like streaming)
Python code for each service, a Dockerfile, and a requirements.txt
Instructions to build & run, and notes about scaling and improvements

# How to run
Save the files in the structure shown above.
Edit docker-compose.yml to set TARGET_URL to the page you want to scrape, and adjust SCRAPE_INTERVAL.

Run and setup locally:
```bash
./setup.sh
```

Build & run:
```bash
docker compose up --build
```

Then visit http://localhost:5050 to see the web UI.
Logs for each service are shown in the compose output — you'll see scraped events, processed messages, and web consumer activity.
