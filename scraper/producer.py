"""Web scraper that emits events to Kafka for downstream processing."""

import os
import time
import json
import random
from urllib.parse import urlparse, urljoin
from urllib.robotparser import RobotFileParser
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC_SCRAPING_EVENTS", "scraping-events")
URL = os.getenv("TARGET_URL", "https://httpbin.org/html")
INTERVAL = int(os.getenv("SCRAPE_INTERVAL", "60"))
MIN_SCRAPE_INTERVAL = int(os.getenv("MIN_SCRAPE_INTERVAL", "15"))
ROBOTS_TTL_SECONDS = int(os.getenv("ROBOTS_TTL_SECONDS", "3600"))
POLITE_JITTER_FRAC = float(os.getenv("POLITE_JITTER_FRAC", "0.2"))

# Cache of parsed robots.txt per host to avoid frequent re-fetching
_ROBOTS_CACHE = {}

producer = KafkaProducer(
    bootstrap_servers=[KAFKA],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
)

HEADERS = {
    "User-Agent": "kafka-scraper-bot/1.0 (+https://example.com)"
}

def _origin_from_url(url):
    """Return (origin_url, netloc) for a given URL."""
    parsed = urlparse(url)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc
    origin = f"{scheme}://{netloc}"
    return origin, netloc


def _get_robots_parser(url):
    """Fetch and cache robots.txt for the URL's host.

    Returns:
        Tuple[RobotFileParser|None, bool]: (parser, ok) where ok indicates
        whether robots.txt was successfully retrieved.
    """
    now = time.time()
    origin, netloc = _origin_from_url(url)
    cached = _ROBOTS_CACHE.get(netloc)
    if cached and now - cached["fetched_at"] < ROBOTS_TTL_SECONDS:
        return cached["rp"], cached["ok"]

    robots_url = urljoin(origin, "/robots.txt")
    try:
        resp = requests.get(robots_url, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            rp = RobotFileParser()
            rp.set_url(robots_url)
            rp.parse(resp.text.splitlines())
            ok = True
        else:
            rp = None
            ok = False
    except requests.exceptions.RequestException:
        rp = None
        ok = False

    _ROBOTS_CACHE[netloc] = {"rp": rp, "ok": ok, "fetched_at": now}
    return rp, ok


def compute_polite_delay(url, user_agent):
    """Determine if scraping is allowed and a polite delay based on robots.txt.

    Returns:
        tuple[bool, float, str]: (allowed, delay_seconds, reason)
    """
    rp, ok = _get_robots_parser(url)
    base = max(INTERVAL, MIN_SCRAPE_INTERVAL)
    reasons = []

    if ok and rp is not None:
        is_allowed = rp.can_fetch(user_agent, url)
        if not is_allowed:
            reasons.append("disallowed by robots.txt")
        crawl_delay = rp.crawl_delay(user_agent)
        if crawl_delay is not None:
            base = max(base, float(crawl_delay))
            reasons.append(f"crawl-delay={crawl_delay}")
        request_rate_obj = rp.request_rate(user_agent)
        if (
            request_rate_obj is not None
            and getattr(request_rate_obj, "requests", 0)
            and getattr(request_rate_obj, "seconds", 0)
        ):
            per_request_delay = request_rate_obj.seconds / max(request_rate_obj.requests, 1)
            base = max(base, float(per_request_delay))
            reasons.append(
                f"request-rate={request_rate_obj.requests}/{request_rate_obj.seconds}s"
            )
    else:
        is_allowed = False
        reasons.append("robots.txt unavailable")

    jitter = random.uniform(0, base * POLITE_JITTER_FRAC)
    delay_seconds = base + jitter
    reason_str = ", ".join(reasons) if reasons else "ok"
    return is_allowed, delay_seconds, reason_str


def extract_price(html):
    """Extract a price-like string from the HTML document.

    This is a naive example; customize selectors for the target site.
    """
    soup = BeautifulSoup(html, "html.parser")
    # Example: find an element with class 'product-price' or fallback to body text
    el = soup.find(class_="product-price")
    if el:
        return el.get_text(strip=True)
    # fallback: demonstrate extracting some text
    body = soup.find("body")
    return body.get_text(strip=True)[:200] if body else "N/A"

def build_event(url, price):
    """Build the event payload to publish to Kafka.

    Args:
        url: The page URL that was scraped.
        price: Extracted price-like string or an error message.

    Returns:
        A dict suitable for Kafka serialization.
    """
    return {
        "timestamp": int(time.time()),
        "product_url": url,
        "price": price
    }

def scrape_and_publish():
    """Fetch the target URL, extract a price snippet, and publish to Kafka."""
    try:
        resp = requests.get(URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.exceptions.RequestException as err:
        price = f"ERROR: {err}"
    else:
        price = extract_price(resp.text)
    event = build_event(URL, price)
    producer.send(TOPIC, value=event)
    producer.flush()
    print("published:", event)

if __name__ == "__main__":
    print(f"Starting scraper -> {KAFKA} -> topic {TOPIC}")
    while True:
        allowed, sleep_secs, reason = compute_polite_delay(URL, HEADERS["User-Agent"])
        if not allowed:
            print(
                f"Skipping fetch due to robots policy for {URL} ({reason}). "
                f"Sleeping {sleep_secs:.1f}s"
            )
        else:
            scrape_and_publish()
        time.sleep(sleep_secs)
