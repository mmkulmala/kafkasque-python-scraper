FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# copy all service directories
COPY scraper /app/scraper
COPY processor /app/processor
COPY web /app/web

# default command (overridden in compose for different services)
CMD ["python", "scraper/producer.py"]
