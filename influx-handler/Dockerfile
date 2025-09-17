FROM python:3.11-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates librdkafka-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN python -m pip install --upgrade pip \
 && pip install --no-cache-dir confluent-kafka==2.11.1 influxdb3-python requests

COPY main.py /app/main.py

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
CMD ["python", "-u", "/app/main.py"]
