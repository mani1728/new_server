# Dockerfile.influx
FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir influxdb-client==1.48.0
COPY influx_handler.py /app/influx_handler.py
ENV PYTHONUNBUFFERED=1
CMD ["python", "-u", "/app/influx_handler.py"]
