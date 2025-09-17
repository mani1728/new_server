FROM python:3.11-slim

# برای اطمینان از گواهی‌ها و پایداری نصب pip
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ارتقای pip و نصب کلاینت رسمی v3
RUN python -m pip install --upgrade pip \
 && pip install --no-cache-dir influxdb3-python

# کد شما
COPY influx_handler.py /app/influx_handler.py

ENV PYTHONUNBUFFERED=1
CMD ["python", "-u", "/app/influx_handler.py"]
