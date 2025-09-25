FROM python:3.11-slim

WORKDIR /app
RUN pip install --no-cache-dir confluent-kafka==2.11.1

COPY ./register_service.py /app/register_service.py

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
CMD ["python", "-u", "/app/register_service.py"]
