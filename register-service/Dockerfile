FROM python:3.11-slim

WORKDIR /app


COPY ./register-service/register_service.py /app/register_service.py
COPY ./register-service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt



ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
CMD ["python", "-u", "/app/register_service.py"]
