# topic-manager/Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# اگر می‌خواهی نسخه دقیقی داشته باشی، از requirements.txt استفاده کن.
# در غیر این صورت، نصب مستقیم:
RUN pip install --no-cache-dir confluent-kafka==2.11.1

# ⛳️ فیکسِ COPY: همه فایل‌های پروژه را به /app کپی کن
COPY ./app/*.py /app/

# پیش‌فرض بهتر: اجازه بده از Compose کنترل شود
# ENV DRY_RUN=true   # ← پیشنهاد می‌کنم این خط را حذف کنی

# اجرای پیش‌فرض
CMD ["python", "-u", "/app/main.py"]
