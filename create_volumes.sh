#!/bin/bash

# اسکریپتی برای ساخت 10 والیوم داکر برای MinIO

echo "🚀 Starting to create 10 Docker volumes for MinIO..."

# حلقه از 1 تا 10 برای ساخت والیوم‌ها با نام‌های شماره‌دار
for i in {1..10}
do
  VOLUME_NAME="minio-data$i"
  docker volume create $VOLUME_NAME
  echo "✅ Volume '$VOLUME_NAME' created."
done

echo "🎉 All 10 volumes have been created successfully!"
echo ""
echo "--- Verifying created volumes ---"

# نمایش والیوم‌های ساخته شده برای تایید
docker volume ls | grep "minio-data"
