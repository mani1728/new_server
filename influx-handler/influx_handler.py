from influxdb_client_3 import InfluxDBClient3, Point, WriteOptions
import os, time

HOST = os.getenv("INFLUX_URL", "http://influx-db:8181")   # v3 روی 8181
DB   = os.getenv("INFLUX_DATABASE", "app")        # اسم database v3
TOKEN= os.getenv("INFLUX_TOKEN")                          # توکن با دسترسی R/W

# نوشتن همزمان (سینک) یا batching (اختیاری)
client = InfluxDBClient3(host=HOST, database=DB, token=TOKEN)

def heartbeat_once():
    p = Point("heartbeat").tag("service", "influx-handler").field("alive", 1)
    client.write(record=p)  # write_precision پیش‌فرض ns؛ در صورت نیاز مشخص کن

if __name__ == "__main__":
    while True:
        heartbeat_once()
        time.sleep(10)
