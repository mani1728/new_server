# influx_handler.py (اسکلت ساده)
from influxdb_client import InfluxDBClient, Point, WriteOptions
import os, time

url   = os.environ.get("INFLUX_URL", "http://influx-db:8086")
token = os.environ.get("INFLUX_TOKEN")
org   = os.environ.get("INFLUX_ORG", "my-org")
bucket= os.environ.get("INFLUX_BUCKET", "my-bucket")

client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=WriteOptions(batch_size=1))

while True:
    p = Point("heartbeat").tag("service","influx-handler").field("alive", 1)
    write_api.write(bucket=bucket, org=org, record=p)
    time.sleep(10)
