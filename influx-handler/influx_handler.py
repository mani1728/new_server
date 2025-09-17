from influxdb_client_3 import InfluxDBClient3, Point
import os, time, sys, traceback

HOST = os.getenv("INFLUX_URL", "http://influx-db:8181")
DB   = os.getenv("INFLUX_DATABASE", "my_database")
TOKEN= os.getenv("INFLUX_TOKEN")

def write_heartbeat(client):
    p = Point("heartbeat").tag("service", "influx-handler").field("alive", 1)
    client.write(record=p)

def main():
    print(f"[boot] host={HOST} db={DB} token={'set' if TOKEN else 'MISSING'}", flush=True)
    if not TOKEN:
        print("[error] INFLUX_TOKEN is missing", flush=True)
        sys.exit(1)

    # backoff ساده
    backoff = 1
    while True:
        try:
            with InfluxDBClient3(host=HOST, database=DB, token=TOKEN) as client:
                print("[ok] connected to InfluxDB v3", flush=True)
                while True:
                    write_heartbeat(client)
                    print("[ok] heartbeat write", flush=True)
                    time.sleep(10)
        except KeyboardInterrupt:
            print("[exit] interrupted", flush=True)
            break
        except Exception as e:
            print(f"[warn] write/connect failed: {e}", flush=True)
            traceback.print_exc()
            time.sleep(min(backoff, 30))
            backoff = min(backoff * 2, 30)

if __name__ == "__main__":
    main()
