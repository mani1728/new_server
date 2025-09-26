#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, time, secrets, re, uuid, logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Any

from confluent_kafka import Consumer, Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

# ---------- Optional deps ----------
try:
    import pandas as _pd  # برای پشتیبانی ns-precision
except Exception:
    _pd = None

# ---------- Logging ----------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("register-service")

# ---------- Env ----------
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", os.getenv("BOOTSTRAP_SERVERS", "kafka:9092"))
GROUP_ID = os.getenv("REGISTER_GROUP_ID", "register-service")
REGISTER_TOPIC = os.getenv("REGISTER_TOPIC", "clients.register")
REGISTER_RESP_TOPIC = os.getenv("REGISTER_RESPONSES_TOPIC", "clients.register.responses")
STATUS_TOPIC = os.getenv("STATUS_TOPIC", "clients.status")
REPLIES_TOPIC = os.getenv("REPLIES_TOPIC", "server.replies")

CMD_PREFIX = os.getenv("CMD_TOPIC_PREFIX", "cmd.")
CMD_SHARDS = int(os.getenv("CLIENT_TOPIC_SHARDS", "3"))
PARTITIONS_DEFAULT = int(os.getenv("PARTITIONS_DEFAULT", "1"))
REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", "1"))
CREATE_CMD_TOPICS = os.getenv("CREATE_CMD_TOPICS", "true").lower() in ("1", "true", "yes", "on")

# DB proxy over Kafka
DB_REQ_TOPIC = os.getenv("DB_REQ_TOPIC", "db.register")
DB_RESP_TOPIC = os.getenv("DB_RESP_TOPIC", "db.register.responses")
DB_TIMEOUT_SEC = float(os.getenv("DB_TIMEOUT_SEC", "8"))
DB_WAIT_GROUP = os.getenv("DB_WAIT_GROUP", "register-service.dbwait")
DB_FALLBACK_TO_LOCAL = os.getenv("DB_FALLBACK_TO_LOCAL", "true").lower() in ("1","true","yes","on")

SECURITY_PROTOCOL = os.getenv("SECURITY_PROTOCOL")
SASL_MECHANISM = os.getenv("SASL_MECHANISM")
SASL_USERNAME = os.getenv("SASL_USERNAME")
SASL_PASSWORD = os.getenv("SASL_PASSWORD")

# ---------- Helpers ----------
VALID_TOPIC_RE = re.compile(r"^[a-zA-Z0-9._-]+$")

def sanitize(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]", "-", s.strip()) if s else ""

def is_valid_topic(name: str) -> bool:
    return bool(VALID_TOPIC_RE.match(name))

def now_utc_iso() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

def admin_client() -> AdminClient:
    conf = {"bootstrap.servers": BOOTSTRAP}
    if SECURITY_PROTOCOL: conf["security.protocol"] = SECURITY_PROTOCOL
    if SASL_MECHANISM: conf["sasl.mechanism"] = SASL_MECHANISM
    if SASL_USERNAME: conf["sasl.username"] = SASL_USERNAME
    if SASL_PASSWORD: conf["sasl.password"] = SASL_PASSWORD
    return AdminClient(conf)

def producer() -> Producer:
    conf = {"bootstrap.servers": BOOTSTRAP}
    if SECURITY_PROTOCOL: conf["security.protocol"] = SECURITY_PROTOCOL
    if SASL_MECHANISM: conf["sasl.mechanism"] = SASL_MECHANISM
    if SASL_USERNAME: conf["sasl.username"] = SASL_USERNAME
    if SASL_PASSWORD: conf["sasl.password"] = SASL_PASSWORD
    return Producer(conf)

def consumer(group_id: str, auto_offset_reset: str = "earliest") -> Consumer:
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group_id,
        "enable.auto.commit": True,
        "auto.offset.reset": auto_offset_reset,
    }
    if SECURITY_PROTOCOL: conf["security.protocol"] = SECURITY_PROTOCOL
    if SASL_MECHANISM: conf["sasl.mechanism"] = SASL_MECHANISM
    if SASL_USERNAME: conf["sasl.username"] = SASL_USERNAME
    if SASL_PASSWORD: conf["sasl.password"] = SASL_PASSWORD
    return Consumer(conf)

def ensure_cmd_topics(ac: AdminClient, cid: str):
    if not CREATE_CMD_TOPICS:
        return
    topics = []
    if CMD_SHARDS and CMD_SHARDS > 0:
        for i in range(CMD_SHARDS):
            t = f"{CMD_PREFIX}{cid}.p{i}"
            if is_valid_topic(t):
                topics.append(NewTopic(t, num_partitions=PARTITIONS_DEFAULT,
                                       replication_factor=REPLICATION_FACTOR,
                                       config={"cleanup.policy": "delete", "retention.ms": "86400000"}))
    else:
        t = f"{CMD_PREFIX}{cid}"
        if is_valid_topic(t):
            topics.append(NewTopic(t, num_partitions=PARTITIONS_DEFAULT,
                                   replication_factor=REPLICATION_FACTOR,
                                   config={"cleanup.policy": "delete", "retention.ms": "86400000"}))
    if not topics:
        return
    fs = ac.create_topics(topics, request_timeout=30)
    for t, f in fs.items():
        try:
            f.result()
            log.info("✔ created topic: %s", t)
        except Exception as e:
            msg = str(e)
            if "already exists" in msg or "TopicExists" in msg:
                log.info("= topic existed: %s", t)
            else:
                log.warning("⚠ create topic failed for %s: %s", t, e)

def send_status(p: Producer, client_id: str, status: str, meta: Dict, expires_at_iso: str):
    payload = {
        "schema": "ClientStatusV1",
        "client_id": client_id,
        "status": status,
        "meta": meta or {},
        "expires_at": expires_at_iso,
        "ts": now_utc_iso(),
    }
    p.produce(STATUS_TOPIC, key=client_id.encode("utf-8"),
              value=json.dumps(payload, ensure_ascii=False).encode("utf-8"))
    p.flush()

def send_register_response(p: Producer, corr_id: str, client_tmp_id: str,
                           client_id: str, auth_token: str, expires_at_iso: str):
    resp = {
        "schema": "ClientRegisterResponseV1",
        "client_id": client_id,
        "auth_token": auth_token,
        "expires_at": expires_at_iso,
        "reply_topic": REPLIES_TOPIC,
    }
    headers = [
        ("schema", "ClientRegisterResponseV1"),
        ("corr_id", corr_id),
        ("client_tmp_id", client_tmp_id),
        ("content_type", "application/json"),
        ("encoding", "utf-8"),
    ]
    p.produce(REGISTER_RESP_TOPIC,
              key=client_id.encode("utf-8"),
              value=json.dumps(resp, ensure_ascii=False).encode("utf-8"),
              headers=headers)
    p.flush()

def wait_db_response(corr_id: str, timeout: float) -> Optional[Dict]:
    c = consumer(DB_WAIT_GROUP, auto_offset_reset="earliest")
    c.subscribe([DB_RESP_TOPIC])
    t_end = time.time() + timeout
    try:
        while time.time() < t_end:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            hdrs = dict(msg.headers() or [])
            # هدرها ممکنه bytes باشن
            for k, v in list(hdrs.items()):
                if isinstance(v, (bytes, bytearray)):
                    try:
                        hdrs[k] = v.decode("utf-8")
                    except Exception:
                        hdrs[k] = str(v)
            if hdrs.get("corr_id") == corr_id:
                try:
                    return json.loads((msg.value() or b"").decode("utf-8", "ignore"))
                except Exception:
                    return None
        return None
    finally:
        try:
            c.close()
        except Exception:
            pass

# ---------- Timestamp utilities ----------

def _digits(n: int) -> int:
    n = abs(int(n))
    return len(str(n))

def to_iso_from_any(x: Any, fallback_days: int = 7) -> str:
    """تبدیل هر نوع زمان (ns/us/ms/s/ISO/pandas.Timestamp) به ISO8601 UTC"""
    # بدون مقدار → زمان آینده‌ی پیش‌فرض
    if x is None:
        return (datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(days=fallback_days)).isoformat()

    # اگر pandas داریم از آن استفاده کنیم (ns-safe)
    if _pd is not None:
        try:
            if isinstance(x, (int, float)):
                n = int(x)
                d = _digits(n)
                if d >= 19:
                    unit = "ns"
                elif d >= 16:
                    unit = "us"
                elif d >= 13:
                    unit = "ms"
                else:
                    unit = "s"
                ts = _pd.to_datetime(n, unit=unit, utc=True)
            else:
                ts = _pd.to_datetime(x, utc=True, errors="coerce")
            if ts is not None and not _pd.isna(ts):
                return ts.isoformat()
        except Exception:
            pass  # می‌افتیم به مسیر fallback پایین

    # fallback بدون pandas
    try:
        if isinstance(x, (int, float)):
            n = int(x)
            d = _digits(n)
            if d >= 19:
                seconds = n / 1_000_000_000  # ns
            elif d >= 16:
                seconds = n / 1_000_000      # us
            elif d >= 13:
                seconds = n / 1_000          # ms
            else:
                seconds = float(n)           # s
            dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
            return dt.isoformat()
        if isinstance(x, str):
            # ISO با یا بدون Z
            try:
                return datetime.fromisoformat(x.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
            except Exception:
                pass
    except Exception:
        pass

    # اگر هیچ‌کدام نشد
    return (datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(days=fallback_days)).isoformat()

def main():
    log.info("== Register Service (with db-handler) ==")
    log.info("BOOTSTRAP=%s | GROUP=%s | topics: %s / %s / DB %s→%s | shards=%s",
             BOOTSTRAP, GROUP_ID, REGISTER_TOPIC, REGISTER_RESP_TOPIC, DB_REQ_TOPIC, DB_RESP_TOPIC, CMD_SHARDS)

    ac = admin_client()
    try:
        md = ac.list_topics(timeout=10)
        log.info("✔ Kafka connected. brokers=%s", list(md.brokers.keys()))
    except Exception as e:
        raise SystemExit(f"Kafka not reachable: {e}")

    c = consumer(GROUP_ID, auto_offset_reset=os.getenv("REGISTER_AUTO_OFFSET_RESET", "earliest"))
    p = producer()
    c.subscribe([REGISTER_TOPIC])

    recent_corr_ids: Set[str] = set()
    MAX_RECENT = 2000

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.warning("consumer error: %s", msg.error())
                continue

            # headers → dict[str,str]
            hdrs = {}
            for k, v in (msg.headers() or []):
                try:
                    hdrs[k] = v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else str(v)
                except Exception:
                    hdrs[k] = str(v)

            corr_id = hdrs.get("corr_id") or str(uuid.uuid4())
            client_tmp_id = hdrs.get("client_tmp_id") or "unknown"

            try:
                payload = json.loads((msg.value() or b"").decode("utf-8", "ignore"))
            except Exception:
                payload = {}

            if corr_id in recent_corr_ids:
                log.info("~ duplicate corr_id, skip: %s", corr_id)
                continue

            # --- مرحله 1: درخواست به db-handler
            db_req_headers = [
                ("schema", "DbRegisterRequestV1"),
                ("op", "register"),
                ("corr_id", corr_id),
                ("client_tmp_id", client_tmp_id),
                ("content_type", "application/json"),
                ("encoding", "utf-8"),
            ]
            p.produce(DB_REQ_TOPIC,
                      key=(client_tmp_id or "").encode("utf-8"),
                      value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                      headers=db_req_headers)
            p.flush()

            # --- مرحله 2: منتظر پاسخ DB
            db_resp = wait_db_response(corr_id, DB_TIMEOUT_SEC)

            # خروجی‌های احتمالی db_resp:
            #   { "status": "ok", "client_id": "...", "auth_token": "...", "expires_at": <ns/us/ms/s/iso/ts> }
            #   { "status": "error", "error": "..." }
            use_fallback = False
            if not db_resp:
                log.warning("DB response timeout for corr_id=%s", corr_id)
                use_fallback = True
            elif db_resp.get("status") != "ok":
                err = db_resp.get("error") or "unknown db error"
                log.error("DB error (corr_id=%s): %s", corr_id, err)
                use_fallback = DB_FALLBACK_TO_LOCAL
            else:
                use_fallback = False

            if use_fallback:
                if not DB_FALLBACK_TO_LOCAL:
                    # بدون fallback اجازه نداریم ادامه دهیم
                    log.error("DB response not usable and fallback disabled; aborting registration.")
                    continue
                # fallback: client_id موقتی بر اساس meta
                meta = payload.get("meta") or {}
                identity = meta.get("client_id") or meta.get("uuid") or meta.get("serial") or client_tmp_id
                identity = sanitize(identity)
                client_id = f"client-{identity}" if not str(identity).startswith("client-") else identity
                auth_token = secrets.token_urlsafe(32)
                exp_iso = to_iso_from_any(None)  # 7 روز پیش‌فرض
                log.warning("DB fallback → client_id=%s", client_id)
            else:
                client_id = sanitize(db_resp.get("client_id") or "")
                if not client_id:
                    if DB_FALLBACK_TO_LOCAL:
                        log.error("DB response missing client_id; switching to fallback.")
                        meta = payload.get("meta") or {}
                        identity = meta.get("client_id") or meta.get("uuid") or meta.get("serial") or client_tmp_id
                        identity = sanitize(identity)
                        client_id = f"client-{identity}" if not str(identity).startswith("client-") else identity
                        use_fallback = True
                    else:
                        log.error("DB response missing client_id and fallback disabled; abort.")
                        continue

                auth_token = db_resp.get("auth_token") or secrets.token_urlsafe(32)
                exp_iso = to_iso_from_any(db_resp.get("expires_at"))  # ← تبدیل امن هر نوع timestamp

            # --- مرحله 3: ساخت تاپیک‌های cmd.<client_id>.* و ارسال پاسخ
            ensure_cmd_topics(ac, client_id)
            send_register_response(p, corr_id, client_tmp_id, client_id, auth_token, exp_iso)
            send_status(p, client_id, "registered", payload.get("meta") or {}, exp_iso)

            recent_corr_ids.add(corr_id)
            if len(recent_corr_ids) > MAX_RECENT:
                # set.pop() یکی از اعضا را حذف می‌کند (ترتیب ندارد)؛ برای ما کافی است
                recent_corr_ids.pop()
            log.info("✔ registered client_id=%s (tmp=%s) corr_id=%s", client_id, client_tmp_id, corr_id)

    except KeyboardInterrupt:
        log.warning("Interrupted by user.")
    finally:
        try:
            c.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
