#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, json, logging, re, secrets, uuid, unicodedata, time, traceback
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple, List

import requests
from confluent_kafka import Consumer, Producer, KafkaException
from influxdb_client_3 import InfluxDBClient3, Point

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("influx-handler")

# Kafka
BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID    = os.getenv("DB_HANDLER_GROUP_ID", "db-handler")
AUTO_OFFSET = os.getenv("DB_CONSUMER_OFFSET", "earliest")
REQ_TOPIC_REG   = os.getenv("DB_REQ_TOPIC", "db.register")
RESP_TOPIC_REG  = os.getenv("DB_RESP_TOPIC", "db.register.responses")
REQ_TOPIC_LIST  = os.getenv("DB_LIST_REQ_TOPIC", "db.clients.list")
RESP_TOPIC_LIST = os.getenv("DB_LIST_RESP_TOPIC", "db.clients.list.responses")
SECURITY_PROTOCOL = os.getenv("SECURITY_PROTOCOL")
SASL_MECHANISM    = os.getenv("SASL_MECHANISM")
SASL_USERNAME     = os.getenv("SASL_USERNAME")
SASL_PASSWORD     = os.getenv("SASL_PASSWORD")

# Influx v3
INFLUX_URL      = os.getenv("INFLUX_URL", "http://influx-db:8181")
INFLUX_TOKEN    = os.getenv("INFLUX_TOKEN")
INFLUX_DATABASE = os.getenv("INFLUX_DATABASE", "my_database")
# جدول/measurement هدف (طبق تصویرت)
ACCOUNT_TABLE   = os.getenv("ACCOUNT_TABLE", "account")

# Scheme پیشنهادی جدول account:
#   tags : client_id, tenant
#   fields: identity (string), auth_token (string), meta_json (string)
ACCOUNT_TAGS    = [s.strip() for s in os.getenv("ACCOUNT_TAGS","client_id,tenant").split(",") if s.strip()]
# --- جایگزین این بلوک کن ---
DEFAULT_ACCOUNT_FIELDS = [
    {"name": "identity",   "type": "string"},
    {"name": "auth_token", "type": "string"},
    {"name": "meta_json",  "type": "string"},
]

_raw = os.getenv("ACCOUNT_FIELDS_JSON", "").strip()
if _raw:
    try:
        ACCOUNT_FIELDS = json.loads(_raw)
    except Exception as e:
        log.warning("Bad ACCOUNT_FIELDS_JSON → using default. err=%s raw=%r", e, _raw)
        ACCOUNT_FIELDS = DEFAULT_ACCOUNT_FIELDS
else:
    ACCOUNT_FIELDS = DEFAULT_ACCOUNT_FIELDS

# قوانین identity/client_id (مثل سرویس قدیمی)
IDENTITY_KEYS = [k.strip() for k in os.getenv(
    "IDENTITY_KEYS",
    "meta.client_id,meta.uuid,meta.serial,meta.hostname,header.client_id,header.uuid,header.serial,client_tmp_id"
).split(",") if k.strip()]
ID_PREFIX         = os.getenv("ID_PREFIX", "client-")
ID_REQUIRE_PREFIX = os.getenv("ID_REQUIRE_PREFIX", "true").lower() in ("1","true","yes","on")
ID_MAX_LEN        = int(os.getenv("ID_MAX_LEN", "64"))
NORMALIZE_LOWER   = os.getenv("NORMALIZE_LOWER", "true").lower() in ("1","true","yes","on")
DISALLOWED_IDS    = set([s.strip() for s in os.getenv("DISALLOWED_IDS", "").split(",") if s.strip()])
TENANT_ENABLED    = os.getenv("TENANT_ENABLED", "false").lower() in ("1","true","yes","on")
TENANT_KEYS       = [k.strip() for k in os.getenv("TENANT_KEYS", "meta.tenant,header.tenant").split(",") if k.strip()]
TENANT_DEFAULT    = os.getenv("TENANT_DEFAULT", "default")
TENANT_IN_ID      = os.getenv("TENANT_IN_ID", "false").lower() in ("1","true","yes","on")
IDENTITY_UNIQUE   = os.getenv("IDENTITY_UNIQUE", "false").lower() in ("1","true","yes","on")

VALID_TOPIC_RE = re.compile(r"^[a-zA-Z0-9._-]+$")
WS_RE          = re.compile(r"\s+")
NON_ALLOWED_RE = re.compile(r"[^a-zA-Z0-9._-]+")
DIGIT_MAP      = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")

def _security_conf(conf: dict) -> dict:
    if SECURITY_PROTOCOL: conf["security.protocol"] = SECURITY_PROTOCOL
    if SASL_MECHANISM:    conf["sasl.mechanism"]    = SASL_MECHANISM
    if SASL_USERNAME:     conf["sasl.username"]     = SASL_USERNAME
    if SASL_PASSWORD:     conf["sasl.password"]     = SASL_PASSWORD
    return conf

def build_consumer() -> Consumer:
    conf = _security_conf({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "enable.auto.commit": True,
        "auto.offset.reset": AUTO_OFFSET,
    })
    return Consumer(conf)

def build_producer() -> Producer:
    conf = _security_conf({"bootstrap.servers": BOOTSTRAP})
    return Producer(conf)

def normalize_ascii(s: str) -> str:
    if s is None: return ""
    s = str(s)
    import unicodedata, re
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.translate(DIGIT_MAP)
    s = WS_RE.sub("-", s.strip())
    if NORMALIZE_LOWER: s = s.lower()
    s = NON_ALLOWED_RE.sub("-", s)
    s = re.sub(r"[-]{2,}", "-", s)
    s = re.sub(r"[.]{2,}", ".", s)
    s = re.sub(r"[_]{2,}", "_", s)
    s = s.strip("._-")
    return s

def is_valid_topic(name: str) -> bool:
    return bool(VALID_TOPIC_RE.match(name)) and len(name) > 0

def get_nested(d: Dict, path: str) -> Optional[str]:
    cur = d
    for p in path.split("."):
        if not isinstance(cur, dict) or p not in cur: return None
        cur = cur[p]
    return str(cur) if isinstance(cur, (str,int,float)) else None

def resolve_identity(body: Dict, headers: Dict[str, str], client_tmp_id: str) -> Tuple[str, str]:
    bag = {"meta": body.get("meta") or {}, "header": headers or {}, "client_tmp_id": client_tmp_id}
    identity_raw = None
    for key in IDENTITY_KEYS:
        if key == "client_tmp_id": identity_raw = client_tmp_id
        else:
            val = get_nested(bag, key)
            if val: identity_raw = val
        if identity_raw: break
    if not identity_raw: identity_raw = str(uuid.uuid4())[:8]
    tenant = TENANT_DEFAULT
    if TENANT_ENABLED:
        for k in TENANT_KEYS:
            v = get_nested(bag, k)
            if v:
                tenant = normalize_ascii(v) or TENANT_DEFAULT
                break
    return tenant, identity_raw

def canonical_client_id(tenant: str, identity_raw: str) -> str:
    ident = normalize_ascii(identity_raw)
    if ID_REQUIRE_PREFIX and ID_PREFIX and not ident.startswith(ID_PREFIX):
        ident = f"{ID_PREFIX}{ident}"
    if TENANT_ENABLED and TENANT_IN_ID:
        if not ident.startswith(f"{ID_PREFIX}{tenant}-"):
            ident = f"{ID_PREFIX}{tenant}-{ident[len(ID_PREFIX):]}" if ID_PREFIX else f"{tenant}-{ident}"
    if len(ident) > ID_MAX_LEN: ident = ident[:ID_MAX_LEN]
    if not is_valid_topic(ident) or ident in DISALLOWED_IDS or not ident:
        ident = f"{ID_PREFIX}unknown" if ID_PREFIX else "unknown"
    return ident

def mk_token() -> str:
    import secrets
    return secrets.token_urlsafe(32)

def coerce_meta(meta_val) -> Dict:
    if isinstance(meta_val, dict): return meta_val
    if isinstance(meta_val, str):
        try:
            x = json.loads(meta_val)
            return x if isinstance(x, dict) else {}
        except Exception:
            return {}
    return {}

# ---------- Influx Repo ----------
class InfluxRepo:
    def __init__(self, host: str, database: str, token: str):
        self.client = InfluxDBClient3(host=host, database=database, token=token)

    def query_one(self, sql: str):
        tbl = self.client.query(sql)
        if tbl is None or tbl.num_rows == 0: return None
        cols = tbl.column_names
        row0 = {cols[i]: tbl.column(i)[0].as_py() for i in range(len(cols))}
        return row0

    def list_tables(self) -> List[str]:
        tbl = self.client.query("SHOW TABLES")
        if tbl is None or tbl.num_rows == 0: return []
        return [v.as_py() for v in tbl.column(tbl.schema.get_field_index("table_name"))]

    def latest_account(self, client_id: str):
        sql = f'SELECT * FROM "{ACCOUNT_TABLE}" WHERE client_id = \'{client_id}\' ORDER BY time DESC LIMIT 1'
        return self.query_one(sql)

    def first_time_account(self, client_id: str):
        sql = f'SELECT MIN(time) AS created_at FROM "{ACCOUNT_TABLE}" WHERE client_id = \'{client_id}\''
        row = self.query_one(sql)
        return row.get("created_at") if row else None

    def find_by_identity(self, identity: str):
        sql = f'SELECT client_id FROM "{ACCOUNT_TABLE}" WHERE identity = \'{identity}\' ORDER BY time DESC LIMIT 1'
        return self.query_one(sql)

    def write_account(self, client_id: str, tenant: Optional[str], identity: str, meta: Dict, auth_token: str):
        p = Point(ACCOUNT_TABLE) \
            .tag("client_id", client_id) \
            .tag("tenant", tenant if tenant else TENANT_DEFAULT) \
            .field("identity", identity) \
            .field("auth_token", auth_token) \
            .field("meta_json", json.dumps(meta, ensure_ascii=False))
        self.client.write(record=p)

# ---------- Handlers ----------
def handle_register(repo: InfluxRepo, body: Dict, headers: Dict[str, str], corr_id: str, client_tmp_id: str) -> Dict:
    # هویت
    tenant, identity_raw = resolve_identity(body, headers, client_tmp_id)
    identity = normalize_ascii(identity_raw)
    client_id = canonical_client_id(tenant, identity_raw)
    meta = coerce_meta(body.get("meta"))

    # یونیک بودن identity (اختیاری)
    if IDENTITY_UNIQUE:
        other = repo.find_by_identity(identity)
        if other and other.get("client_id") and other["client_id"] != client_id:
            return {"schema":"DbRegisterResponseV1","status":"error","error":"identity already in use","client_id":client_id}

    prev = repo.latest_account(client_id)
    inserted = (prev is None)
    auth_token = (prev.get("auth_token") if prev and "auth_token" in prev else mk_token())

    repo.write_account(client_id=client_id, tenant=(tenant if TENANT_ENABLED else None),
                       identity=identity, meta=meta, auth_token=auth_token)

    created_at = repo.first_time_account(client_id)
    updated_at = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

    return {
        "schema": "DbRegisterResponseV1",
        "status": "ok",
        "exists": (not inserted),
        "client_id": client_id,
        "auth_token": auth_token,
        "profile": {
            "identity": identity,
            "tenant": (tenant if TENANT_ENABLED else None),
            "meta": meta,
            "created_at": (created_at.isoformat() if hasattr(created_at,"isoformat") else str(created_at) if created_at else None),
            "updated_at": updated_at,
        },
    }

def handle_list_clients(repo: InfluxRepo, _body: Dict, _headers: Dict[str, str], _corr_id: str) -> Dict:
    # SHOW DISTINCT client_id ها
    tbl = repo.client.query(f'SELECT DISTINCT client_id FROM "{ACCOUNT_TABLE}"')
    ids = []
    if tbl and tbl.num_rows:
        ids = [x.as_py() for x in tbl.column(tbl.schema.get_field_index("client_id")) if isinstance(x.as_py(), str)]
    return {"schema": "DbClientsListV1", "status": "ok", "count": len(ids), "clients": sorted(ids)}

# ---------- Bootstrap: ensure database & table ----------
def ensure_database_and_table():
    if not INFLUX_TOKEN:
        log.error("INFLUX_TOKEN is missing")
        raise SystemExit(1)

    from influxdb_client_3 import InfluxDBClient3
    import requests, json, time

    def _show_tables():
        cli = InfluxDBClient3(host=INFLUX_URL, database=INFLUX_DATABASE, token=INFLUX_TOKEN)
        tbl = cli.query("SHOW TABLES")
        names = []
        if tbl and tbl.num_rows:
            idx = tbl.schema.get_field_index("table_name")
            if idx == -1 and len(tbl.column_names) >= 3:
                idx = 2
            if idx != -1:
                names = [x.as_py() for x in tbl.column(idx)]
        return names

    # 1) DB check
    db_ok = False
    try:
        _ = _show_tables()
        db_ok = True
    except Exception as e:
        log.info("[bootstrap] DB check failed (%s) → will create DB", e)

    # 2) create DB (prefer API; fallback write_lp with precision=nanosecond)
    if not db_ok:
        try:
            r = requests.post(
                f"{INFLUX_URL}/api/v3/configure/database",
                headers={"Authorization": f"Bearer {INFLUX_TOKEN}", "Content-Type": "application/json"},
                data=json.dumps({"db": INFLUX_DATABASE}).encode("utf-8"), timeout=5
            )
            if r.status_code in (200, 201, 204):
                log.info("[bootstrap] database '%s' created via configure/database", INFLUX_DATABASE)
            else:
                log.warning("[bootstrap] configure/database returned %s → fallback to write_lp", r.status_code)
                lp = f'bootstrap,component=influx-handler alive=1i {int(time.time()*1e9)}'
                r2 = requests.post(
                    f"{INFLUX_URL}/api/v3/write_lp",
                    headers={"Authorization": f"Bearer {INFLUX_TOKEN}", "Content-Type": "text/plain"},
                    params={"db": INFLUX_DATABASE, "precision": "nanosecond"},
                    data=lp.encode("utf-8"), timeout=5
                )
                if r2.status_code not in (200, 204):
                    log.error("write_lp failed for db create: %s %s", r2.status_code, r2.text)
                    raise SystemExit(1)
        except Exception as e:
            log.error("DB create failed: %s", e)
            raise
        time.sleep(0.5)

    # 3) table check
    exists = False
    try:
        table_names = _show_tables()
        exists = ACCOUNT_TABLE in set(table_names)
    except Exception as e:
        log.warning("SHOW TABLES after DB-create failed: %s", e)

    # 4) create table (prefer API; fallback first write)
    if not exists:
        log.info("[bootstrap] creating table '%s'", ACCOUNT_TABLE)
        payload = {"db": INFLUX_DATABASE, "table": ACCOUNT_TABLE, "tags": ACCOUNT_TAGS, "fields": ACCOUNT_FIELDS}
        r2 = requests.post(
            f"{INFLUX_URL}/api/v3/configure/table",
            headers={"Authorization": f"Bearer {INFLUX_TOKEN}", "Content-Type": "application/json"},
            data=json.dumps(payload).encode("utf-8"), timeout=5
        )
        if r2.status_code not in (200, 201, 204):
            log.warning("configure/table failed (%s) → fallback to auto-create by first write", r2.status_code)
            lp = (
                f'{ACCOUNT_TABLE},client_id=bootstrap,tenant={TENANT_DEFAULT} '
                f'identity="auto",auth_token="auto",meta_json="{{}}" {int(time.time()*1e9)}'
            )
            r3 = requests.post(
                f"{INFLUX_URL}/api/v3/write_lp",
                headers={"Authorization": f"Bearer {INFLUX_TOKEN}", "Content-Type": "text/plain"},
                params={"db": INFLUX_DATABASE, "precision": "nanosecond"},
                data=lp.encode("utf-8"), timeout=5
            )
            if r3.status_code not in (200, 204):
                log.error("write_lp fallback failed: %s %s", r3.status_code, r3.text)
                raise SystemExit(1)

# ---------- Main loop ----------
def run():
    log.info("== influx-handler ==")
    log.info("Kafka=%s | Group=%s | REQ: %s , %s | RESP: %s , %s",
             BOOTSTRAP, GROUP_ID, REQ_TOPIC_REG, REQ_TOPIC_LIST, RESP_TOPIC_REG, RESP_TOPIC_LIST)
    log.info("InfluxDB=%s [%s] table=%s", INFLUX_URL, INFLUX_DATABASE, ACCOUNT_TABLE)
    ensure_database_and_table()

    repo = InfluxRepo(host=INFLUX_URL, database=INFLUX_DATABASE, token=INFLUX_TOKEN)
    p = build_producer(); c = build_consumer()
    c.subscribe([REQ_TOPIC_REG, REQ_TOPIC_LIST])

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None: continue
            if msg.error():
                log.warning("consumer error: %s", msg.error()); continue

            hdrs: Dict[str,str] = {}
            for k, v in (msg.headers() or []):
                try:
                    k_s = k.decode("utf-8") if isinstance(k,(bytes,bytearray)) else str(k)
                    v_s = v.decode("utf-8") if isinstance(v,(bytes,bytearray)) else str(v)
                    hdrs[k_s] = v_s
                except Exception: pass

            corr_id = hdrs.get("corr_id") or str(uuid.uuid4())
            client_tmp_id = hdrs.get("client_tmp_id") or ""

            try:
                body = json.loads((msg.value() or b"").decode("utf-8","ignore"))
            except Exception:
                body = {}

            if msg.topic() == REQ_TOPIC_REG:
                try:
                    out = handle_register(repo, body, hdrs, corr_id, client_tmp_id)
                    resp_topic = RESP_TOPIC_REG
                except Exception as e:
                    log.exception("register failed")
                    out = {"schema":"DbError","status":"error","error":str(e),"client_id":""}
                    resp_topic = RESP_TOPIC_REG

            elif msg.topic() == REQ_TOPIC_LIST:
                try:
                    out = handle_list_clients(repo, body, hdrs, corr_id)
                    resp_topic = RESP_TOPIC_LIST
                except Exception as e:
                    log.exception("list failed")
                    out = {"schema":"DbError","status":"error","error":str(e)}
                    resp_topic = RESP_TOPIC_LIST
            else:
                continue

            headers = [
                ("schema", out.get("schema","DbResponse")),
                ("corr_id", corr_id),
                ("client_tmp_id", client_tmp_id),
                ("content_type","application/json"),
                ("encoding","utf-8"),
                ("status", out.get("status","ok")),
            ]
            key = (out.get("client_id") or "").encode("utf-8")
            try:
                p.produce(resp_topic, key=key,
                          value=json.dumps(out, ensure_ascii=False).encode("utf-8"),
                          headers=headers)
                p.poll(0)
            except KafkaException as e:
                log.warning("produce failed: %s", e)

    except KeyboardInterrupt:
        log.warning("Interrupted.")
    except Exception as e:
        log.error("fatal: %s", e); traceback.print_exc(); raise
    finally:
        try: c.close()
        except Exception: pass

if __name__ == "__main__":
    run()
