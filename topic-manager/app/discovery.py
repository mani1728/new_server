# ------------------------------
#   ایمپورت‌ها
# ------------------------------
import json
import os
import time
import uuid
import re
from typing import Set, List, Optional, Tuple
from confluent_kafka import KafkaException
from utils import log, env_bool, is_canonical_client_id, sanitize_topic_component  # از utils: log, env_bool, validatorها
from kafka_clients import get_consumer, get_producer  # از kafka_clients: consumer و producer

# ------------------------------
#   پارس client_id از پیام
# ------------------------------
def parse_client_id_from_msg(value: bytes, headers: Optional[List[Tuple[str, bytes]]]) -> Tuple[Optional[str], Optional[str]]:
    """پارس client_id از payload JSON یا headers (اولویت: client_id واقعی، سپس tmp)"""
    try:
        if value:
            j = json.loads(value.decode("utf-8", "ignore"))
            # 1) پاسخ‌ها معمولاً client_id دارند
            cid = j.get("client_id")
            if isinstance(cid, str) and is_canonical_client_id(cid):
                return cid, None
            # 2) درخواستی‌ها: شاید meta.client_id داشته باشند
            meta = j.get("meta") or {}
            if isinstance(meta, dict):
                m_cid = meta.get("client_id")
                if isinstance(m_cid, str) and is_canonical_client_id(m_cid):
                    return m_cid, None
            # 3) آخرین انتخاب (کم‌اولویت): client_tmp_id → فقط برای fallback
            tmp = j.get("client_tmp_id")
            if isinstance(tmp, str) and is_canonical_client_id(tmp):
                return None, tmp
    except Exception:
        pass
    # fallback از headers
    if headers:
        for k, v in headers:
            if isinstance(v, bytes):
                v = v.decode("utf-8", "ignore")
            if k == "client_id" and isinstance(v, str) and is_canonical_client_id(v):
                return v, None
    return None, None

# ------------------------------
#   اکتشاف از register
# ------------------------------
def discover_from_register() -> List[str]:
    """اکتشاف از پاسخ‌های رجیستر (ایمن‌ترین منبع client_id قطعی)"""
    resp_topic = os.getenv("DISCOVERY_REGISTER_RESPONSES_TOPIC", "clients.register.responses")
    topics = [resp_topic]
    duration = int(os.getenv("DISCOVERY_DURATION_SEC", "15"))
    earliest = env_bool("DISCOVERY_FROM_BEGINNING", True)
    group = os.getenv("DISCOVERY_GROUP_ID", "topic-manager.discovery")

    c = get_consumer(group, earliest)
    found: Set[str] = set()
    try:
        c.subscribe(topics)
        log.info("🔎 Discovery: subscribing %r for %ss (from %s)", topics, duration, "earliest" if earliest else "latest")
        t0 = time.time()
        MAX_RESULTS = int(os.getenv("DISCOVERY_MAX_RESULTS", "1000"))
        while time.time() - t0 < duration and len(found) < MAX_RESULTS:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            cid, _ = parse_client_id_from_msg(msg.value(), msg.headers())
            if cid and is_canonical_client_id(cid):
                safe = sanitize_topic_component(cid, max_len=100)
                if safe and safe not in found:
                    log.info("➕ کشف client_id: %s", safe)
                    found.add(safe)
        return sorted(list(found))
    finally:
        try:
            c.close()
        except:
            pass

# ------------------------------
#   اکتشاف از DB list
# ------------------------------
def discover_from_db_list() -> List[str]:
    """
    درخواست به db-handler برای لیست کلاینت‌ها:
    - درخواست روی DB_LIST_REQ_TOPIC با corr_id
    - پاسخ روی DB_LIST_RESP_TOPIC شامل آرایه‌ای از client_id
    فرمت پیشنهادی پاسخ:
      {"schema":"DbClientsListV1","clients":[{"client_id":"client-..."}]}
    """
    if not env_bool("DB_LIST_ENABLE", False):
        return []
    req_topic = os.getenv("DB_LIST_REQ_TOPIC", "db.clients.list")
    resp_topic = os.getenv("DB_LIST_RESP_TOPIC", "db.clients.list.responses")
    timeout_s = int(os.getenv("DB_LIST_TIMEOUT_SEC", "8"))
    p = get_producer()
    c = get_consumer(group_id="topic-manager.db-list", earliest=True)
    corr_id = str(uuid.uuid4())
    headers = [
        ("schema", "DbClientsListRequestV1"),
        ("corr_id", corr_id),
        ("content_type", "application/json"),
        ("encoding", "utf-8")
    ]
    try:
        p.produce(req_topic, key=b"topic-manager", value=b"{}", headers=headers)
        log.info("درخواست db-list فرستاده شد. corr_id=%s به %s", corr_id, req_topic)
        p.flush(timeout_s / 2)
    except Exception as e:
        log.warning("⚠ ارسال درخواست db-list ناموفق: %s", e)
        return []
    found: Set[str] = set()
    deadline = time.time() + timeout_s
    try:
        c.subscribe([resp_topic])
        while time.time() < deadline:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.debug("consumer error (db-list): %s", msg.error())
                continue
            log.debug("پیام دریافت شد از %s: key=%s, headers=%s",
                      msg.topic(), msg.key(), msg.headers() or [])
            # پارس headers
            hdrs = {}
            if msg.headers():
                for k, v in msg.headers():
                    if isinstance(v, bytes):
                        v = v.decode("utf-8", "ignore")
                    hdrs[k] = v
            if hdrs.get("corr_id") != corr_id:
                log.debug("corr_id match نکرد: %s != %s", hdrs.get("corr_id"), corr_id)
                continue
            try:
                payload = json.loads((msg.value() or b"").decode("utf-8", "ignore"))
                clients = payload.get("clients") or []
                log.debug("پاسخ پارس شد: clients=%s", clients)
                for it in clients:
                    if isinstance(it, str):
                        cid = it.strip()  # اگر string array بود، مستقیم استفاده کن
                    elif isinstance(it, dict):
                        cid = it.get("client_id")
                    else:
                        cid = None
                    if isinstance(cid, str) and is_canonical_client_id(cid):
                        found.add(cid)
                        log.debug("client_id اضافه شد: %s", cid)  # لاگ اضافی برای تایید
                    else:
                        log.debug("client_id رد شد: %s (طول=%d, canonical=%s)", cid, len(cid) if cid else 0, is_canonical_client_id(cid) if cid else False)
                break
            except Exception as e:
                log.debug("خطا در پارس payload: %s", e)
                continue
    finally:
        try:
            c.close()
        except:
            pass
    if found:
        log.info("🔎 Discovery(db): %d client(s) از DB دریافت شد.", len(found))
    return sorted(list(found))

# ------------------------------
#   اکتشاف از فایل
# ------------------------------
def discover_from_file() -> List[str]:
    """اکتشاف client_id ها از یک فایل متنی (هر خط یک id)"""
    path = os.getenv("DISCOVERY_FILE_PATH", "/state/clients.txt")
    try:
        with open(path, "r", encoding="utf-8") as f:
            ids = []
            for line in f:
                s = line.strip()
                if is_canonical_client_id(s):
                    ids.append(sanitize_topic_component(s, max_len=100))
            ids = [i for i in ids if i]
            if ids:
                log.info("🔎 Discovery(file): %d id از %s", len(ids), path)
            return sorted(list(set(ids)))
    except FileNotFoundError:
        log.debug("فایل discovery یافت نشد: %s", path)
        return []
    except Exception as e:
        log.warning("⚠ خواندن فایل discovery خطا داد: %s", e)
        return []

# ------------------------------
#   اجرای اکتشاف
# ------------------------------
def run_discovery() -> List[str]:
    """
    اجرای اکتشاف مطابق ENV:
    - DISCOVERY_MODE = register|db|file|none
    """
    mode = os.getenv("DISCOVERY_MODE", "none").strip().lower()
    if mode == "register":
        return discover_from_register()
    if mode == "db":
        return discover_from_db_list()
    if mode == "file":
        return discover_from_file()
    return []

# ------------------------------
#   کش client_id ها
# ------------------------------
def load_known_clients(path: str) -> Set[str]:
    """بارگیری client_idهای شناخته‌شده از فایل کش"""
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception as e:
            log.warning("⚠ بارگیری کش ناموفق: %s", e)
    return set()

def save_known_clients(path: str, known: Set[str]):
    """ذخیره client_idهای شناخته‌شده در فایل کش"""
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(list(known), f, indent=2)
        log.debug("✔ کش ذخیره شد: %s", path)
    except Exception as e:
        log.warning("⚠ ذخیرهٔ کش ناموفق: %s", e)