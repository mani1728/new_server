# ------------------------------
#   ایمپورت‌ها
# ------------------------------
import json, os
from typing import Dict, List, Tuple
from confluent_kafka import KafkaException
from confluent_kafka.admin import NewTopic, NewPartitions, ConfigResource, ResourceType
from utils import log, env_bool, is_valid_topic_name, sanitize_topic_component  # از utils: log و validatorها
from kafka_clients import AdminClient  # از kafka_clients: AdminClient

# ------------------------------
#   ابزارهای مدیریت تاپیک (بدون تغییر از کد اصلی)
# ------------------------------
def topic_exists(ac: AdminClient, name: str) -> Tuple[bool, int]:
    """بررسی وجود تاپیک + تعداد پارتیشن فعلی"""
    md = ac.list_topics(timeout=10)
    if name in md.topics:
        parts = md.topics[name].partitions or {}
        return True, len(parts)
    return False, 0

def list_topics(ac: AdminClient) -> List[str]:
    """لیست تمام تاپیک‌ها برای حذفِ احتمالیِ اضافه‌ها"""
    md = ac.list_topics(timeout=10)
    return sorted(md.topics.keys())

def ensure_partitions(ac: AdminClient, name: str, desired: int, dry: bool, timeout: int):
    """اگر پارتیشن‌ها کمتر از مقدار مطلوب بود، افزایش بده (کاهش امکان‌پذیر نیست)"""
    if not desired or desired <= 0:
        return
    exists, current = topic_exists(ac, name)
    if not exists or current >= desired:
        # اگر تاپیک نیست یا پارتیشن کافی است، کاری نکن
        return
    log.info("↑ افزایش پارتیشن‌های %s: %d → %d", name, current, desired)
    if dry:
        log.info("DRY_RUN: create_partitions اجرا نشد.")
        return
    fs = ac.create_partitions([NewPartitions(name, desired)], request_timeout=timeout)
    try:
        fs[name].result()
        log.info("✔ پارتیشن‌های %s به %d رسید.", name, desired)
    except Exception as e:
        log.warning("⚠ create_partitions(%s) خطا: %s", name, e)

def describe_topic_configs(ac: AdminClient, name: str) -> Dict[str, str]:
    """گرفتن تنظیمات فعلی تاپیک از کافکا"""
    cr = ConfigResource(ResourceType.TOPIC, name)
    futures = ac.describe_configs([cr])
    res = futures[cr].result(timeout=30)
    return {k: v.value for k, v in res.items()}

def ensure_configs(ac: AdminClient, name: str, want: Dict[str, str], dry: bool, timeout: int):
    """به‌روزرسانی تنظیمات تاپیک در صورت نابرابری با وضعیت مطلوب"""
    if not want:
        return
    try:
        current = describe_topic_configs(ac, name)
    except KafkaException as e:
        log.warning("⚠ describe_configs(%s) ناموفق: %s", name, e)
        return

    patch: Dict[str, str] = {}
    for k, want_val in want.items():
        cur_val = current.get(k)
        if cur_val != str(want_val):
            patch[k] = str(want_val)

    if not patch:
        return  # نیازی به تغییر نیست

    log.info("⚙ به‌روزرسانی کانفیگ‌های %s: %s", name, patch)
    if dry:
        log.info("DRY_RUN: alter_configs اجرا نشد.")
        return

    cr_set = ConfigResource(ResourceType.TOPIC, name, set_config=patch)
    futures = ac.alter_configs([cr_set], request_timeout=timeout)
    try:
        futures[cr_set].result()
        log.info("✔ کانفیگ‌های %s به‌روزرسانی شد.", name)
    except Exception as e:
        log.warning("⚠ alter_configs(%s) خطا: %s", name, e)

# ------------------------------
#   ساخت specs از ENV (پیاده‌سازی بر اساس توضیحات کد اصلی؛ بدون تغییر در منطق)
# ------------------------------
def build_specs_from_env(discovered_ids: List[str]) -> List[Dict]:
    """ساخت لیست specs تاپیک‌ها از ENV: TOPICS_JSON + تاپیک‌های عمومی + per-client"""
    specs = []

    # 1. پارس TOPICS_JSON (تاپیک‌های ثابت مثل db.register)
    topics_json = os.getenv("TOPICS_JSON", "")
    if topics_json:
        try:
            json_topics = json.loads(topics_json)
            for t in json_topics:
                name = t.get("name")
                if is_valid_topic_name(name):
                    spec = {
                        "name": name,
                        "partitions": t.get("partitions", int(os.getenv("PARTITIONS_DEFAULT", "1"))),
                        "replication_factor": t.get("replication_factor", int(os.getenv("REPLICATION_FACTOR", "1"))),
                        "config": t.get("config", {})
                    }
                    specs.append(spec)
                    log.debug("از TOPICS_JSON: %s", name)
        except json.JSONDecodeError as e:
            log.warning("⚠ پارس TOPICS_JSON ناموفق: %s", e)

    # 2. تاپیک‌های عمومی (مثل server.replies)
    if os.getenv("REPLIES_TOPIC"):
        spec = {
            "name": os.getenv("REPLIES_TOPIC"),
            "partitions": int(os.getenv("REPLIES_PARTITIONS", "1")),
            "replication_factor": int(os.getenv("REPLICATION_FACTOR", "1")),
            "config": {
                "retention.ms": os.getenv("REPLIES_RETENTION_MS", "604800000"),
                "min.insync.replicas": os.getenv("REPLIES_MIN_ISR", "1"),
                "cleanup.policy": os.getenv("REPLIES_CLEANUP_POLICY", "delete")
            }
        }
        specs.append(spec)
        log.debug("تاپیک عمومی replies: %s", spec["name"])

    if env_bool("ENABLE_CLIENT_TOPICS", True):
        # clients.register
        specs.append({
            "name": "clients.register",
            "partitions": int(os.getenv("PARTITIONS_DEFAULT", "1")),
            "replication_factor": int(os.getenv("REPLICATION_FACTOR", "1")),
            "config": {}
        })
        # clients.status (compact اگر فعال)
        config_status = {"cleanup.policy": "compact"} if env_bool("CLIENTS_STATUS_COMPACT", True) else {}
        specs.append({
            "name": "clients.status",
            "partitions": int(os.getenv("PARTITIONS_DEFAULT", "1")),
            "replication_factor": int(os.getenv("REPLICATION_FACTOR", "1")),
            "config": config_status
        })

    # 3. تاپیک‌های per-client (cmd.<client> یا sharded cmd.<client>.pN)
    shards = int(os.getenv("CLIENT_TOPIC_SHARDS", "0"))
    prefix = os.getenv("CMD_TOPIC_PREFIX", "cmd.")
    ids = discovered_ids or []
    if os.getenv("CLIENT_IDS"):
        ids += [cid.strip() for cid in os.getenv("CLIENT_IDS").split(",") if cid.strip()]
    if os.getenv("CLIENT_ID"):
        ids.append(os.getenv("CLIENT_ID").strip())

    ids = [sanitize_topic_component(id_, max_len=100) for id_ in set(ids) if is_valid_topic_name(id_)]

    for cid in ids:
        if shards == 0:
            # تک‌تاپیک: cmd.<client>
            name = f"{prefix}{cid}"
            specs.append({
                "name": name,
                "partitions": int(os.getenv("PARTITIONS_DEFAULT", "1")),
                "replication_factor": int(os.getenv("REPLICATION_FACTOR", "1")),
                "config": {}
            })
            log.debug("per-client تک: %s", name)
        else:
            # sharded: cmd.<client>.p0 ... pN
            for i in range(shards):
                name = f"{prefix}{cid}.p{i}"
                specs.append({
                    "name": name,
                    "partitions": int(os.getenv("PARTITIONS_DEFAULT", "1")),
                    "replication_factor": int(os.getenv("REPLICATION_FACTOR", "1")),
                    "config": {}
                })
                log.debug("per-client sharded: %s", name)

    # حذف تکراری‌ها
    unique_specs = []
    seen = set()
    for spec in specs:
        if spec["name"] not in seen:
            seen.add(spec["name"])
            unique_specs.append(spec)
    return unique_specs

# ------------------------------
#   ایجاد/به‌روزرسانی تاپیک (بدون تغییر از کد اصلی؛ اما کامل‌شده)
# ------------------------------
def create_topic(ac: AdminClient, spec: Dict, dry: bool, timeout: int):
    """ایجاد اگر وجود نداشت، سپس ensure partitions و configs"""
    name = spec["name"]
    if not is_valid_topic_name(name):
        log.warning("⚠ نام تاپیک نامعتبر: %s", name)
        return
    log.debug("چک وجود تاپیک: %s", name)
    exists, current_parts = topic_exists(ac, name)
    if not exists:
        log.info("➕ ایجاد تاپیک جدید: %s (partitions=%d)", name, spec["partitions"])
        if dry:
            log.info("DRY_RUN: create_topics اجرا نشد.")
            return
        nt = NewTopic(
            name,
            num_partitions=spec["partitions"],
            replication_factor=spec["replication_factor"],
            config=spec.get("config", {})
        )
        fs = ac.create_topics([nt], request_timeout=timeout)
        try:
            fs[name].result()
            log.info("✔ تاپیک %s ایجاد شد.", name)
        except Exception as e:
            log.warning("⚠ create_topics(%s) خطا: %s", name, e)
            return
    else:
        log.debug("تاپیک %s از قبل وجود دارد.", name)

    # همیشه ensure partitions و configs (حتی اگر وجود داشت)
    ensure_partitions(ac, name, spec["partitions"], dry, timeout)
    ensure_configs(ac, name, spec.get("config", {}), dry, timeout)