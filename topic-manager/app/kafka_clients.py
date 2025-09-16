# ------------------------------
#   ایمپورت‌ها
# ------------------------------
from typing import Optional
from confluent_kafka import KafkaException, Consumer, Producer
from confluent_kafka.admin import (
    AdminClient, NewTopic, NewPartitions, ConfigResource, ResourceType
)
from utils import get_bootstrap, log, env_bool  # ایمپورت از utils (get_bootstrap و log از اونجا)
import os  # اضافه شده: برای os.getenv در get_admin و غیره
import time
# ------------------------------
#   ساخت کلاینت‌های کافکا
# ------------------------------
def get_admin() -> AdminClient:
    """AdminClient برای عملیات مدیریتی تاپیک‌ها"""
    conf = {"bootstrap.servers": get_bootstrap()}
    # اگر احراز هویت/SSL داشتید اینجا ست می‌شود
    sec = os.getenv("SECURITY_PROTOCOL")
    if sec: conf["security.protocol"] = sec
    mech = os.getenv("SASL_MECHANISM")
    if mech: conf["sasl.mechanism"] = mech
    user = os.getenv("SASL_USERNAME")
    pwd  = os.getenv("SASL_PASSWORD")
    if user: conf["sasl.username"] = user
    if pwd:  conf["sasl.password"] = pwd
    return AdminClient(conf)

def get_consumer(group_id: str, earliest: bool) -> Consumer:
    """Consumer برای خواندن پیام‌ها در فاز اکتشاف"""
    conf = {
        "bootstrap.servers": get_bootstrap(),
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest" if earliest else "latest",
    }
    # امنیت اختیاری
    sec = os.getenv("SECURITY_PROTOCOL")
    if sec: conf["security.protocol"] = sec
    mech = os.getenv("SASL_MECHANISM")
    if mech: conf["sasl.mechanism"] = mech
    user = os.getenv("SASL_USERNAME")
    pwd  = os.getenv("SASL_PASSWORD")
    if user: conf["sasl.username"] = user
    if pwd:  conf["sasl.password"] = pwd
    return Consumer(conf)

def get_producer() -> Producer:
    """Producer برای درخواست db.clients.list (اگر فعال باشد)"""
    conf = {"bootstrap.servers": get_bootstrap()}
    # امنیت اختیاری
    sec = os.getenv("SECURITY_PROTOCOL")
    if sec: conf["security.protocol"] = sec
    mech = os.getenv("SASL_MECHANISM")
    if mech: conf["sasl.mechanism"] = mech
    user = os.getenv("SASL_USERNAME")
    pwd  = os.getenv("SASL_PASSWORD")
    if user: conf["sasl.username"] = user
    if pwd:  conf["sasl.password"] = pwd
    return Producer(conf)

# ------------------------------
#   انتظار برای آماده‌شدن کافکا
# ------------------------------
def wait_for_kafka(ac: AdminClient, timeout_total: int = 60) -> None:
    """چک ساده اتصال به کافکا (لیست تاپیک‌ها باید جواب بدهد)"""
    t0 = time.time()
    last_err: Optional[Exception] = None
    while time.time() - t0 < timeout_total:
        try:
            md = ac.list_topics(timeout=5)
            if md and md.brokers:
                log.info("✔ اتصال به Kafka برقرار شد. brokers=%s", list(md.brokers.keys()))
                return
        except Exception as e:
            last_err = e
        time.sleep(2)
    raise SystemExit(f"❌ عدم دسترسی به Kafka در {timeout_total}s: {last_err}")