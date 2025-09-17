# ------------------------------
#   ایمپورت‌ها
# ------------------------------
from typing import List
from confluent_kafka import KafkaException
from utils import log, env_bool, os
from topic_ops import list_topics  # برای لیست کردن تاپیک‌های موجود
from kafka_clients import AdminClient  # برای عملیات حذف

# ------------------------------
#   حذف تاپیک (بدون تغییر از کد اصلی)
# ------------------------------
def delete_topic(ac: AdminClient, name: str, dry: bool):
    """حذف تاپیک غیرضروری؛ با احتیاط و تنها اگر در پیشوندهای مجاز باشد"""
    log.info("− حذف تاپیک اضافه: %s", name)
    if dry:
        log.info("DRY_RUN: delete_topics اجرا نشد.")
        return
    fs = ac.delete_topics([name], operation_timeout=30)
    try:
        [f.result() for f in fs.values()]
        log.info("✔ حذف شد: %s", name)
    except KafkaException as e:
        log.warning("⚠ delete_topics(%s) خطا: %s", name, e)

# ------------------------------
#   حذف تاپیک‌های اضافه (بدون تغییر از کد اصلی)
# ------------------------------
def delete_extras_if_requested(ac: AdminClient, desired_names: List[str], dry: bool):
    """اگر DELETE_EXTRA=true باشد، تاپیک‌هایی با پیشوندهای مشخص که در desired نیستند حذف می‌شوند"""
    if not env_bool("DELETE_EXTRA", False):
        return
    prefixes = [p.strip() for p in os.getenv("DELETE_PREFIXES", "cmd.,clients.,server.").split(",") if p.strip()]
    existing = set(list_topics(ac))
    desired  = set(desired_names)

    def allowed(name: str) -> bool:
        return any(name.startswith(p) for p in prefixes)

    extras = sorted([t for t in existing if allowed(t) and t not in desired])
    if not extras:
        log.info("هیچ تاپیک اضافه‌ای برای حذف وجود ندارد.")
    else:
        log.warning("⚠ تاپیک‌های اضافه که حذف خواهند شد (%d): %s", len(extras), extras)
        for t in extras:
            delete_topic(ac, t, dry)