import os, time, sys  # مستقیم ایمپورت کن (built-in)
from utils import log, env_bool, is_canonical_client_id, get_bootstrap  # get_bootstrap اضافه شد
from kafka_clients import get_admin, wait_for_kafka
from discovery import run_discovery, load_known_clients, save_known_clients
from topic_ops import build_specs_from_env, create_topic
from deleter import delete_extras_if_requested

def main():
    # خواندن ENVها (بدون تغییر: timeout, dry, wait_s)
    timeout = int(os.getenv("TIMEOUT_SEC", "30"))                         # تایم‌اوت فراخوانی‌های ادمین
    dry     = env_bool("DRY_RUN", True)                                   # اگر true → فقط شبیه‌سازی
    wait_s  = int(os.getenv("WAIT_FOR_KAFKA_SEC", "60"))                  # حداکثر صبر برای آماده‌شدن کافکا

    log.info("== Topic Manager ==")
    log.info("BOOTSTRAP: %s | DRY_RUN=%s | TIMEOUT=%ss", get_bootstrap(), dry, timeout)  # حالا درست ایمپورت می‌شه

    ac = get_admin()
    wait_for_kafka(ac, timeout_total=wait_s)

    cache_path = os.getenv("DISCOVERY_CACHE_FILE", "/state/known_clients.json")
    known = load_known_clients(cache_path)

    newly = run_discovery()
    if newly:
        newly = [x for x in newly if is_canonical_client_id(x)]  # حالا درست ایمپورت می‌شه
        log.info("🔎 شناسه‌های تازه کشف‌شده (n=%d): %s", len(newly), newly)
        known.update(newly)
        try:
            save_known_clients(cache_path, known)
        except Exception as e:
            log.warning("⚠ ذخیرهٔ کش ناموفق: %s", e)

    specs = build_specs_from_env(discovered_ids=list(known))
    if not specs:
        log.warning("هیچ تاپیکی برای مدیریت تعریف نشده است.")
        return

    for spec in specs:
        create_topic(ac, spec, dry=dry, timeout=timeout)

    delete_extras_if_requested(ac, [s["name"] for s in specs], dry)

    log.info("✅ کار Topic Manager تمام شد. DRY_RUN=%s", dry)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Interrupted by user")
        sys.exit(130)