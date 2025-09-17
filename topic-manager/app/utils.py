# ------------------------------
#   ایمپورت‌ها و مقدمات
# ------------------------------
import json, os, sys, time, logging, re  # time اضافه شده
from typing import Dict, List, Tuple, Optional, Set
# بقیه بدون تغییر (لاگینگ، env_bool، get_bootstrap، regexها، is_canonical_client_id، is_valid_topic_name، sanitize_topic_component)
# ------------------------------
#   لاگ و سطح آن
# ------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()                        # سطح لاگ از ENV
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("topic-manager")                                  # لاگر اصلی

# ------------------------------
#   ابزارهای عمومی ENV
# ------------------------------
def env_bool(name: str, default: bool = False) -> bool:
    """خواندن مقدار بولین از ENV با پذیرش 1/true/yes/on"""
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

def get_bootstrap() -> str:
    """bootstrap سرور کافکا؛ اولویت با KAFKA_BOOTSTRAP_SERVERS سپس BOOTSTRAP_SERVERS"""
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS") or os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")

# ------------------------------
#   ابزارهای مدیریت تاپیک
# ------------------------------
# الگوی client_id کانونیکال: شروع با client- و سپس حروف کوچک/عدد/خط‌تیره/کوچک hex (a-f)
CANON_CLIENT_RE = re.compile(r"^client-[a-f0-9][a-f0-9-]{4,63}$")  # a-f اضافه شد برای hex

def is_canonical_client_id(s: str) -> bool:
    """تشخیص client_id «واقعی/کانونیکال»: client-... با طول ≤ 64"""
    if not isinstance(s, str):
        return False
    s = s.strip()
    if len(s) > 64:
        return False
    return bool(CANON_CLIENT_RE.match(s))

VALID_TOPIC_RE = re.compile(r"^[a-zA-Z0-9._-]+$")                         # الگوی نام‌های مجاز کافکا

def is_valid_topic_name(name: str) -> bool:
    """اعتبارسنجی نام تاپیک (فقط حروف/عدد/._-)"""
    return bool(VALID_TOPIC_RE.match(name)) and len(name) > 0

def sanitize_topic_component(s: str, max_len: int = 249) -> str:
    """
    پاک‌سازی جزء نام تاپیک برای کافکا:
    - فقط حروف/عدد/._- مجاز است → بقیه '-' می‌شود
    - جداکننده‌های تکراری (--, .., __) فشرده می‌شوند
    - جداکننده‌های ابت...(truncated in original, but assume full from manage_topics.py)
    """
    # کد کامل sanitize از manage_topics.py رو اینجا بگذار (بدون تغییر)
    # برای مثال:
    s = re.sub(r'[^a-zA-Z0-9._-]', '-', s)
    s = re.sub(r'[-._]+', '-', s.strip('-._'))
    return s[:max_len] if len(s) > max_len else s