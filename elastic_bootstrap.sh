#!/usr/bin/env bash
set -euo pipefail

# =========================
# Config (قابل سفارشی‌سازی)
# =========================
COMPOSE_CMD="${COMPOSE_CMD:-docker compose}"   # یا docker-compose
PROJECT_DIR="${PROJECT_DIR:-.}"
ENV_FILE="${ENV_FILE:-.env}"

# URLs پیش‌فرض (در صورت نبود ELASTIC_HOST)
DEFAULT_ES_URL="http://localhost:9200"
DEFAULT_KB_URL="http://localhost:5601"

# =========================
# Helpers
# =========================
log()  { printf "\033[1;34m[INFO]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[WARN]\033[0m %s\n" "$*"; }
err()  { printf "\033[1;31m[ERR ]\033[0m %s\n" "$*" >&2; }

need() {
  command -v "$1" >/dev/null 2>&1 || { err "Missing dependency: $1"; exit 1; }
}

upsert_env() {
  local var="$1"; shift
  local val="$1"; shift || true
  if grep -qE "^${var}=" "$ENV_FILE"; then
    # escape delimiter pipes in val
    sed -i.bak -E "s|^(${var}=).*|\1${val//|/\\|}|" "$ENV_FILE"
  else
    echo "${var}=${val}" >> "$ENV_FILE"
  fi
}

random_b64() {
  head -c 32 /dev/urandom | base64 -w0 2>/dev/null || head -c 32 /dev/urandom | base64
}

now_utc() { date -u +"%Y%m%d%H%M%S"; }

json_get() {
  # usage: json_get FILTER
  # stdin: json
  local filter="$1"
  if command -v jq >/dev/null 2>&1; then
    jq -r "$filter"
  else
    # fallback خیلی ساده برای '.token.value' و '.item.api_key'
    if [[ "$filter" == ".token.value" ]]; then
      sed -nE 's/.*"token"\s*:\s*\{[^}]*"value"\s*:\s*"([^"]+)".*/\1/p' | head -n1
    elif [[ "$filter" == ".item.api_key" ]]; then
      sed -nE 's/.*"api_key"\s*:\s*"([^"]+)".*/\1/p' | head -n1
    else
      sed -nE 's/.*"value"\s*:\s*"([^"]+)".*/\1/p' | head -n1
    fi
  fi
}

wait_for_http() {
  # usage: wait_for_http URL [AUTH] [TIMEOUT_SEC]
  local url="$1"; shift
  local auth="${1:-}"; shift || true
  local timeout="${1:-300}"; shift || true
  local start_ts now
  start_ts=$(date +%s)
  while true; do
    if [ -n "$auth" ]; then
      curl -sSf -u "$auth" "$url" >/dev/null 2>&1 && break || true
    else
      curl -sSf "$url" >/dev/null 2>&1 && break || true
    fi
    now=$(date +%s)
    if [ $((now - start_ts)) -ge "$timeout" ]; then
      err "Timeout waiting for $url"
      exit 1
    fi
    sleep 3
  done
}

kb_status_available() {
  # exit 0 اگر Kibana available
  local url="$1"
  local out
  out="$(curl -s "${url}")" || return 1
  if command -v jq >/dev/null 2>&1; then
    local lvl
    lvl="$(printf "%s" "$out" | jq -r '.status.overall.level // .overall.level // empty')"
    [[ "$lvl" == "available" ]] && return 0 || return 1
  else
    echo "$out" | grep -q '"available"' && return 0 || return 1
  fi
}

kb_api() {
  # usage: kb_api METHOD PATH [DATA_JSON]
  # از Basic auth elastic استفاده می‌کنیم (Kibana API)
  local method="$1"; shift
  local path="$1"; shift
  local data="${1:-}"
  local headers=(-H 'kbn-xsrf: true')
  if [[ -n "$data" ]]; then
    headers+=(-H 'Content-Type: application/json' --data "$data")
  fi
  curl -sS -u "${ELASTIC_USERNAME}:${ELASTIC_PASSWORD}" -X "$method" "${KIBANA_URL}${path}" "${headers[@]}"
}

es_api() {
  # usage: es_api METHOD PATH [DATA_JSON] [HEADERS...]
  local method="$1"; shift
  local path="$1"; shift
  local data="${1:-}"
  shift || true
  local extra_headers=("$@")
  local headers=()
  if [[ -n "$data" ]]; then
    headers+=(-H 'Content-Type: application/json' --data "$data")
  fi
  if [[ ${#extra_headers[@]} -gt 0 ]]; then
    headers+=("${extra_headers[@]}")
  fi
  curl -sS -u "${ELASTIC_USERNAME}:${ELASTIC_PASSWORD}" -X "$method" "${ELASTIC_URL}${path}" "${headers[@]}"
}

# =========================
# Pre-flight
# =========================
need curl
if ! command -v docker >/dev/null 2>&1; then err "Docker is required."; exit 1; fi
if ! $COMPOSE_CMD version >/dev/null 2>&1; then
  if command -v docker-compose >/dev/null 2>&1; then COMPOSE_CMD="docker-compose"; else
    err "docker compose or docker-compose is required."; exit 1
  fi
fi

cd "$PROJECT_DIR"
[[ -f "$ENV_FILE" ]] || { err "ENV file '$ENV_FILE' not found. Create it and set ELASTIC_PASSWORD at minimum."; exit 1; }

# Load env
set -a
. "$ENV_FILE"
set +a

: "${ELASTIC_USERNAME:=elastic}"
: "${ELASTIC_HOST:=http://elasticsearch:9200}"
: "${TZ:=Asia/Tehran}"

# Resolve usable URLs from host
ELASTIC_URL="$ELASTIC_HOST"
if ! curl -sSf "$ELASTIC_URL" >/dev/null 2>&1; then
  ELASTIC_URL="$DEFAULT_ES_URL"
fi
KIBANA_URL="${KIBANA_URL:-$DEFAULT_KB_URL}"

# Network & volume
log "Ensuring external network 'internal_net' exists..."
docker network inspect internal_net >/dev/null 2>&1 || docker network create internal_net >/dev/null

log "Ensuring external volume 'elasticsearch' exists..."
docker volume inspect elasticsearch >/dev/null 2>&1 || docker volume create elasticsearch >/dev/null

# OS hint
if command -v sysctl >/dev/null 2>&1; then
  cur="$(sysctl -n vm.max_map_count 2>/dev/null || echo 0)"
  if [ "$cur" -lt 262144 ]; then
    warn "vm.max_map_count=$cur (<262144). Consider: sudo sysctl -w vm.max_map_count=262144 (و دائمی‌کردن آن)."
  fi
fi

# Kibana encryption keys (if missing)
if [ -z "${KIBANA_ENCRYPTION_KEY:-}" ]; then
  KIBANA_ENCRYPTION_KEY="$(random_b64)"; upsert_env KIBANA_ENCRYPTION_KEY "$KIBANA_ENCRYPTION_KEY"; log "Generated KIBANA_ENCRYPTION_KEY"
fi
if [ -z "${KIBANA_SECURITY_KEY:-}" ]; then
  KIBANA_SECURITY_KEY="$(random_b64)"; upsert_env KIBANA_SECURITY_KEY "$KIBANA_SECURITY_KEY"; log "Generated KIBANA_SECURITY_KEY"
fi
if [ -z "${KIBANA_REPORTING_KEY:-}" ]; then
  KIBANA_REPORTING_KEY="$(random_b64)"; upsert_env KIBANA_REPORTING_KEY "$KIBANA_REPORTING_KEY"; log "Generated KIBANA_REPORTING_KEY"
fi

# =========================
# 1) Elasticsearch
# =========================
: "${ELASTIC_PASSWORD:?ELASTIC_PASSWORD must be set in ${ENV_FILE}}"
log "Starting Elasticsearch..."
$COMPOSE_CMD up -d elasticsearch

log "Waiting for Elasticsearch to be reachable at $ELASTIC_URL ..."
wait_for_http "$ELASTIC_URL" "${ELASTIC_USERNAME}:${ELASTIC_PASSWORD}" 300

# =========================
# 2) Service Tokens (ES Security API)
#    - بدون body؛ نام توکن در مسیر URL
# =========================
create_service_token() {
  # usage: create_service_token VAR_NAME NAMESPACE SERVICE PREFIXNAME
  local var="$1" ns="$2" svc="$3" prefix="$4"
  local cur_val="${!var:-}"
  if [[ -n "$cur_val" && "$cur_val" != "null" ]]; then
    log "$var already set; skipping creation."
    return 0
  fi
  local name="${prefix}-$(now_utc)"
  # POST /_security/service/{namespace}/{service}/credential/token/{name}
  local resp
  resp="$(es_api POST "/_security/service/${ns}/${svc}/credential/token/${name}")" || true
  local token
  token="$(printf "%s" "$resp" | json_get '.token.value')"
  if [[ -z "$token" || "$token" == "null" ]]; then
    err "Failed to create $var. Response: $resp"
    return 1
  fi
  upsert_env "$var" "$token"
  eval "$var=\"$token\""
  log "Created $var and stored in ${ENV_FILE}"
}

log "Creating Kibana service account token (if missing)..."
create_service_token KIBANA_SA_TOKEN elastic kibana kibana-sa || exit 1

log "Creating Fleet Server service account token (if missing)..."
create_service_token FLEET_SERVER_SERVICE_TOKEN elastic fleet-server fleet-sa || exit 1

# =========================
# 3) Kibana & Fleet Server
# =========================
log "Starting Kibana and Fleet Server..."
$COMPOSE_CMD up -d kibana fleet-server

# Kibana readiness: صبر تا available
log "Waiting for Kibana API to be available at ${KIBANA_URL}/api/status ..."
# ابتدا به صورت بدون احراز هویت (اکثراً نیاز نیست)، در صورت خطا با Basic هم تست
t0=$(date +%s)
while true; do
  if kb_status_available "${KIBANA_URL}/api/status"; then
    break
  fi
  # گاهی 503 می‌دهد؛ تکرار می‌کنیم
  if (( $(date +%s) - t0 > 420 )); then
    err "Kibana did not become 'available' in time."
    exit 1
  fi
  sleep 3
done
log "Kibana is available."

# =========================
# 4) Fleet setup + Default Policy
# =========================
log "Running Fleet setup (idempotent)..."
kb_api POST "/api/fleet/setup" >/dev/null || true

# پیدا کردن Default policy
log "Fetching default agent policy id..."
pol_json="$(kb_api GET "/api/fleet/agent_policies?perPage=200")"
POLICY_ID="$(printf "%s" "$pol_json" | json_get '.items[] | select(.is_default==true) | .id' | head -n1)"
if [[ -z "$POLICY_ID" || "$POLICY_ID" == "null" ]]; then
  warn "Default policy not found; creating one..."
  create_resp="$(kb_api POST "/api/fleet/agent_policies" '{"name":"Default policy","namespace":"default","is_default":true}')" || true
  # دوباره بگیر
  pol_json="$(kb_api GET "/api/fleet/agent_policies?perPage=200")"
  POLICY_ID="$(printf "%s" "$pol_json" | json_get '.items[] | select(.is_default==true) | .id' | head -n1)"
fi
if [[ -z "$POLICY_ID" || "$POLICY_ID" == "null" ]]; then
  err "Could not determine default agent policy id. Response: ${pol_json}"
  exit 1
fi
log "Default policy id: $POLICY_ID"
# =========================
# 4.1) Fix: Ensure Fleet Server hosts are correct
# =========================
# --- Ensure Fleet Server hosts are correct ---
log "Ensuring Fleet Server hosts (for internal & host access)..."
kb_api PUT "/api/fleet/settings" \
'{"fleet_server_hosts":["http://fleet-server:8220","http://localhost:8220"]}' >/dev/null || true

# =========================
# 4.2) Fix: Fleet Message Signing Key mismatch
# =========================
# --- Fix: Fleet Message Signing Key mismatch ---
log "Checking Fleet message signing keys..."
msk_list="$(kb_api GET "/api/saved_objects/_find?type=fleet-message-signing-keys&per_page=200" || true)"
msk_total="$(printf "%s" "$msk_list" | json_get '.total' 2>/dev/null || echo 0)"

# اگر آبجکتی هست، حذف و دوباره بساز (در سناریوی mismatch با کلیدهای رمزگذاری قبلی)
if [[ "$msk_total" =~ ^[0-9]+$ && "$msk_total" -gt 0 ]]; then
  log "Removing existing message signing keys ($msk_total) due to potential encryption mismatch..."
  if command -v jq >/dev/null 2>&1; then
    ids=$(printf "%s" "$msk_list" | jq -r '.saved_objects[].id')
  else
    ids=$(printf "%s" "$msk_list" | sed -nE 's/.*"id":"([^"]+)".*/\1/p')
  fi
  for id in $ids; do
    kb_api DELETE "/api/saved_objects/fleet-message-signing-keys/${id}" >/dev/null || true
  done
  # تلاش برای ساخت کلید جدید (اگر endpoint موجود باشد)
  kb_api POST "/api/fleet/message_signing/rotate" >/dev/null 2>&1 || true
  log "Message signing keys rotated."
else
  # اگر هیچ کلیدی نیست، یکی بساز (برای نسخه‌هایی که نیاز به کلید دارند)
  kb_api POST "/api/fleet/message_signing/rotate" >/dev/null 2>&1 || true
fi
# --- End Fix ---

# =========================
# 5) Enrollment Token
# =========================
if [[ -z "${ENROLLMENT_TOKEN:-}" || "$ENROLLMENT_TOKEN" == "null" ]]; then
  log "Creating enrollment token for the default policy..."
  et_resp="$(kb_api POST "/api/fleet/enrollment_api_keys" "{\"policy_id\":\"${POLICY_ID}\"}")" || true
  ENROLLMENT_TOKEN="$(printf "%s" "$et_resp" | json_get '.item.api_key')"
  # fallback: در صورت نادر، اگر API key برنگشت، یکبار دیگر تلاش کن
  if [[ -z "$ENROLLMENT_TOKEN" || "$ENROLLMENT_TOKEN" == "null" ]]; then
    warn "Enrollment token not returned; retrying once after 5s..."
    sleep 5
    et_resp="$(kb_api POST "/api/fleet/enrollment_api_keys" "{\"policy_id\":\"${POLICY_ID}\"}")" || true
    ENROLLMENT_TOKEN="$(printf "%s" "$et_resp" | json_get '.item.api_key')"
  fi
  if [[ -z "$ENROLLMENT_TOKEN" || "$ENROLLMENT_TOKEN" == "null" ]]; then
    err "Failed to obtain ENROLLMENT_TOKEN. Response: $et_resp"
    exit 1
  fi
  upsert_env ENROLLMENT_TOKEN "$ENROLLMENT_TOKEN"
  log "Enrollment token created and stored in ${ENV_FILE}"
else
  log "Enrollment token already present in ${ENV_FILE}, skipping creation."
fi

# =========================
# 6) Elastic Agent
# =========================
log "Starting Elastic Agent..."
$COMPOSE_CMD up -d elastic-agent

# =========================
# 7) Quick health probes
# =========================
log "All services are starting. Quick health probes:"
echo "  - Elasticsearch: ${ELASTIC_URL}"
curl -s -u "${ELASTIC_USERNAME}:${ELASTIC_PASSWORD}" "${ELASTIC_URL}" | head -c 200 || true; echo
echo "  - Kibana: ${KIBANA_URL}"
curl -s "${KIBANA_URL}/api/status" | head -c 200 || true; echo
echo "  - Fleet Server: http://localhost:8220/api/status"
curl -s "http://localhost:8220/api/status" | head -c 200 || true; echo

log "Done. Open Kibana at ${KIBANA_URL}"
