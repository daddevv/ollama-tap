#!/usr/bin/env bash
set -euo pipefail

PROXY="${OLLAMA_TAP_PROXY:-http://localhost:11435}"
PASS=0
FAIL=0
check() {
  local desc="$1" ok="$2"
  if [ "$ok" = "true" ]; then
    echo "  ✓ $desc"
    PASS=$((PASS+1))
  else
    echo "  ✗ $desc"
    FAIL=$((FAIL+1))
  fi
}

echo "=== ollama-tap smoke tests ==="

# --- In-process endpoints (no upstream needed) ---
echo ""
echo "-- In-process endpoints --"

# 1. Health endpoint
resp=$(curl -sf --max-time 5 "$PROXY/_tap/health") && \
  check "health endpoint returns {ok:true}" "$(echo "$resp" | jq -r '.ok' 2>/dev/null || echo false)" || true

# 2. Stats endpoint
resp=$(curl -sf --max-time 5 "$PROXY/_tap/stats") && \
  check "stats endpoint returns json" "$(echo "$resp" | jq 'has("total_requests")' -r 2>/dev/null || echo false)" || true

# 3. Dashboard HTML page
resp=$(curl -sf --max-time 5 "$PROXY/_tap/dashboard" | head -c 1024) && \
  check "dashboard page loads (HTML)" "$(echo "$resp" | grep -qi '<html' && echo true || echo false)" || true

# 4. Dashboard snapshot API
resp=$(curl -sf --max-time 5 "$PROXY/_tap/dashboard/api/snapshot") && \
  check "dashboard snapshot returns json" "$(echo "$resp" | jq 'has("total_requests")' -r 2>/dev/null || echo false)" || true

# 5. Dashboard history API
resp=$(curl -sf --max-time 5 "$PROXY/_tap/dashboard/api/history?minutes=10") && \
  check "dashboard history returns json array" "$(echo "$resp" | jq 'type == "array"' -r 2>/dev/null || echo false)" || true

# 6. Dashboard models API
resp=$(curl -sf --max-time 5 "$PROXY/_tap/dashboard/api/models") && \
  check "dashboard models returns json map" "$(echo "$resp" | jq 'type == "object"' -r 2>/dev/null || echo false)" || true

# --- Ollama proxy endpoints (require live upstream) ---
echo ""
echo "-- Ollama proxy endpoints (requires upstream) --"

_upstream_ok="false"

# 7. Ollama version (native)
if resp=$(curl -sf --max-time 5 "$PROXY/api/version" 2>/dev/null); then
  _upstream_ok="true"
  check "api/version proxy works" "$(echo "$resp" | jq 'has("version")' -r 2>/dev/null || echo false)" || true
else
  echo "  ⚠ api/version unavailable (no live Ollama upstream)"
fi

# 8. Ollama tags
if [ "$_upstream_ok" = "true" ]; then
  resp=$(curl -sf --max-time 5 "$PROXY/api/tags") && \
    check "api/tags proxy works" "$(echo "$resp" | jq 'has("models") or (length > 10)' -r 2>/dev/null || echo false)" || true
else
  echo "  ⚠ api/tags skipped (no upstream)"
fi

# 9. OpenAI models
if [ "$_upstream_ok" = "true" ]; then
  resp=$(curl -sf --max-time 5 "$PROXY/v1/models") && \
    check "/v1/models proxy works" "$(echo "$resp" | jq 'has("data")' -r 2>/dev/null || echo false)" || true
else
  echo "  ⚠ /v1/models skipped (no upstream)"
fi

# 10. OpenAI chat (non-streaming)
if [ "$_upstream_ok" = "true" ]; then
  resp=$(curl -sf --max-time 5 -X POST "$PROXY/v1/chat/completions" \
    -H "Content-Type: application/json" \
    -d '{"model":"dummy","messages":[{"role":"user","content":"hi"}],"stream":false}') && \
    check "/v1/chat/completions proxy works" "$(echo "$resp" | jq 'has("id")' -r 2>/dev/null || echo false)" || true
else
  echo "  ⚠ /v1/chat/completions skipped (no upstream)"
fi

# 11. Streaming chat (check headers)
if [ "$_upstream_ok" = "true" ]; then
  headers=$(curl -sI --max-time 5 "$PROXY/v1/chat/completions" \
    -H "Content-Type: application/json" \
    -d '{"model":"dummy","messages":[{"role":"user","content":"hi"}],"stream":true}' 2>/dev/null) && \
    check "streaming sets Content-Type header" "$(printf '%s\n' "$headers" | grep -qi 'text/event-stream' && echo true || echo false)" || true
else
  echo "  ⚠ streaming skipped (no upstream)"
fi

# 12. Native chat (NDJSON)
if [ "$_upstream_ok" = "true" ]; then
  resp=$(curl -sf --max-time 5 -N -X POST "$PROXY/api/chat" \
    -H "Content-Type: application/json" \
    -d '{"model":"dummy","messages":[{"role":"user","content":"hi"}]}') && \
    check "/api/chat proxy works (stream)" "$(echo "$resp" | head -1 | jq 'has("model")' -r 2>/dev/null || echo false)" || true
else
  echo "  ⚠ /api/chat skipped (no upstream)"
fi

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" = "0" ] && exit 0 || exit 1
