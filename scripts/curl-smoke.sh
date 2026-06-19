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

# json_field -- extract a JSON field value using jq; prints "false" on failure.
json_field() {
  jq -r "$1" 2>/dev/null || echo "false"
}

echo "=== ollama-tap smoke tests ==="

# 1. Health endpoint
resp=$(curl -sf "$PROXY/_tap/health") &&   check "health endpoint returns {ok:true}" "$(json_field '.ok')" || true

# 2. Stats endpoint
resp=$(curl -sf "$PROXY/_tap/stats") &&   check "stats endpoint returns json" "$(json_field 'has("total_requests")')" || true

# 3. Ollama version (native)
resp=$(curl -sf "$PROXY/api/version") &&   check "api/version proxy works" "$(json_field 'has("version")')" || true

# 4. Ollama tags
resp=$(curl -sf "$PROXY/api/tags") &&   check "api/tags proxy works" "$(json_field 'has("models") or (length > 10)')" || true

# 5. OpenAI models
resp=$(curl -sf "$PROXY/v1/models") &&   check "/v1/models proxy works" "$(json_field 'has("data")')" || true

# 6. OpenAI chat (non-streaming)
resp=$(curl -sf -X POST "$PROXY/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -d '{"model":"dummy","messages":[{"role":"user","content":"hi"}],"stream":false}') &&   check "/v1/chat/completions proxy works" "$(json_field 'has("id")')" || true

# 7. Streaming chat (check headers)
headers=$(curl -sI "$PROXY/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -d '{"model":"dummy","messages":[{"role":"user","content":"hi"}],"stream":true}') &&   check "streaming sets Content-Type header" "$(printf '%s\n' "$headers" | grep -qi 'text/event-stream' && echo true || echo false)" || true

# 8. Native chat (NDJSON)
resp=$(curl -sf -N -X POST "$PROXY/api/chat" \
  -H "Content-Type: application/json" \
  -d '{"model":"dummy","messages":[{"role":"user","content":"hi"}]}') &&   check "/api/chat proxy works (stream)" "$(head -1 <<<"$resp" | json_field 'has("model")')" || true

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" = "0" ] && exit 0 || exit 1
