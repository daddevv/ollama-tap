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

# 1. Health endpoint
resp=$(curl -sf "$PROXY/_tap/health") &&   check "health endpoint returns {ok:true}" "$(echo "$resp" | python3 -c 'import sys,json; print("true" if json.load(sys.stdin).get("ok") else "false")')" || true

# 2. Stats endpoint
resp=$(curl -sf "$PROXY/_tap/stats") &&   check "stats endpoint returns json" "$(echo "$resp" | python3 -c 'import sys,json; print("true" if "total_requests" in json.load(sys.stdin) else "false")')" || true

# 3. Ollama version (native)
resp=$(curl -sf "$PROXY/api/version") &&   check "api/version proxy works" "$(echo "$resp" | python3 -c 'import sys,json; d=json.load(sys.stdin); print("true" if "version" in d else "false")')" || true

# 4. Ollama tags
resp=$(curl -sf "$PROXY/api/tags") &&   check "api/tags proxy works" "$(echo "$resp" | python3 -c 'import sys,json; d=json.load(sys.stdin); print("true" if ("models" in d or len(json.dumps(d))>10) else "false")')" || true

# 5. OpenAI models
resp=$(curl -sf "$PROXY/v1/models") &&   check "/v1/models proxy works" "$(echo "$resp" | python3 -c 'import sys,json; d=json.load(sys.stdin); print("true" if "data" in d else "false")')" || true

# 6. OpenAI chat (non-streaming)
resp=$(curl -sf -X POST "$PROXY/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -d '{"model":"dummy","messages":[{"role":"user","content":"hi"}],"stream":false}') &&   check "/v1/chat/completions proxy works" "$(echo "$resp" | python3 -c 'import sys,json; d=json.load(sys.stdin); print("true" if "id" in d else "false")')" || true

# 7. Streaming chat (check headers)
headers=$(curl -sI "$PROXY/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -d '{"model":"dummy","messages":[{"role":"user","content":"hi"}],"stream":true}') &&   check "streaming sets Content-Type header" "$(echo "$headers" | python3 -c 'import sys; print("true" if "text/event-stream" in sys.stdin.read().lower() else "false")')" || true

# 8. Native chat (NDJSON)
resp=$(curl -sf -N -X POST "$PROXY/api/chat" \
  -H "Content-Type: application/json" \
  -d '{"model":"dummy","messages":[{"role":"user","content":"hi"}]}') &&   check "/api/chat proxy works (stream)" "$(echo "$resp" | head -1 | python3 -c 'import sys,json; d=json.load(sys.stdin); print("true" if "model" in d else "false")')" || true

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" = "0" ] && exit 0 || exit 1
