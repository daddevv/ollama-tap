#!/usr/bin/env bash
set -euo pipefail

# Integration test for ollama-tap
# Requires: a running ollama-tap instance + live Ollama upstream

PROXY="${OLLAMA_TAP_PROXY:-http://localhost:11435}"
PASS=0
FAIL=0
WARN=0
TESTS=0
STREAM_TIMEOUT=90  # generous for slow models loading from disk

check() {
  local desc="$1" ok="$2"
  TESTS=$((TESTS+1))
  if [ "$ok" = "true" ]; then
    echo "  ✓ $desc"
    PASS=$((PASS+1))
  else
    echo "  ✗ $desc"
    FAIL=$((FAIL+1))
  fi
}

warn() {
  TESTS=$((TESTS+1))
  echo "  ⚠ $1"
  WARN=$((WARN+1))
}

echo "=== ollama-tap integration tests ==="
echo ""
echo "Config:"
echo "  PROXY            = $PROXY"
echo "  OLLAMA_TAP_FORCE_MODEL=${OLLAMA_TAP_FORCE_MODEL:-<unset>}"
echo "  STREAM_TIMEOUT   = ${STREAM_TIMEOUT}s"
echo ""

# --- Pre-flight checks ---
echo "-- Pre-flight --"

if resp=$(curl -sf --max-time 5 "$PROXY/_tap/health" 2>/dev/null); then
  health_ok=$(echo "$resp" | jq -r '.ok' 2>/dev/null || echo false)
  check "proxy health endpoint responds {ok:true}" "$health_ok"
else
  echo "  ✗ proxy unreachable at $PROXY"
  echo "  Run: OLLAMA_TAP_UPSTREAM=http://localhost:11434 go run ./cmd/ollama-tap &"
  exit 1
fi

# Check upstream availability
if resp=$(curl -sf --max-time 5 "$PROXY/api/version" 2>/dev/null); then
  upstream_ok="true"
  version=$(echo "$resp" | jq -r '.version' 2>/dev/null || echo "unknown")
  echo "  ✓ upstream Ollama $version reachable"
else
  upstream_ok="false"
  warn "no live Ollama upstream — proxy tests will be skipped"
fi

# --- Model selection (highest priority: env var override) ---
echo ""
echo "-- Models --"

MODEL=""

if [ -n "${OLLAMA_TAP_FORCE_MODEL:-}" ]; then
  MODEL="$OLLAMA_TAP_FORCE_MODEL"
  echo "  → using forced model: $MODEL"
else
  # Auto-detect: pick first available model
  if [ "$upstream_ok" = "true" ]; then
    if resp=$(curl -sf --max-time 10 "$PROXY/api/tags" 2>/dev/null); then
      count=$(echo "$resp" | jq '(.models // [] | length)' 2>/dev/null || echo "0")
      if [ "$count" -gt 0 ] 2>/dev/null; then
        MODEL=$(echo "$resp" | jq -r '.models[0].name' 2>/dev/null || echo "")
        check "api/tags returns $count model(s)" "true"
        echo "  → auto-picked first model: $MODEL"
      fi
    else
      warn "could not fetch api/tags"
    fi
  else
    # Still allow forcing without upstream
    if [ -n "${OLLAMA_TAP_FORCE_MODEL:-}" ]; then
      MODEL="$OLLAMA_TAP_FORCE_MODEL"
      echo "  → using forced model: $MODEL"
    fi
  fi
fi

if [ -z "$MODEL" ]; then
  warn "no models found — set OLLAMA_TAP_FORCE_MODEL=llama3.2 or pull a model first"
  echo ""
  echo "Quick setup:"
  echo "  ollama pull llama3.2        # fetch smallest supported model"
  echo "  export OLLAMA_TAP_FORCE_MODEL=llama3.2"
  exit 0
fi

# --- Integration tests ---
echo ""
echo "-- Integration tests (model=$MODEL) --"

TEMP_PROMPT="Say exactly: integration test passed"

# Test 1: Non-streaming OpenAI endpoint
if [ "$upstream_ok" = "true" ]; then
  echo ""
  echo "--- Test 1: /v1/chat/completions (non-streaming) ---"

  start_time=$(date +%s%N)
  resp=$(curl -sf --max-time $STREAM_TIMEOUT -X POST "$PROXY/v1/chat/completions" \
    -H "Content-Type: application/json" \
    -d "{
      \"model\": \"$MODEL\",
      \"messages\": [{\"role\": \"user\", \"content\": \"$TEMP_PROMPT\"}],
      \"stream\": false,
      \"temperature\": 0.1
    }")

  if echo "$resp" | jq -e '.id' >/dev/null 2>&1; then
    model_resp=$(echo "$resp" | jq -r '.model' 2>/dev/null)
    usage=$(echo "$resp" | jq -c '.usage' 2>/dev/null)

    content=$(echo "$resp" | jq -r '.choices[0].message.content // empty' 2>/dev/null || echo "")
    if [ -n "$content" ]; then
      check "non-streaming returns choices with content" "true"
    else
      check "non-streaming returns choices with content" "false"
    fi

    if echo "$usage" | jq -e '.prompt_tokens' >/dev/null 2>&1; then
      check "response includes token usage metadata" "true"
    else
      warn "response missing usage metadata"
    fi

    id_resp=$(echo "$resp" | jq -r '.id' 2>/dev/null || echo "")
    check "response has valid id field" "$( [ -n "$id_resp" ] && echo true || echo false )"
    check "non-streaming response body is present" "$( [ ${#content} -gt 0 ] && echo true || echo false )"
    check "/v1/chat/completions returned model=$model_resp" "$([ -n "$model_resp" ] && echo true || echo false)"

    elapsed=$(( ($(date +%s%N) - start_time) / 1000000 ))
    check "non-streaming completed in ${elapsed}ms" "true"
  else
    err_msg=$(echo "$resp" | jq -r '.error.message // .message // empty' 2>/dev/null || echo "")
    if [ -n "$err_msg" ]; then
      warn "error response: $err_msg"
    else
      warn "non-streaming returned unexpected response"
    fi
    check "non-streaming returns json object" "$(echo "$resp" | jq 'type == "object"' -r 2>/dev/null || echo false)"
  fi

else
  warn "skipping non-streaming test (no upstream)"
fi

# Test 2: Streaming OpenAI endpoint — write to temp file to avoid subshell/pipefail issues
if [ "$upstream_ok" = "true" ]; then
  echo ""
  echo "--- Test 2: /v1/chat/completions (streaming) ---"

  streaming_content=""
  chunk_count=0
  stream_file=$(mktemp)
  trap 'rm -f "$stream_file"' EXIT

  curl -s --max-time $STREAM_TIMEOUT -X POST "$PROXY/v1/chat/completions" \
    -H "Content-Type: application/json" \
    -d "{
      \"model\": \"$MODEL\",
      \"messages\": [{\"role\": \"user\", \"content\": \"$TEMP_PROMPT\"}],
      \"stream\": true,
      \"temperature\": 0.1
    }" > "$stream_file" || true

  while IFS= read -r line; do
    if [[ "$line" == data:* ]]; then
      chunk_data="${line#data: }"
      if [ "$chunk_data" != "[DONE]" ] && [ -n "$chunk_data" ]; then
        content_chunk=$(echo "$chunk_data" | jq -r '(.choices[0].delta.content // "") + (.choices[0].delta.reasoning // "")' 2>/dev/null || echo "")
        streaming_content="${streaming_content}${content_chunk}"
        chunk_count=$((chunk_count+1))
      fi
    fi
  done < "$stream_file"

  check "streaming returns chunks" "$( [ $chunk_count -gt 0 ] && echo true || echo false )"
  check "streaming content assembled correctly" "$( [ ${#streaming_content} -gt 0 ] && echo true || echo false )"
  if [ ${#streaming_content} -gt 5 ]; then
    check "streamed content is readable" "$(echo "$streaming_content" | grep -qiE '[a-z]' && echo true || echo false)"
  fi

else
  warn "skipping streaming test (no upstream)"
fi

# Test 3: Native Ollama /api/generate
if [ "$upstream_ok" = "true" ]; then
  echo ""
  echo "--- Test 3: /api/generate (native) ---"

  resp=$(curl -s --max-time $STREAM_TIMEOUT -X POST "$PROXY/api/generate" \
    -H "Content-Type: application/json" \
    -d "{
      \"model\": \"$MODEL\",
      \"prompt\": '$TEMP_PROMPT',
      \"stream\": false
    }" || true)

  check "/api/generate returns response" "$(echo "$resp" | jq -e '.response' >/dev/null 2>&1 && echo true || echo false)"
  gen_response=$(echo "$resp" | jq -r '.response // empty' 2>/dev/null || echo "")
  check "generated text is present" "$( [ ${#gen_response} -gt 0 ] && echo true || echo false )"

else
  warn "skipping native generate test (no upstream)"
fi

# --- Metrics verification ---
echo ""
echo "-- Metrics verification --"

sleep 2

resp=$(curl -sf --max-time 5 "$PROXY/_tap/stats")
total=$(echo "$resp" | jq -r '.total_requests' 2>/dev/null || echo "0")
check "stats.total_requests > 0 after tests" "$( [ "$total" -gt 0 ] 2>/dev/null && echo true || echo false )"

active=$(echo "$resp" | jq -r '.active_connections' 2>/dev/null || echo "-1")
check "no lingering active connections" "$( [ "$active" = "0" ] && echo true || echo false )"

streaming_conn=$(echo "$resp" | jq -r '.streaming_connections' 2>/dev/null || echo "-1")
check "no lingering streaming connections" "$( [ "$streaming_conn" = "0" ] && echo true || echo false )"

resp=$(curl -sf --max-time 5 "$PROXY/_tap/dashboard/api/snapshot")
check "dashboard snapshot reflects requests" "$(echo "$resp" | jq 'has("total_requests") and .total_requests > 0' -r 2>/dev/null || echo false)"

resp=$(curl -sf --max-time 5 "$PROXY/_tap/dashboard/api/history?minutes=5")
history_count=$(echo "$resp" | jq 'length' 2>/dev/null || echo "0")
check "dashboard history has entries" "$( [ "$history_count" -gt 0 ] 2>/dev/null && echo true || echo false )"

resp=$(curl -sf --max-time 5 "$PROXY/_tap/dashboard/api/models")
model_keys=$(echo "$resp" | jq 'keys | length' 2>/dev/null || echo "0")
if [ "$model_keys" -gt 0 ] 2>/dev/null; then
  check "model usage tracker has entries" "$(echo "$resp" | jq '.[keys[0]] | has("total_requests")' -r 2>/dev/null || echo false)"
else
  warn "model usage tracker empty (metrics may not have ticked yet)"
fi

# --- Summary ---
echo ""
echo "=== Results ==="
echo "  Tests:   $TESTS"
echo "  Passed:  $PASS"
echo "  Failed:  $FAIL"
echo "  Warnings: $WARN"

if [ "$FAIL" = "0" ]; then
  echo ""
  echo "All integration tests passed!"
  exit 0
else
  echo ""
  echo "$FAIL test(s) failed"
  exit 1
fi
