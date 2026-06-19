# Ollama Tap Proxy implementation spec

## Project name

`ollama-tap`

A transparent HTTP proxy for observing Codex/Ollama traffic without changing request or response behavior.

Primary flow:

```text
Codex -> ollama-tap -> Ollama
```

Example:

```toml
[model_providers.ollama_tap]
name = "Ollama Tap"
base_url = "http://192.168.0.55:11435/v1"
wire_api = "responses"
stream_idle_timeout_ms = 600000
```

`ollama-tap` forwards to:

```text
http://192.168.0.13:11434
```

## Goals

The proxy must:

* Forward OpenAI-compatible Ollama requests:

  * `/v1/responses`
  * `/v1/chat/completions`
  * `/v1/completions`
  * `/v1/models`
  * `/v1/embeddings`
* Forward native Ollama API requests:

  * `/api/chat`
  * `/api/generate`
  * `/api/tags`
  * `/api/show`
  * `/api/embeddings`
* Preserve streaming behavior exactly enough for Codex to keep working.
* Log requests and responses to JSONL files.
* Capture streaming chunks incrementally.
* Record basic metrics:

  * method
  * path
  * model if detectable
  * status code
  * duration
  * request byte count
  * response byte count
  * stream chunk count
  * error if any
* Avoid modifying schemas, headers, chunks, or response format unless explicitly configured.
* Be safe for local LAN use.

## Non-goals for v1

Do not implement:

* Model routing
* Load balancing
* API-key management
* Request mutation
* Response mutation
* Token-perfect counting
* Web UI
* Database storage
* Semantic tracing
* Langfuse/OpenTelemetry integration

Those can come later. The MVP is a stable transparent recorder.

## Recommended stack

Use Go.

Why Go:

* good HTTP streaming support
* easy single binary
* no Python async/proxy weirdness
* fits your existing preferences
* easy systemd/Docker deployment

Target Go version: `1.22+`.

## CLI/config

The service should support env vars and flags.

Environment variables:

```bash
OLLAMA_TAP_LISTEN_ADDR=0.0.0.0:11435
OLLAMA_TAP_UPSTREAM=http://192.168.0.13:11434
OLLAMA_TAP_LOG_DIR=/var/log/ollama-tap
OLLAMA_TAP_MAX_CAPTURE_BYTES=10485760
OLLAMA_TAP_CAPTURE_RESPONSE=true
OLLAMA_TAP_CAPTURE_STREAM_CHUNKS=true
OLLAMA_TAP_REDACT_AUTH=true
OLLAMA_TAP_TIMEOUT_SECONDS=0
```

Equivalent flags:

```bash
ollama-tap \
  --listen 0.0.0.0:11435 \
  --upstream http://192.168.0.13:11434 \
  --log-dir ./logs
```

Important: default timeout should be disabled or very high, because Codex tool loops and local models can sit for a long time.

## Repo structure

```text
ollama-tap/
  README.md
  go.mod
  cmd/
    ollama-tap/
      main.go
  internal/
    config/
      config.go
    proxy/
      handler.go
      capture.go
      headers.go
    logstore/
      jsonl.go
      sanitize.go
    observability/
      summary.go
  scripts/
    curl-smoke.sh
  deploy/
    docker-compose.yaml
    ollama-tap.service
```

## Proxy behavior

### Request handling

For every incoming request:

1. Generate a request ID.
2. Read the request body into memory up to `max_capture_bytes`.
3. If body exceeds limit:

   * forward full body if possible
   * log only the first `max_capture_bytes`
   * mark `request_truncated: true`
4. Create a new outbound request to upstream:

   * same method
   * same path and query
   * same body
5. Copy headers, except hop-by-hop headers.
6. Optionally redact auth headers in logs only.
7. Send request to upstream.
8. Copy upstream response status and headers back to client.
9. Stream response body to client while also recording chunks.
10. Flush after each copied chunk if `ResponseWriter` supports `http.Flusher`.
11. Write a final summary JSONL record.

### Hop-by-hop headers to skip

Do not forward these directly:

```text
Connection
Keep-Alive
Proxy-Authenticate
Proxy-Authorization
Te
Trailer
Transfer-Encoding
Upgrade
```

Let Go manage transfer encoding.

### Streaming requirements

This is the most important part.

The proxy must not wait for the full response before sending data to Codex.

Implementation hint:

```go
buf := make([]byte, 32*1024)

for {
    n, readErr := upstreamResp.Body.Read(buf)
    if n > 0 {
        chunk := buf[:n]

        // Write to client immediately.
        _, writeErr := w.Write(chunk)

        // Flush immediately.
        if flusher, ok := w.(http.Flusher); ok {
            flusher.Flush()
        }

        // Log chunk asynchronously or cheaply.
        capture.RecordResponseChunk(chunk)
    }

    if readErr == io.EOF {
        break
    }

    if readErr != nil {
        // log stream error
        break
    }
}
```

Do not parse SSE during forwarding. Parsing can be added as a side-channel later, but raw bytes must be forwarded as-is.

## Log files

Use JSONL because it is easy to inspect, grep, stream, and later import.

Suggested files:

```text
logs/
  requests-2026-06-18.jsonl
  chunks-2026-06-18.jsonl
  errors-2026-06-18.jsonl
```

### Summary record schema

Each completed request writes one summary record:

```json
{
  "type": "summary",
  "request_id": "018ff6da-...",
  "started_at": "2026-06-18T22:14:03.123Z",
  "ended_at": "2026-06-18T22:14:17.991Z",
  "duration_ms": 14868,
  "method": "POST",
  "path": "/v1/responses",
  "query": "",
  "client_addr": "192.168.0.42:58122",
  "upstream": "http://192.168.0.13:11434",
  "status": 200,
  "model": "qwen3.6:latest",
  "stream": true,
  "request_bytes": 39211,
  "response_bytes": 184532,
  "response_chunks": 244,
  "request_truncated": false,
  "response_truncated": false,
  "error": ""
}
```

### Request capture schema

```json
{
  "type": "request",
  "request_id": "018ff6da-...",
  "timestamp": "2026-06-18T22:14:03.123Z",
  "method": "POST",
  "path": "/v1/responses",
  "headers": {
    "content-type": ["application/json"],
    "authorization": ["REDACTED"]
  },
  "body_json": {
    "model": "qwen3.6:latest",
    "input": "..."
  },
  "body_raw": null,
  "truncated": false
}
```

If JSON parsing fails, store `body_raw` as a string.

### Chunk capture schema

For streaming, write chunk records:

```json
{
  "type": "response_chunk",
  "request_id": "018ff6da-...",
  "timestamp": "2026-06-18T22:14:04.551Z",
  "index": 12,
  "bytes": 821,
  "text": "data: {...}\n\n",
  "truncated": false
}
```

For binary or invalid UTF-8, use base64:

```json
{
  "type": "response_chunk",
  "encoding": "base64",
  "data": "..."
}
```

For v1, it is okay to log text chunks only when valid UTF-8.

## Model detection

Try to detect model from JSON request body.

Supported shapes:

### OpenAI Responses

```json
{
  "model": "qwen3.6:latest",
  "input": "..."
}
```

### Chat Completions

```json
{
  "model": "qwen3.6:latest",
  "messages": []
}
```

### Ollama native

```json
{
  "model": "qwen3.6:latest",
  "messages": []
}
```

If missing, set:

```json
"model": ""
```

## Token tracking

For v1, do not promise exact token counting.

Record these when present in upstream response JSON:

```json
{
  "prompt_eval_count": 1234,
  "eval_count": 567,
  "total_duration": 1234567890,
  "prompt_eval_duration": 123456789,
  "eval_duration": 987654321
}
```

These usually appear in native Ollama `/api/chat` and `/api/generate` final records.

For OpenAI-compatible `/v1/*`, record `usage` if present:

```json
{
  "usage": {
    "prompt_tokens": 1234,
    "completion_tokens": 567,
    "total_tokens": 1801
  }
}
```

If neither exists, leave token fields null.

Later enhancement: add approximate token estimation, but keep it clearly labeled:

```json
"estimated_input_tokens": 1234,
"estimated_output_tokens": 567,
"token_estimate_method": "chars_div_4"
```

## Redaction

Default behavior:

* redact `Authorization`
* redact `Cookie`
* redact `X-Api-Key`
* redact `X-Bf-Vk`
* redact `X-Bf-Api-Key`

Optional future redaction:

```bash
OLLAMA_TAP_REDACT_BODY=false
OLLAMA_TAP_REDACT_PATTERNS='["sk-[a-zA-Z0-9]+","ghp_[a-zA-Z0-9]+"]'
```

For your local debugging, I’d keep body logging enabled, but do not expose this service outside trusted LAN/VPN.

## Health endpoints

Add local proxy health endpoints:

```text
GET /_tap/health
GET /_tap/config
GET /_tap/stats
```

### `/_tap/health`

Returns:

```json
{
  "ok": true,
  "upstream": "http://192.168.0.13:11434"
}
```

Optional: call upstream `/api/tags` and include upstream health.

### `/_tap/stats`

In-memory counters since startup:

```json
{
  "started_at": "2026-06-18T22:00:00Z",
  "requests_total": 42,
  "requests_active": 1,
  "errors_total": 0,
  "bytes_in": 123456,
  "bytes_out": 987654
}
```

## Docker Compose

```yaml
services:
  ollama-tap:
    image: ollama-tap:latest
    build:
      context: .
    container_name: ollama-tap
    restart: unless-stopped
    ports:
      - "0.0.0.0:11435:11435"
    environment:
      OLLAMA_TAP_LISTEN_ADDR: "0.0.0.0:11435"
      OLLAMA_TAP_UPSTREAM: "http://192.168.0.13:11434"
      OLLAMA_TAP_LOG_DIR: "/logs"
      OLLAMA_TAP_CAPTURE_RESPONSE: "true"
      OLLAMA_TAP_CAPTURE_STREAM_CHUNKS: "true"
    volumes:
      - ./logs:/logs
```

## Systemd service

```ini
[Unit]
Description=Ollama Tap Proxy
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/ollama-tap \
  --listen 0.0.0.0:11435 \
  --upstream http://192.168.0.13:11434 \
  --log-dir /var/log/ollama-tap
Restart=always
RestartSec=3
User=ollama-tap
Group=ollama-tap

[Install]
WantedBy=multi-user.target
```

## Codex config example

```toml
model = "qwen3.6:latest"
model_provider = "ollama_tap"
model_context_window = 65536
model_reasoning_effort = "minimal"

[model_providers.ollama_tap]
name = "Ollama Tap"
base_url = "http://192.168.0.55:11435/v1"
wire_api = "responses"
stream_idle_timeout_ms = 600000
```

If Codex previously worked with direct Ollama without `/v1`, mirror that exact shape and only swap the host/port:

```toml
base_url = "http://192.168.0.55:11435"
```

## MVP acceptance tests

### 1. Health

```bash
curl http://localhost:11435/_tap/health
```

Expected: JSON with `ok: true`.

### 2. Native Ollama tags

```bash
curl http://localhost:11435/api/tags
```

Expected: same output as:

```bash
curl http://192.168.0.13:11434/api/tags
```

### 3. OpenAI-compatible models

```bash
curl http://localhost:11435/v1/models
```

Expected: same behavior as direct Ollama.

### 4. Non-stream chat completion

```bash
curl -X POST http://localhost:11435/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "qwen3.6:latest",
    "messages": [
      {"role": "user", "content": "Say hello in one sentence."}
    ],
    "stream": false
  }'
```

Expected:

* valid model response
* request summary logged
* response body logged

### 5. Streaming chat completion

```bash
curl -N -X POST http://localhost:11435/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "qwen3.6:latest",
    "messages": [
      {"role": "user", "content": "Count from 1 to 20 slowly."}
    ],
    "stream": true
  }'
```

Expected:

* chunks appear progressively
* no buffering until completion
* chunks logged incrementally

### 6. Codex smoke test

Point Codex to `ollama-tap`, then ask it to run a simple command:

```text
list files and tell me what repo this is
```

Expected:

* Codex performs more than one tool/action if needed
* no one-command-then-die behavior
* logs show the full request/stream sequence

## Implementation phases

### Phase 1: Transparent proxy

Build:

* config loader
* health endpoint
* raw forwarding
* streaming-safe response copying
* basic summary logs

No fancy parsing yet.

### Phase 2: Capture and inspect

Add:

* request body JSON capture
* response chunk JSONL capture
* model detection
* status/duration metrics

### Phase 3: Token/usage extraction

Add best-effort extraction from:

* native Ollama final response records
* OpenAI-compatible `usage` fields
* SSE chunks if they contain usage events

### Phase 4: Developer UX

Add:

* `ollama-tap tail`
* `ollama-tap summarize logs/...jsonl`
* maybe a tiny TUI or web page later

## Key engineering rule

If there is ever a tradeoff between “better logging” and “Codex still works,” choose “Codex still works.”

The proxy should be boring enough that if direct Ollama works, tap proxy also works.
