# ollama-tap implementation spec

## Purpose

`ollama-tap` is a transparent HTTP proxy that sits between a client such as Codex and an Ollama server.

Primary path:

```text
client -> ollama-tap -> Ollama
```

The proxy must preserve request and response behavior closely enough that a client already working against Ollama continues to work when pointed at `ollama-tap`, while the proxy records request/response metadata and best-effort generation statistics.

Example client config:

```toml
[model_providers.ollama_tap]
name = "Ollama Tap"
base_url = "http://192.168.0.55:11435/v1"
wire_api = "responses"
stream_idle_timeout_ms = 600000
```

Example upstream:

```text
http://192.168.0.13:11434
```

## Definition of done

The implementation is complete when all of the following are true:

1. A client can switch its base URL from Ollama to `ollama-tap` without changing request payloads or expected response schemas.
2. The proxy forwards both native `/api/*` and OpenAI-compatible `/v1/*` traffic without buffering streaming responses until completion.
3. The proxy records enough data to answer which request ran, which model it targeted, whether it streamed, how long it took, how many bytes moved, and what usage or timing fields Ollama returned.
4. The proxy exposes local health and in-memory counters.
5. The repository includes code, tests, a README, a smoke-test script, a Dockerfile, Docker Compose, and a systemd unit.
6. `go test ./...` passes and the manual smoke tests in this document pass against a real Ollama server.

## Assumptions and constraints

* Language: Go 1.22+.
* Deployment: single binary, Linux-friendly, Docker and systemd friendly.
* Network model: trusted LAN or VPN only for v1.
* Upstream model: one configured Ollama server.
* Minimum upstream compatibility for `/v1/responses`: Ollama `0.13.3+`.
* The proxy does not emulate unsupported Ollama endpoints. If upstream lacks an endpoint, the proxy returns the upstream result unchanged.
* No request or response mutation in v1.
* No authentication, ACLs, or multitenancy in v1.

## Non-goals for v1

Do not implement any of the following in v1:

* model routing
* load balancing
* API key management
* schema transformation
* prompt or response mutation
* token-perfect counting when upstream does not provide usage
* a web UI
* database storage
* OpenTelemetry, Langfuse, or external tracing backends

## Transparent proxy contract

The most important rule is simple:

If direct Ollama works, the same client should work through `ollama-tap`.

When there is a tradeoff between richer observability and transport transparency, choose transport transparency.

### Path routing

* Reserve `/_tap/*` for internal proxy endpoints.
* Forward every other request path to the configured Ollama upstream unchanged.
* Do not maintain a brittle allow-list of proxied paths. The proxy should work for current and future Ollama endpoints.

Known important paths that must work in tests:

* OpenAI-compatible:
  * `/v1/responses`
  * `/v1/chat/completions`
  * `/v1/completions`
  * `/v1/models`
  * `/v1/models/{model}`
  * `/v1/embeddings`
* Native Ollama:
  * `/api/chat`
  * `/api/generate`
  * `/api/tags`
  * `/api/show`
  * `/api/embed`
  * `/api/embeddings`
  * `/api/version`
  * `/api/ps`

Experimental endpoints such as image generation should still proxy transparently even if semantic stats extraction for them is minimal.

### URL handling

For proxied requests:

* Replace only scheme, host, and base authority with the upstream value.
* Preserve request method exactly.
* Preserve `URL.Path`, `URL.RawPath`, and `URL.RawQuery` exactly.
* Do not normalize slashes.
* Do not re-encode query strings beyond what Go already requires for the original request object.

### Header handling

Copy request and response headers as transparently as possible.

Rules:

* Strip hop-by-hop headers in both directions.
* Also strip any header tokens named by the inbound `Connection` header.
* Do not forward these directly:

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

* Let Go manage transfer encoding and chunked framing.
* Preserve end-to-end headers such as `Content-Type`, `Accept`, `Accept-Encoding`, `Cache-Control`, `ETag`, and `Authorization`.
* Redaction applies only to logs, never to forwarded traffic.
* Set outbound `Host` to the upstream host.
* Append or set these forwarding headers:
  * `X-Forwarded-For`
  * `X-Forwarded-Host`
  * `X-Forwarded-Proto`

### Timeout policy

Do not impose an end-to-end request timeout in v1.

Long local generations and Codex tool loops must be allowed to run for a long time.

Use low-level transport and server timeouts instead of a total request timeout:

* `http.Server.ReadHeaderTimeout = 10s`
* `http.Server.ReadTimeout = 0`
* `http.Server.WriteTimeout = 0`
* `http.Server.IdleTimeout = 120s`
* transport dial timeout `10s`
* transport TLS handshake timeout `10s`
* transport response header timeout `0`
* transport expect-continue timeout `1s`
* transport idle connection timeout `90s`

Also set `Transport.DisableCompression = true` so the proxy forwards upstream bytes as-is and does not transparently gunzip responses before logging or forwarding them.

### Cancellation behavior

* If the client disconnects, cancel the upstream request context immediately.
* If the upstream request fails before any response headers are written, return `502 Bad Gateway`.
* If the upstream fails after headers have already been sent, do not try to rewrite the status code. Record the stream failure in logs and stop forwarding.
* If logging fails, do not fail the proxied request. Record the internal failure to stderr and in runtime counters if possible.

## Recommended implementation approach

Implement the proxy manually with `net/http` rather than delegating everything to `httputil.ReverseProxy`.

Reason:

* explicit control over request body replay
* explicit per-chunk flushing
* explicit byte counting
* explicit side-channel parsing for NDJSON and SSE
* simpler reasoning about what is and is not mutated

The core runtime should use:

* one `http.Server`
* one shared `http.Client` with a custom `http.Transport`
* one events log writer goroutine
* one chunk log writer goroutine
* concurrency-safe in-memory counters

## Required repository layout

Create the repository with this shape:

```text
ollama-tap/
  README.md
  go.mod
  go.sum
  Dockerfile
  cmd/
    ollama-tap/
      main.go
  internal/
    config/
      config.go
      config_test.go
    internalapi/
      handler.go
    logstore/
      jsonl.go
      rotate.go
    capture/
      request.go
      response.go
      ndjson.go
      sse.go
      truncbuf.go
      truncbuf_test.go
    modeldetect/
      model.go
      model_test.go
    proxy/
      handler.go
      headers.go
      bodybuffer.go
      headers_test.go
      handler_test.go
    stats/
      runtime.go
  scripts/
    curl-smoke.sh
  deploy/
    docker-compose.yaml
    ollama-tap.service
```

Package responsibilities:

* `config`: flags, env vars, defaults, validation
* `internalapi`: `/_tap/*` handlers
* `logstore`: JSONL append and daily UTC rotation
* `capture`: bounded body capture, NDJSON parsing, SSE parsing, text or base64 preview helpers
* `modeldetect`: best-effort model extraction from request metadata
* `proxy`: request duplication, upstream forwarding, response streaming, summary finalization
* `stats`: process-level counters and snapshots

## Runtime configuration

Support both flags and environment variables. Precedence is:

1. explicit flags
2. environment variables
3. built-in defaults

Required config surface:

```bash
OLLAMA_TAP_LISTEN_ADDR=0.0.0.0:11435
OLLAMA_TAP_UPSTREAM=http://192.168.0.13:11434
OLLAMA_TAP_LOG_DIR=./logs
OLLAMA_TAP_CAPTURE_REQUESTS=true
OLLAMA_TAP_CAPTURE_RESPONSES=true
OLLAMA_TAP_CAPTURE_STREAM_CHUNKS=true
OLLAMA_TAP_MAX_REQUEST_CAPTURE_BYTES=1048576
OLLAMA_TAP_MAX_RESPONSE_CAPTURE_BYTES=1048576
OLLAMA_TAP_MAX_CHUNK_CAPTURE_BYTES=4096
OLLAMA_TAP_REQUEST_SPOOL_THRESHOLD_BYTES=8388608
OLLAMA_TAP_REDACT_HEADERS=Authorization,Cookie,X-Api-Key,X-Bf-Vk,X-Bf-Api-Key
```

Equivalent flags:

```bash
ollama-tap \
  --listen 0.0.0.0:11435 \
  --upstream http://192.168.0.13:11434 \
  --log-dir ./logs \
  --capture-requests \
  --capture-responses \
  --capture-stream-chunks \
  --max-request-capture-bytes 1048576 \
  --max-response-capture-bytes 1048576 \
  --max-chunk-capture-bytes 4096 \
  --request-spool-threshold-bytes 8388608
```

Config validation rules:

* `upstream` is required and must be a valid `http` or `https` URL.
* `listen` is required.
* all byte limits must be non-negative integers.
* `request_spool_threshold_bytes` must be greater than or equal to `max_request_capture_bytes`.
* create the log directory if it does not exist.

## Request lifecycle

For every proxied request, execute these steps in order.

### 1. Classify internal versus proxied request

* If the path starts with `/_tap/`, serve the internal endpoint locally and do not contact upstream.
* Otherwise continue with proxy handling.

### 2. Start request summary state

Create a request-scoped summary object with:

* `request_id`
* `started_at`
* `method`
* `path`
* `query`
* `client_addr`
* `upstream`

`request_id` should be a UUID. UUIDv7 is preferred if convenient, UUIDv4 is acceptable.

### 3. Duplicate the request body without losing transparency

This is one of the most important implementation details.

Requirements:

* The full request body must be forwarded upstream.
* Logging capture limits must not truncate what is forwarded.
* Large request bodies must not force the proxy to keep everything in memory.

Implementation rules:

* If there is no request body, use `nil`.
* If `Content-Length` is known and less than or equal to `request_spool_threshold_bytes`, read the full body into memory.
* If `Content-Length` is unknown or larger than `request_spool_threshold_bytes`, stream the body into a temp file while also copying the first `max_request_capture_bytes` bytes into a bounded capture buffer.
* After duplication, the outbound request body must replay the full captured payload from memory or from the temp file.
* Delete any temp file after request completion.
* Do not reject large bodies solely because logging capture is bounded.

What to record from the request body:

* total request byte count
* first `max_request_capture_bytes` bytes
* whether capture was truncated

### 4. Build the outbound request

Create the upstream request with `http.NewRequestWithContext` using the duplicated body reader.

Copy:

* method
* path
* raw query
* content length when known
* all end-to-end headers

Set:

* `req.URL.Scheme` to upstream scheme
* `req.URL.Host` to upstream host
* `req.Host` to upstream host

### 5. Detect request metadata for logging

Before sending upstream, perform best-effort request inspection.

Record:

* `model`
* `stream`
* `request_content_type`

Detection rules:

* If request body is JSON and has a top-level string field `model`, use it.
* For `GET /v1/models/{model}`, derive `model` from the final path segment.
* For OpenAI and native Ollama JSON endpoints, inspect the top-level `stream` field when present.
* For `/api/chat` and `/api/generate`, if `stream` is omitted, treat it as `true` for summary classification because Ollama streams by default.
* If model is not detectable, store an empty string.

### 6. Dispatch upstream request

Use the shared `http.Client` and configured transport.

If this step fails before response headers are available:

* return `502 Bad Gateway`
* write a summary record with `status = 0`
* write `error` with the upstream failure string

## Response lifecycle

### 1. Copy response status and headers

When the upstream response arrives:

* record `status`
* record `response_content_type`
* copy upstream response headers to the client after stripping hop-by-hop headers
* call `WriteHeader` once before streaming the body

### 2. Stream the response body to the client immediately

Do not wait for the full response body before sending bytes downstream.

Use a manual loop with a fixed buffer, for example `32 KiB`.

Required behavior:

* read a chunk from upstream
* write that chunk to the client immediately
* if the writer supports `http.Flusher`, call `Flush()` after every successful write
* count raw response bytes
* increment raw response chunk count for each successful write
* mirror the same bytes into capture and semantic parsing side channels

The side channels must never mutate the forwarded bytes.

### 3. Track timing

Record:

* `first_response_byte_at` when the first body bytes are successfully written to the client
* `ended_at` when streaming completes or fails
* `duration_ms = ended_at - started_at`
* `ttfb_ms = first_response_byte_at - started_at` when available

### 4. Handle special response cases

* For `HEAD`, `204`, and `304`, do not attempt body streaming.
* If the client write fails, stop copying, cancel the upstream context, and mark `client_canceled = true` if appropriate.
* If the upstream read fails after partial streaming, record `completed = false` and store the read error string in the summary.

## Streaming classification and semantic parsing

The proxy forwards raw bytes first and parses only as a side effect.

Never let parsing success or failure affect proxy correctness.

### Stream kinds

Classify responses into one of these values for the summary record:

* `json`
* `ndjson`
* `sse`
* `other`

Classification rules:

* if `Content-Type` starts with `text/event-stream`, use `sse`
* else if path is `/api/chat` or `/api/generate` and request is classified as streaming, use `ndjson`
* else if `Content-Type` contains `application/json`, use `json`
* else use `other`

### Native Ollama streaming parser

For `/api/chat` and `/api/generate`, Ollama streams newline-delimited JSON objects and the final object includes timing stats.

Parser requirements:

* accumulate bytes until newline boundaries
* ignore empty lines
* parse each full line as a JSON object
* do not assume chunk boundaries align with JSON object boundaries
* only the final object with `done = true` contributes summary timing fields

Extract when present:

* `done`
* `done_reason`
* `total_duration`
* `load_duration`
* `prompt_eval_count`
* `prompt_eval_duration`
* `eval_count`
* `eval_duration`

For `/api/chat`, if a `message.tool_calls` array appears, keep forwarding unchanged. Tool call extraction is optional for v1 and should not block implementation.

### OpenAI-compatible streaming parser

For `/v1/chat/completions`, `/v1/completions`, and `/v1/responses`, Ollama streams SSE-style events when streaming is enabled.

Parser requirements:

* accumulate data until a blank-line SSE frame terminator
* join multiple `data:` lines per SSE event according to normal SSE rules
* ignore comments and blank events
* ignore `[DONE]`
* parse JSON payloads on a best-effort basis

Extract when present:

* `id`
* `model`
* `usage.prompt_tokens`
* `usage.completion_tokens`
* `usage.total_tokens`

Do not attempt to fully reconstruct assistant text in v1. The goal is stats, not a replay engine.

### Non-streaming JSON parsing

For non-streaming JSON responses, parse only when the full response body is available within the configured response capture limit.

Use that parsed body to extract:

* OpenAI `usage` fields
* native Ollama timing fields
* model name when present in the response

If the response exceeds the capture limit or is not valid JSON, skip semantic extraction and still write normal summary metrics.

## Logging and captured data

Use JSONL because it is easy to append, inspect, grep, and import later.

### Log files

Use UTC date-based rotation and create these files:

```text
logs/
  events-YYYY-MM-DD.jsonl
  chunks-YYYY-MM-DD.jsonl
```

Events file types:

* `request`
* `response`
* `summary`
* `internal_error`

Chunk file types:

* `response_chunk`

File rules:

* JSONL only, one object per line
* UTF-8 output
* timestamps in RFC3339Nano UTC
* directory mode `0750`
* file mode `0640` when practical

### Writer behavior

The log writer must not corrupt lines under concurrency.

Implementation requirements:

* serialize event writes through a single goroutine per output file
* open files in append mode
* rotate when the UTC date changes
* flush writes promptly enough for debugging, but do not fsync every line

Backpressure policy:

* request, response, and summary events are important and should be queued reliably
* chunk events may be dropped if the chunk queue is full
* if chunk records are dropped, increment a runtime counter and expose it in `/_tap/stats`
* never block response streaming for a long time just to preserve chunk logs

### Redaction

Redact these request or response headers in logs by default:

* `Authorization`
* `Cookie`
* `X-Api-Key`
* `X-Bf-Vk`
* `X-Bf-Api-Key`

Redaction rules:

* header matching is case-insensitive
* redact values as `REDACTED`
* redact in logs only, never in forwarded traffic
* body redaction is not required in v1

### Request record schema

Write one request record per proxied request after request duplication succeeds.

Example:

```json
{
  "type": "request",
  "request_id": "018ff6da-...",
  "timestamp": "2026-06-18T22:14:03.123456789Z",
  "method": "POST",
  "path": "/v1/responses",
  "query": "",
  "headers": {
    "content-type": ["application/json"],
    "authorization": ["REDACTED"]
  },
  "content_type": "application/json",
  "model": "qwen3.6:latest",
  "stream": true,
  "body_json": {
    "model": "qwen3.6:latest",
    "input": "..."
  },
  "body_text": null,
  "body_base64": null,
  "captured_bytes": 39211,
  "truncated": false
}
```

Encoding rules:

* lower-case header names in logs for stable output
* if body preview is valid JSON, populate `body_json`
* else if preview is valid UTF-8 text, populate `body_text`
* else populate `body_base64`
* only one of `body_json`, `body_text`, or `body_base64` should be non-null

### Response record schema

Write one response preview record per proxied request after the response completes or fails.

This record stores only the first `max_response_capture_bytes` bytes of the full response body, not the entire body.

Example:

```json
{
  "type": "response",
  "request_id": "018ff6da-...",
  "timestamp": "2026-06-18T22:14:17.991234567Z",
  "status": 200,
  "headers": {
    "content-type": ["text/event-stream"]
  },
  "content_type": "text/event-stream",
  "body_json": null,
  "body_text": "data: {...}\n\n",
  "body_base64": null,
  "captured_bytes": 4096,
  "truncated": true
}
```

### Chunk record schema

If `capture_stream_chunks` is enabled and the response is classified as streaming, write a chunk record for each chunk successfully written to the client.

Example:

```json
{
  "type": "response_chunk",
  "request_id": "018ff6da-...",
  "timestamp": "2026-06-18T22:14:04.551234567Z",
  "index": 12,
  "bytes": 821,
  "capture_text": "data: {...}\n\n",
  "capture_base64": null,
  "captured_bytes": 821,
  "truncated": false
}
```

Chunk capture rules:

* capture at most `max_chunk_capture_bytes` bytes per chunk record
* if preview is valid UTF-8, use `capture_text`
* otherwise use `capture_base64`
* do not parse chunk boundaries semantically for logging; record the raw bytes that were written

### Summary record schema

Write exactly one summary record per proxied request, even when the request fails.

Example:

```json
{
  "type": "summary",
  "request_id": "018ff6da-...",
  "started_at": "2026-06-18T22:14:03.123456789Z",
  "first_response_byte_at": "2026-06-18T22:14:03.345678901Z",
  "ended_at": "2026-06-18T22:14:17.991234567Z",
  "duration_ms": 14868,
  "ttfb_ms": 222,
  "method": "POST",
  "path": "/api/chat",
  "query": "",
  "client_addr": "192.168.0.42:58122",
  "upstream": "http://192.168.0.13:11434",
  "status": 200,
  "model": "qwen3.6:latest",
  "stream": true,
  "stream_kind": "ndjson",
  "request_bytes": 39211,
  "response_bytes": 184532,
  "response_chunks": 244,
  "request_capture_truncated": false,
  "response_capture_truncated": true,
  "prompt_eval_count": 1234,
  "eval_count": 567,
  "total_duration_ns": 1234567890,
  "load_duration_ns": 123456789,
  "prompt_eval_duration_ns": 234567890,
  "eval_duration_ns": 345678901,
  "usage": null,
  "done": true,
  "done_reason": "stop",
  "completed": true,
  "client_canceled": false,
  "error": ""
}
```

Required summary fields:

* request identity and timing
* HTTP method, path, query, status
* client address and upstream address
* model and stream classification
* request and response byte counts
* raw response chunk count
* capture truncation flags
* native Ollama timing fields when available
* OpenAI `usage` object when available
* completion state and error string

If a field is unknown, use the zero value that keeps the schema stable:

* empty string for missing strings
* `0` for missing integer counters
* `false` for missing booleans
* `null` for missing `usage`

## Health and local stats endpoints

Expose these internal endpoints:

```text
GET /_tap/health
GET /_tap/config
GET /_tap/stats
```

### `GET /_tap/health`

This is a proxy self-health endpoint only. It does not need to call upstream.

Example response:

```json
{
  "ok": true,
  "started_at": "2026-06-18T22:00:00Z",
  "upstream": "http://192.168.0.13:11434"
}
```

### `GET /_tap/config`

Return sanitized effective config.

Rules:

* include effective listen address, upstream URL, log dir, capture flags, and byte limits
* do not include unredacted secrets if more config is added later

### `GET /_tap/stats`

Return in-memory counters since process start.

Example:

```json
{
  "started_at": "2026-06-18T22:00:00Z",
  "upstream": "http://192.168.0.13:11434",
  "requests_total": 42,
  "requests_active": 1,
  "requests_completed": 41,
  "requests_failed": 1,
  "client_canceled": 0,
  "bytes_in": 123456,
  "bytes_out": 987654,
  "chunk_records_written": 244,
  "chunk_records_dropped": 0,
  "event_log_write_errors": 0
}
```

Use atomics or another concurrency-safe approach.

## Build and process behavior

The binary should:

* load config
* initialize logging and counters
* build the upstream HTTP client
* register `/_tap/*` routes plus a catch-all proxy handler
* serve until interrupted
* shut down gracefully on `SIGINT` or `SIGTERM`

Graceful shutdown requirements:

* stop accepting new requests
* give active requests a short drain window, for example `5s`
* close log writers cleanly

## Required tests

Add automated tests. This repo is greenfield, so tests are part of the implementation, not an optional follow-up.

### Unit tests

At minimum, add unit tests for:

* config parsing and precedence
* header stripping and forwarding header injection
* bounded capture buffer truncation behavior
* model detection from request bodies and `/v1/models/{model}` paths
* native NDJSON parser with split object boundaries across reads
* SSE parser with split frame boundaries across reads

### Integration tests

Use `httptest.Server` to simulate an Ollama upstream and verify end-to-end behavior.

Required integration scenarios:

1. `GET /api/tags` is forwarded and the response body is preserved.
2. `GET /v1/models` is forwarded and the response body is preserved.
3. `POST /api/chat` streaming NDJSON reaches the client incrementally rather than after upstream completion.
4. `POST /v1/chat/completions` streaming SSE reaches the client incrementally.
5. `POST /v1/responses` non-streaming JSON forwards unchanged and usage extraction works when present.
6. large request bodies spill to temp file and are still forwarded exactly.
7. hop-by-hop headers are stripped and `X-Forwarded-*` headers are set.
8. gzip or other content encoding is preserved because the proxy does not auto-decompress.
9. client cancellation cancels the upstream request context.
10. `/_tap/health` and `/_tap/stats` are served locally and never forwarded upstream.

For the streaming tests, deliberately delay the upstream between chunks and assert that the client sees early bytes before the upstream sends the final chunk.

## Manual smoke tests

Add `scripts/curl-smoke.sh` that runs the following checks against a real upstream.

### 1. Proxy health

```bash
curl http://localhost:11435/_tap/health
```

Expected:

* HTTP 200
* JSON with `ok: true`

### 2. Native Ollama version

```bash
curl http://localhost:11435/api/version
```

Expected:

* same JSON behavior as direct upstream

### 3. Native Ollama tags

```bash
curl http://localhost:11435/api/tags
```

Expected:

* same body and status as direct upstream

### 4. OpenAI-compatible models

```bash
curl http://localhost:11435/v1/models
```

Expected:

* same behavior as direct upstream

### 5. Non-streaming OpenAI chat completion

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
* request record written
* response preview written
* summary record written

### 6. Streaming OpenAI chat completion

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
* chunk records are written incrementally

### 7. Native Ollama chat streaming

```bash
curl -N -X POST http://localhost:11435/api/chat \
  -H "Content-Type: application/json" \
  -d '{
    "model": "qwen3.6:latest",
    "messages": [
      {"role": "user", "content": "Count from 1 to 20 slowly."}
    ]
  }'
```

Expected:

* newline-delimited JSON objects arrive progressively
* final summary includes Ollama timing fields when upstream provides them

### 8. Codex smoke test

Point Codex to `ollama-tap`, then ask it to run:

```text
list files and tell me what repo this is
```

Expected:

* Codex still behaves normally through the proxy
* logs show the full request and response sequence

## README requirements

The generated README should include:

* what `ollama-tap` is
* why it exists
* quickstart commands
* required environment variables
* an example Codex config
* where logs go
* what `/_tap/*` endpoints exist
* how to run tests

## Docker and systemd deliverables

### Dockerfile

Create a minimal production Dockerfile that builds a static Go binary and runs it as the container entrypoint.

### Docker Compose

Provide `deploy/docker-compose.yaml` similar to:

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
      OLLAMA_TAP_CAPTURE_REQUESTS: "true"
      OLLAMA_TAP_CAPTURE_RESPONSES: "true"
      OLLAMA_TAP_CAPTURE_STREAM_CHUNKS: "true"
    volumes:
      - ./logs:/logs
```

### systemd unit

Provide `deploy/ollama-tap.service` similar to:

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

## Example Codex config

If Codex is using the OpenAI-compatible responses API:

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

If Codex previously worked against Ollama without `/v1`, keep the same path shape and change only host and port:

```toml
base_url = "http://192.168.0.55:11435"
```

## Implementation order

Implement in this order:

1. config loading, HTTP server startup, internal endpoints, and runtime counters
2. manual proxy path with request duplication, header copying, and raw response streaming
3. JSONL event logging and summary records
4. request and response preview capture
5. native NDJSON and OpenAI SSE semantic parsers for stats extraction
6. tests, README, Dockerfile, Docker Compose, and systemd unit

## Final engineering rule

The proxy should be boring.

If direct Ollama works and `ollama-tap` does not, treat that as a proxy bug unless the upstream itself is failing.
