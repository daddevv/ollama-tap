# ollama-tap

A transparent HTTP proxy for Ollama that records request/response metadata and provides real-time observability.

## Quick Overview

`ollama-tap` sits between your AI client (e.g. Codex CLI, a browser extension) and an Ollama server. It forwards every request unmodified while capturing:

- Request method, path, headers, and body preview
- Response status code, headers, and body preview
- Streaming chunks (NDJSON or SSE) with on-the-fly parsing for token counts and timing
- Per-request summary records

If your client works with Ollama directly, it works through `ollama-tap` too. Just point it at the proxy's listen address instead of Ollama's.

## Requirements

### System dependencies

| Tool | When needed | Install example |
|---|---|---|
| [Go 1.23+](https://go.dev/dl/) | Building from source | `sudo pacman -S go` (Arch Linux), `sudo apt install golang-go` (Debian/Ubuntu), `brew install go` (macOS), `choco install golang` (Windows) |
| [curl](https://curl.se/) | Smoke tests only | Included in most OSes; `sudo pacman -S curl` (Arch Linux), `sudo apt install curl` (Debian/Ubuntu), `brew install curl` (macOS), `choco install curl` (Windows) |
| [jq](https://jqlang.github.io/jq/) | Smoke tests only | `sudo pacman -S jq` (Arch Linux), `sudo apt install jq` (Debian/Ubuntu), `brew install jq` (macOS), `choco install jq` (Windows) |

> **Note:** If you deploy via Docker or systemd (below), Go, curl, and jq are not needed at runtime.

### Prerequisites

You need an Ollama server running somewhere accessible on your network (or locally). This proxy does **not** start or embed Ollama — it only mirrors its traffic.

## Quickstart

```bash
# Point to your Ollama server
export OLLAMA_TAP_UPSTREAM=http://localhost:11434

# Start the proxy on :11435 (default)
go run ./cmd/ollama-tap/
```

Point your client at `http://localhost:11435` instead of wherever Ollama was running before.

### Enable full capture

By default no request/response data is logged. To record everything, set the capture flags before starting:

```bash
export OLLAMA_TAP_UPSTREAM=http://localhost:11434
export OLLAMA_TAP_LOG_DIR=/tmp/ollama-tap
export OLLAMA_TAP_CAPTURE_REQUESTS=true
export OLLAMA_TAP_CAPTURE_RESPONSES=true
export OLLAMA_TAP_CAPTURE_STREAM_CHUNKS=true
go run ./cmd/ollama-tap/
```

Logged files are written to `$OLLAMA_TAP_LOG_DIR` as JSONL (one JSON object per line).

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `OLLAMA_TAP_LISTEN_ADDR` | `:11435` | Address to listen on |
| `OLLAMA_TAP_UPSTREAM` | *(required)* | Ollama server URL (e.g. `http://192.168.0.13:11434`) |
| `OLLAMA_TAP_LOG_DIR` | `/tmp/ollama-tap` | Directory for JSONL logs |
| `OLLAMA_TAP_CAPTURE_REQUESTS` | `false` | Record request metadata |
| `OLLAMA_TAP_CAPTURE_RESPONSES` | `false` | Record response preview data |
| `OLLAMA_TAP_CAPTURE_STREAM_CHUNKS` | `false` | Record streaming chunk data |

## Internal Endpoints

These endpoints are accessible at the proxy's listen address for monitoring:

| Endpoint | Method | Description |
|---|---|---|
| `/_tap/health` | GET | Health check — returns `{"ok":true}` |
| `/_tap/stats` | GET | In-memory counters snapshot |
| `/_tap/dashboard` | GET | Live observability dashboard (real-time charts) |
| `/_tap/dashboard/api/snapshot` | GET | Current live state (active connections, uptime, total requests) |
| `/_tap/dashboard/api/history?minutes=60` | GET | Time-series history for charting (delta snapshots) |
| `/_tap/dashboard/api/models` | GET | Per-model token usage map |

The `/stats` response includes: `total_requests`, `uptime`, `last_request`, `streaming_connections`, `non_streaming_connections`, `active_connections`, `uplink_bytes`, `downlink_bytes`, `failed_requests`.

## Logging Format

All log files are JSONL. Each field is optional (omitted when empty).

### Request logs (`request_*.jsonl`)

One file per request. Example:

```json
{"id":"1234567890","method":"POST","path":"/v1/chat/completions","url":"/v1/chat/completions","req_type":"openai_chat","headers":{"Content-Type":["application/json"]},"body_preview":"{\"model\":\"qwen3.6:latest\",\"messages\":[...]}",
```

| Field | Description |
|---|---|
| `id` | Unique request ID (Unix nanoseconds) |
| `method`, `path`, `url` | HTTP method and URL path |
| `req_type` | Classification: `openai_chat`, `ollama_native`, etc. |
| `headers` | Request headers (snapshot) |
| `body_preview` | First 4 KiB of the request body |
| `body_truncated` | `true` if the captured body exceeded 1 MiB |

### Response preview (`response_*.jsonl`)

```json
{"id":"1234567890","status_code":200,"headers":{"Content-Type":["application/json"]},"body_preview":"{\"id\":\"chatcmpl-...\",\"choices\":[...]}",
```

| Field | Description |
|---|---|
| `id`, `status_code` | Request ID and HTTP status from Ollama |
| `headers` | Response headers (hop-by-hop stripped) |
| `body_preview` | First 4 KiB of the response body |
| `body_truncated` | `true` if the captured body exceeded 1 MiB |

### Stream chunks (`chunks_*.jsonl`)

```json
{"id":"1234567890","chunk_type":"text","model":"qwen3.6:latest","delta":"Hello","done":false,"time":"2024-..."}
{"id":"1234567890","chunk_type":"usage","model":"qwen3.6:latest","token_count":42,"done":true,"time":"2024-..."}
```

### Summary (`summary_*.jsonl`)

One per request, written after the response finishes. Includes timing, byte counts, and token usage where available.

## Example Config (Codex)

If using the OpenAI-compatible responses API:

```toml
[model_providers.ollama_tap]
name = "Ollama Tap"
base_url = "http://192.168.0.55:11435/v1"
wire_api = "responses"
stream_idle_timeout_ms = 600000
```

Or for direct Ollama paths without `/v1`:

```toml
[model_providers.ollama_tap]
name = "Ollama Tap"
base_url = "http://192.168.0.55:11435"
```

## Dashboard

The built-in dashboard (`/_tap/dashboard`) provides real-time observability with zero external dependencies — all data is collected in-process and served via Go's embedded file server. Open `http://<listen-addr>/_tap/dashboard` in your browser to see:

- **Live counters**: active connections, total requests, streaming count, uptime
- **Requests/sec line chart**: over the last ~60 minutes (auto-refreshes every 5s)
- **Success/failure rate bar chart**: green = successful, red = failed per interval
- **Model usage table**: per-model prompt/completion/total token counts and request frequency

No setup required — the dashboard is compiled into the binary via Go's `embed` directive. The underlying metrics store runs on a 5-second tick with a ring buffer of ~720 entries (~60 minutes), so data persists in memory without any disk I/O.

## Deploying

### Docker

```bash
docker build -t ollama-tap .
docker run -d \
  --name ollama-tap \
  -p 11435:11435 \
  -e OLLAMA_TAP_UPSTREAM=http://host.docker.internal:11434 \
  -v ./logs:/logs \
  ollama-tap
```

### Docker Compose

See [`deploy/docker-compose.yaml`](deploy/docker-compose.yaml).

### systemd

See [`deploy/ollama-tap.service`](deploy/ollama-tap.service).

### Graceful shutdown

The proxy handles `SIGINT` and `SIGTERM`: stops accepting new requests, waits up to 30 seconds for in-flight requests to complete, then exits.

## Testing

Run unit tests:

```bash
go test ./...
```

Run smoke tests against a live proxy (requires **curl** and **jq**):

```bash
export OLLAMA_TAP_PROXY=http://localhost:11435
bash scripts/curl-smoke.sh
```

## Design Principles

1. **Transparency first**: If direct Ollama works, `ollama-tap` must also work with the same client config.
2. **Boring proxy**: No request/response mutation in v1. No model routing, auth, or schema transformation.
3. **Low overhead**: Streaming responses are forwarded incrementally without buffering. Parsing for stats is done on-the-fly.
4. **Simple deployment**: Single binary, environment-variable config, systemd and Docker support.

## License

See [LICENSE](LICENSE).
