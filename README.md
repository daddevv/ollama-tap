# ollama-tap

A transparent HTTP proxy for Ollama that records request/response metadata and provides real-time observability.

## Purpose

`ollama-tap` sits between a client (e.g. Codex CLI) and an Ollama server, forwarding all traffic unmodified while capturing:

- Request method, path, headers, and body preview
- Response status code, headers, and body preview
- Streaming chunk data (NDJSON or SSE) with on-the-fly parsing for token counts and timing
- Per-request summary records

If direct Ollama works, the same client works through `ollama-tap`.

## Quickstart

```bash
# Set environment variables
export OLLAMA_TAP_UPSTREAM=http://192.168.0.13:11434
export OLLAMA_TAP_LISTEN_ADDR=:11435  # default

# Run the proxy
go run ./cmd/ollama-tap/
```

Point your client at `http://localhost:11435` (or whichever listen address you configured).

### With capture enabled

```bash
export OLLAMA_TAP_LOG_DIR=/tmp/ollama-tap
export OLLAMA_TAP_CAPTURE_REQUESTS=true
export OLLAMA_TAP_CAPTURE_RESPONSES=true
export OLLAMA_TAP_CAPTURE_STREAM_CHUNKS=true
go run ./cmd/ollama-tap/
```

Logs will be written to the configured `OLLAMA_TAP_LOG_DIR` as JSONL files.

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

| Endpoint | Method | Description |
|---|---|---|
| `/_tap/health` | GET | Health check (returns `{"ok":true}`) |
| `/_tap/stats` | GET | In-memory counters snapshot |

Stats response includes:

- `total_requests` — cumulative forwarded requests
- `uptime` — proxy uptime
- `last_request` — timestamp of last request
- `streaming_connections` — count of streaming connections seen
- `non_streaming_connections` — count of non-streaming connections seen
- `active_connections` — currently active upstream connections
- `uplink_bytes` — total bytes sent to upstream
- `downlink_bytes` — total bytes received from upstream
- `failed_requests` — failed upstream requests

## Logging Format

All log files are JSONL (one JSON object per line).

### Request logs (`request_*.jsonl`)

```json
{"id":"1234567890","method":"POST","path":"/v1/chat/completions","url":"/v1/chat/completions","req_type":"openai_chat","headers":{"Content-Type":["application/json"]},"body_preview":"{"model":"qwen3.6:latest","messages":[...]}",
```

### Response preview (`response_*.jsonl`)

```json
{"id":"1234567890","status_code":200,"headers":{"Content-Type":["application/json"]},"body_preview":"{"id":"chatcmpl-...","choices":[...]}",
```

### Stream chunks (`chunks_*.jsonl`)

```json
{"id":"1234567890","chunk_type":"text","model":"qwen3.6:latest","delta":"Hello","done":false,"time":"2024-..."},
{"id":"1234567890","chunk_type":"usage","model":"qwen3.6:latest","token_count":42,"done":true,"time":"2024-..."},
```

### Summary (`summary_*.jsonl`)

```json
{"id":"1234567890","model":"qwen3.6:latest","req_type":"openai_chat","method":"POST","path":"/v1/chat/completions","duration_ms":3421.5,"uplink_bytes":412,"downlink_bytes":12847,"usage_prompt_tokens":10,"usage_completion_tokens":42,"usage_total_tokens":52}
```

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

## Docker

### Build and run

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

## systemd

See [`deploy/ollama-tap.service`](deploy/ollama-tap.service).

## Testing

Run unit tests:

```bash
go test ./...
```

Run smoke tests against a live proxy:

```bash
export OLLAMA_TAP_PROXY=http://localhost:11435
bash scripts/curl-smoke.sh
```

## Design Principles

1. **Transparency first**: If direct Ollama works, `ollama-tap` must also work with the same client config.
2. **Boring proxy**: No request/response mutation in v1. No model routing, auth, or schema transformation.
3. **Low overhead**: Streaming responses are forwarded incrementally without buffering. Parsing for stats is done on-the-fly from the response stream.
4. **Simple deployment**: Single binary, environment-variable config, systemd and Docker support.

## License

See [LICENSE](LICENSE).
