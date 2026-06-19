# ollama-tap implementation spec — Dashboard iteration

## Purpose

`ollama-tap` is a transparent HTTP proxy that sits between a client such as Codex and an Ollama server, **now with a real-time web dashboard for observability**.

Primary path:

```
client -> ollama-tap -> Ollama
                ^
                +-> /dashboard (live metrics visualization)
```

## What has been implemented (v1 proxy — unchanged)

The following is already complete and working. This plan **does not** re-implement these:

1. Transparent HTTP proxy for `/api/*` and `/v1/*` paths
2. Streaming passthrough (NDJSON + SSE) without buffering
3. JSONL event logging (request, response, chunk, summary files)
4. `/_tap/health` and `/_tap/stats` counter endpoints
5. Dockerfile, Docker Compose, systemd unit, smoke tests

## New: Dashboard — goals

Add a **single-file embedded web dashboard** that lets me answer these questions in real time while the proxy runs:

1. How many tokens per second is Ollama generating? (over time)
2. How many requests are hitting the proxy per minute? (over time)
3. What is the current success/failure rate?
4. Which models are being used and how many total tokens each?
5. How many connections are active right now?

No external services, no database, no OpenTelemetry. All data lives in-process and is served via a lightweight API + embedded HTML page with charts.js from CDN.

## New: goals — definition of done

The dashboard is complete when:

1. Visiting `/_tap/dashboard` shows a live-updating page with at least:
   - A line chart of **tokens per second** over time
   - A line chart of **requests per minute** over time
   - A bar or pie chart of **success vs failure rate** (last N minutes)
   - A table or list of **models used with total token counts**
   - An **active connections** counter and uptime display
2. Data refreshes automatically without page reload (polling, ~5s interval)
3. All metrics are collected in-process with zero external dependencies
4. `go test ./...` still passes
5. README is updated to document the dashboard

## Architecture overview

### Time-series metrics store

The current `metrics.Metrics` struct only tracks **cumulative counters** (total requests, total bytes, etc.). For charts, we need **periodic snapshots of counter deltas**.

Add a new file: `internal/metrics/store.go` with a ring-buffer time-series store that runs alongside the existing counters.

```
time -> [snapshot_1] [snapshot_2] ... [snapshot_N] <- ring buffer (oldest overwritten)
         0s       5s       10s     ...    (N-1)*5s
```

Design:

- **Ring buffer** of snapshot slots (configurable size, default ~720 entries = 60 minutes at 5s intervals)
- Goroutine ticks every 5 seconds, computes deltas from cumulative counters, stores a snapshot
- Each snapshot captures: timestamp, total_requests_delta, streaming_count_delta, non_streaming_count_delta, failed_requests_delta, uplink_bytes_delta, downlink_bytes_delta, active_connections, usage per model (prompt/total/completion tokens)
- Thread-safe: all access via atomic reads within the tick goroutine; external API reads snapshot data
- No external dependencies

### New metrics counters for dashboard

Extend `metrics.go` or create a companion in `store.go`:

```go
// Track usage per model name across all requests
modelUsage sync.Map // string (modelName) -> *ModelUsage{prompt, completion, total int64}
```

Each time a summary is written with usage data, update the in-memory map. This gives us per-model token totals without touching disk.

### API endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/_tap/dashboard` | GET | Serves the embedded dashboard HTML page (or redirects to index) |
| `/_tap/dashboard/api/snapshot` | GET | Returns current live snapshot (active connections, uptime, total requests) |
| `/_tap/dashboard/api/history?minutes=60` | GET | Returns time-series history for charts (JSON array of intervals) |
| `/_tap/dashboard/api/models` | GET | Returns per-model token usage map |

### Frontend

Single HTML file embedded via Go's `embed.FS`:

- **charts.js from CDN** — no build step, no npm
- Three main visualizations:
  1. **Tokens/sec line chart** (X=timeline, Y=tokens/sec computed from cumulative usage deltas)
  2. **Requests/min line chart** (X=timeline, Y=requests per interval)
  3. **Success/fail rate bar chart** (last N intervals, green = success, red = fail)
- Below charts: summary panels for active connections, uptime, total requests, and a model usage table

## Implementation steps

### Step 1: Time-series metrics store (`internal/metrics/store.go`)

New file. Contains:

```go
type Snapshot struct {
    Timestamp    time.Time `json:"timestamp"`
    TotalReqs    int64     `json:"total_reqs"`
    Streaming    int64     `json:"streaming"`
    NonStreaming int64     `json:"non_streaming"`
    Failures     int64     `json:"failures"`
    UplinkBytes  int64     `json:"uplink_bytes"`
    DownlinkBytes int64    `json:"downlink_bytes"`
    ActiveConn   int64     `json:"active_connections"`

    // Per-model usage at snapshot time
    ModelUsage map[string]ModelUsage `json:"model_usage,omitempty"`
}

type RingStore struct {
    slots   []Snapshot
    size    int       // ring capacity
    writeIdx int      // next write position
    mu      sync.Mutex
    tickSec  int       // sampling interval (default 5)
    lastTick Snapshot  // previous snapshot for delta computation
}

func NewRingStore(capacity int, tickSec int) *RingStore { ... }
func (s *RingStore) Tick(m *Metrics) Snapshot { ... } // goroutine-safe, called every N seconds
func (s *RingStore) History(minutes int) []Snapshot { ... }
func (s *RingStore) Current() Snapshot { ... }
```

Tick computes deltas: `newTotal - oldTotal` for each counter. The consumer turns these into per-second rates by dividing by the tick interval.

### Step 2: Per-model usage tracking (`internal/metrics/model_usage.go`)

New file. Lightweight concurrent map:

```go
type ModelUsageTracker struct {
    data sync.Map // model -> Usage{Prompt, Completion, Total}
}

func (t *ModelUsageTracker) Record(model string, usage parser.OpenAIUsage) { ... }
func (t *ModelUsageTracker) Snapshot() map[string]ModelUsage { ... }
```

Called from the proxy's `logSummary` path when usage data is available.

### Step 3: Wire up store to main.go and proxy

In `cmd/ollama-tap/main.go`:

- Create `RingStore` alongside existing metrics
- Start tick goroutine: `go func() { ticker := time.NewTicker(5s); for range ticker.C { _ = store.Tick(m) } }()`
- Pass `ModelUsageTracker` to proxy (or attach it as a field on `metrics.Metrics`)

In `internal/proxy/proxy.go`:

- In the success path of `ServeHTTP`, call `m.RecordRequest(duration, streaming)` (existing)
- After writing summary with usage, call `tracker.Record(model, usage)` (new)

### Step 4: Dashboard HTTP handlers (`internal/dashboard/`)

New package: `internal/dashboard`

```go
// handler.go — register endpoints on a given mux
func RegisterHandlers(mux *http.ServeMux, store *metrics.RingStore, tracker *metrics.ModelUsageTracker, m *metrics.Metrics)

// assets.go — embedded dashboard HTML+JS
//go:embed assets
var assets embed.FS

// serveDashboard — serves /_tap/dashboard (HTML with charts.js from CDN)
// serveSnapshot — handles /_tap/dashboard/api/snapshot
// serveHistory — handles /_tap/dashboard/api/history
// serveModels — handles /_tap/dashboard/api/models
```

### Step 5: Dashboard HTML+JS frontend

Single file: `internal/dashboard/assets/index.html` (served as the sole embedded asset).

Structure:

- **Top row**: uptime, active connections, total requests (large numbers)
- **Chart area** (2 rows):
  - Tokens/sec over time (line chart, 720 data points max, auto-scaling Y)
  - Requests/min over time (line chart, matching X-axis with tokens/sec)
  - Success/fail rate (bar chart, last N intervals)
- **Model usage table**: model name | prompt tokens | completion tokens | total tokens | requests count
- **Polling loop**: `fetch` to `/api/snapshot`, `/api/history`, `/api/models` every 5 seconds; updates charts in place via chart.js `data.datasets[].data.push()` and `.shift()`

Charts configuration:

- Use `chartjs-plugin-streaming` is not needed — we manage data ourselves
- All three charts share the same time X-axis for correlation
- Responsive layout with CSS Grid or flexbox (no frameworks)
- Dark theme option via CSS media query or toggle button

### Step 6: Wire dashboard into main.go

```go
import "github.com/daddevv/ollama-tap/internal/dashboard"

// after creating store and tracker
dashboard.RegisterHandlers(mux, store, tracker, m)
```

Add to `/_tap/*` prefix handling (already in place via mux).

### Step 7: Update README

- New section: "Dashboard" explaining how to access `/_tap/dashboard`
- Screenshot placeholder or description of what the dashboard shows
- Note that no setup is required — dashboard is built-in

## Updated design principles

The existing principles remain valid. Add one:

1. **Transparency first** (existing)
2. **Boring proxy** (existing)
3. **Low overhead** (existing)
4. **Simple deployment** (existing)
5. **Dashboard data has zero impact on proxy correctness** — metrics collection never blocks or fails the upstream response.

## Updated "non-goals for v1" (remove these, they are no longer non-goals)

- ~~a web UI~~ <-- done
- database storage <- still not in scope
- OpenTelemetry, Langfuse, or external tracing backends <- still not in scope

## Implementation order

1. Ring buffer time-series store (`internal/metrics/store.go`) with tests
2. Per-model usage tracker (`internal/metrics/model_usage.go`)
3. Wire metrics into proxy (record usage per request)
4. Dashboard HTTP handlers and embedded assets (`internal/dashboard/`)
5. Dashboard frontend (HTML + charts.js, all in one file)
6. Wire everything into `main.go`
7. README update

## Frontend implementation notes

### Data flow

```
proxy.ServeHTTP (every request)
  -> metrics.RecordRequest()         (existing counters)
  -> tracker.Record(model, usage)    (new per-model tracking)

tick goroutine (every 5s)
  -> store.Tick(m)                   (snapshots delta of counters)

Dashboard polls (every 5s):
  GET /_tap/dashboard/api/snapshot  -> current live state
  GET /_tap/dashboard/api/history   -> time-series for charts
  GET /_tap/dashboard/api/models    -> model usage table
```

### Chart.js datasets to produce

1. **Tokens/sec** — computed as `(completion_tokens_delta) / (tick_interval_sec)` per interval
2. **Requests/min** — `(total_reqs_delta / tick_sec) * 60` per interval  
3. **Success/Fail rate** — bar chart where each bar has two segments: green = success count, red = fail count for that interval

### Styling direction

- Clean, utilitarian dark theme (fitting for an ops/monitoring tool)
- No flashy gradients or decorative elements
- Charts with muted colors; data should be immediately readable
- Responsive to window resize
- No external CSS framework — hand-written minimal styles

## Testing considerations

- Ring buffer: test wrap-around, history retrieval for various minute ranges
- Model usage tracker: concurrent record calls, snapshot correctness
- Dashboard API handlers: verify JSON structure of each endpoint
- No browser-based tests needed (no framework dependency)
- `go test ./...` must still pass after all changes

## Docker and systemd considerations

No changes to existing Dockerfile or systemd unit — dashboard is built into the binary via Go's `embed`. Dashboard data does not increase log disk usage.
