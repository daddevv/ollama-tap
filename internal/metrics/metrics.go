package metrics

import (
	"sync/atomic"
	"time"
)

type Metrics struct {
	totalRequests      atomic.Int64
	totalUpstreamBytes atomic.Int64
	totalClientBytes   atomic.Int64
	streamingCount     atomic.Int64
	nonStreamingCount  atomic.Int64
	activeConnections  atomic.Int64
	failedRequests     atomic.Int64

	firstSeen   time.Time
	lastSeenSec int64 // Unix seconds, for atomic access
}

func New() *Metrics {
	now := time.Now()
	m := &Metrics{firstSeen: now}
	atomic.StoreInt64(&m.lastSeenSec, now.Unix())
	return m
}

func (m *Metrics) RecordRequest(duration time.Duration, streaming bool) {
	m.totalRequests.Add(1)
	if streaming {
		m.streamingCount.Add(1)
	} else {
		m.nonStreamingCount.Add(1)
	}
	atomic.StoreInt64(&m.lastSeenSec, time.Now().Unix())
}

func (m *Metrics) RecordUpstreamBytes(n int64) { m.totalUpstreamBytes.Add(n) }
func (m *Metrics) RecordClientBytes(n int64)   { m.totalClientBytes.Add(n) }

func (m *Metrics) IncrementActive() { m.activeConnections.Add(1) }
func (m *Metrics) DecrementActive() { m.activeConnections.Add(-1) }
func (m *Metrics) RecordFailure()   { m.failedRequests.Add(1) }

type Snapshot struct {
	TotalRequests     int64  `json:"total_requests"`
	Uptime            string `json:"uptime"`
	LastRequest       int64  `json:"last_request_unix"`
	StreamingCount    int64  `json:"streaming_connections"`
	NonStreamingCount int64  `json:"non_streaming_connections"`
	ActiveConnections int64  `json:"active_connections"`
	UplinkBytes       int64  `json:"uplink_bytes"`
	DownlinkBytes     int64  `json:"downlink_bytes"`
	Failures          int64  `json:"failed_requests"`
}

func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		TotalRequests:     m.totalRequests.Load(),
		Uptime:            time.Since(m.firstSeen).Round(time.Millisecond).String(),
		LastRequest:       atomic.LoadInt64(&m.lastSeenSec),
		StreamingCount:    m.streamingCount.Load(),
		NonStreamingCount: m.nonStreamingCount.Load(),
		ActiveConnections: m.activeConnections.Load(),
		UplinkBytes:       m.totalUpstreamBytes.Load(),
		DownlinkBytes:     m.totalClientBytes.Load(),
		Failures:          m.failedRequests.Load(),
	}
}
