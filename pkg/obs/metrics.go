package obs

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
)

// Metrics tracks cluster throughput, Raft elections, and tail latencies (p50/p99/p999).
type Metrics struct {
	PutsTotal      atomic.Int64
	GetsTotal      atomic.Int64
	ErrorsTotal    atomic.Int64
	ElectionsTotal atomic.Int64

	mu        sync.Mutex
	latencies []float64 // milliseconds
}

// NewMetrics creates a new Metrics collector.
func NewMetrics() *Metrics {
	return &Metrics{
		latencies: make([]float64, 0, 4096),
	}
}

// RecordLatency records a single request duration in milliseconds.
func (m *Metrics) RecordLatency(ms float64) {
	m.mu.Lock()
	m.latencies = append(m.latencies, ms)
	m.mu.Unlock()
}

// LatencySummary holds exact p50, p99, p999, and max latencies in milliseconds.
type LatencySummary struct {
	Count int
	P50   float64
	P99   float64
	P999  float64
	Max   float64
}

// Summarize computes percentile tail latencies across all recorded samples.
func (m *Metrics) Summarize() LatencySummary {
	m.mu.Lock()
	cp := make([]float64, len(m.latencies))
	copy(cp, m.latencies)
	m.mu.Unlock()

	if len(cp) == 0 {
		return LatencySummary{}
	}
	sort.Float64s(cp)

	pct := func(p float64) float64 {
		idx := int(float64(len(cp)-1) * p)
		return cp[idx]
	}

	return LatencySummary{
		Count: len(cp),
		P50:   pct(0.50),
		P99:   pct(0.99),
		P999:  pct(0.999),
		Max:   cp[len(cp)-1],
	}
}

// String formats a human-readable telemetry line.
func (s LatencySummary) String() string {
	return fmt.Sprintf("n=%d p50=%.2fms p99=%.2fms p999=%.2fms max=%.2fms",
		s.Count, s.P50, s.P99, s.P999, s.Max)
}
