package obs

import "testing"

func TestMetricsPercentiles(t *testing.T) {
	m := NewMetrics()
	for i := 1; i <= 1000; i++ {
		m.RecordLatency(float64(i))
	}
	m.PutsTotal.Add(800)
	m.GetsTotal.Add(200)

	s := m.Summarize()
	if s.Count != 1000 {
		t.Fatalf("Count = %d, want 1000", s.Count)
	}
	if s.P50 < 499 || s.P50 > 501 {
		t.Fatalf("P50 = %.2f, want ~500", s.P50)
	}
	if s.P99 < 989 || s.P99 > 991 {
		t.Fatalf("P99 = %.2f, want ~990", s.P99)
	}
	if s.P999 < 998 || s.P999 > 1000 {
		t.Fatalf("P999 = %.2f, want ~999", s.P999)
	}
}
