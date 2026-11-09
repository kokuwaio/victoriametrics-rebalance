package migration

import (
	"github.com/VictoriaMetrics/metrics"
)

// Metrics are the tool's own Prometheus metrics. A nil *Metrics records nothing.
type Metrics struct {
	chunks      *metrics.Counter
	bytes       *metrics.Counter
	retries     *metrics.Counter
	duration    *metrics.PrometheusHistogram
	syncedUntil *metrics.Gauge
	rateLimit   *metrics.Gauge
	selectUtil  *metrics.Gauge
	selectP99   *metrics.Gauge
}

// NewMetrics registers the metrics in set.
func NewMetrics(set *metrics.Set) *Metrics {
	return &Metrics{
		chunks:  set.NewCounter("vmrebalance_chunks_total"),
		bytes:   set.NewCounter("vmrebalance_copied_bytes_total"),
		retries: set.NewCounter("vmrebalance_chunk_retries_total"),
		duration: set.NewPrometheusHistogramExt("vmrebalance_chunk_duration_seconds",
			[]float64{1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600, 7200, 21600, 86400}),
		syncedUntil: set.NewGauge("vmrebalance_synced_until_timestamp_seconds", nil),
		rateLimit:   set.NewGauge("vmrebalance_rate_limit_bytes_per_second", nil),
		selectUtil:  set.NewGauge("vmrebalance_blue_select_concurrency_ratio", nil),
		selectP99:   set.NewGauge(`vmrebalance_blue_query_duration_seconds{quantile="0.99"}`, nil),
	}
}

func (m *Metrics) chunkDone(c CopyStats, seconds float64, syncedUntil float64) {
	if m == nil {
		return
	}
	m.chunks.Inc()
	m.bytes.Add(int(c.Bytes))
	m.duration.Update(seconds)
	m.syncedUntil.Set(syncedUntil)
}

func (m *Metrics) retry() {
	if m != nil {
		m.retries.Inc()
	}
}

func (m *Metrics) rate(bytesPerSecond int64) {
	if m != nil {
		m.rateLimit.Set(float64(bytesPerSecond))
	}
}

func (m *Metrics) load(l Load) {
	if m != nil {
		m.selectUtil.Set(l.Utilization)
		m.selectP99.Set(l.QueryP99.Seconds())
	}
}
