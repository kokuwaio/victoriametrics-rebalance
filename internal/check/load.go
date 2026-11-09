package check

import (
	"context"
	"time"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/cluster"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/migration"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/vm"
)

// SelectLoad reads the query load of a cluster from all its vmselect pods: the share of concurrent query slots in
// use, how many queries had to wait for a slot, and the highest p99 latency of instant and range queries (over
// vmselect's 5 minute summary window). The tool's own exports are counted under another path and don't count.
func SelectLoad(ctx context.Context, c cluster.Client) (migration.Load, error) {
	pods, err := scrapeAll(ctx, c, cluster.Select)
	if err != nil {
		return migration.Load{}, err
	}
	var l migration.Load
	var current, capacity, p99 float64
	queryP99 := func(labels map[string]string) bool {
		return labels["quantile"] == "0.99" && (labels["path"] == "/api/v1/query" || labels["path"] == "/api/v1/query_range")
	}
	for _, s := range pods {
		current += vm.Sum(s, "vm_concurrent_select_current", nil)
		capacity += vm.Sum(s, "vm_concurrent_select_capacity", nil)
		l.LimitReached += vm.Sum(s, "vm_concurrent_select_limit_reached_total", nil)
		for _, sample := range s {
			if sample.Name == "vm_request_duration_seconds" && queryP99(sample.Labels) {
				p99 = max(p99, sample.Value)
			}
		}
	}
	if capacity > 0 {
		l.Utilization = current / capacity
	}
	l.QueryP99 = time.Duration(p99 * float64(time.Second))
	return l, nil
}
