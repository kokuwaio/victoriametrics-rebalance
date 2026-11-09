package check_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/check"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/cluster"
)

func selectMetrics(current, capacity, reached int, rangeP99, queryP99 float64) string {
	return "# TYPE vm_concurrent_select_current gauge\n" +
		"vm_concurrent_select_current " + strconv.Itoa(current) + "\n" +
		"vm_concurrent_select_capacity " + strconv.Itoa(capacity) + "\n" +
		"vm_concurrent_select_limit_reached_total " + strconv.Itoa(reached) + "\n" +
		`vm_request_duration_seconds{path="/api/v1/query_range",quantile="0.5"} 0.01` + "\n" +
		`vm_request_duration_seconds{path="/api/v1/query_range",quantile="0.99"} ` + fmtFloat(rangeP99) + "\n" +
		`vm_request_duration_seconds{path="/api/v1/query",quantile="0.99"} ` + fmtFloat(queryP99) + "\n" +
		// The tool's own exports have their own path and must not count as query latency.
		`vm_request_duration_seconds{path="/api/v1/export/native",quantile="0.99"} 60` + "\n"
}

func TestSelectLoadCombinesAllVMSelectPods(t *testing.T) {
	blue := newCluster("blue", 2)
	blue.addPod(cluster.Select, "vmselect-blue-1", "v1.150.0", nil)
	blue.podResp["vmselect-blue-0/metrics"] = []string{selectMetrics(2, 8, 3, 0.4, 1.5)}
	blue.podResp["vmselect-blue-1/metrics"] = []string{selectMetrics(6, 8, 4, 0.2, 0.1)}

	l, err := check.SelectLoad(t.Context(), blue)
	if err != nil {
		t.Fatal(err)
	}

	if l.Utilization != 0.5 || l.LimitReached != 7 || l.QueryP99 != 1500*time.Millisecond {
		t.Errorf("load = %+v, want utilization 0.5, 7 waits, p99 1.5s", l)
	}
}

func TestSelectLoadFailsWithoutVMSelectPods(t *testing.T) {
	blue := newCluster("blue", 2)
	blue.pods[cluster.Select] = nil

	if _, err := check.SelectLoad(t.Context(), blue); err == nil {
		t.Fatal("want an error without vmselect pods")
	}
}

func fmtFloat(f float64) string { return strconv.FormatFloat(f, 'g', -1, 64) }
