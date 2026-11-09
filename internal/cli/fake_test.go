package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/cluster"
)

// fakeCluster is a cluster.Client with one pod per component. vmselect reports series series for every day and
// samples samples for the metric "up"; each vminsert scrape returns the next body of inserts (the last repeats).
type fakeCluster struct {
	name    string
	series  int64
	samples float64
	inserts []string
	podGets []string
}

func newFake(name string) *fakeCluster {
	return &fakeCluster{name: name, series: 10, samples: 100, inserts: []string{insertMetrics(1000)}}
}

// insertMetrics is a vminsert /metrics body with rows accepted so far, nothing pending and a reachable vmstorage.
func insertMetrics(rows int) string {
	return fmt.Sprintf("vm_rows_inserted_total{type=\"promremotewrite\"} %d\nvm_app_start_timestamp 1\n"+
		"vm_rpc_rows_pending 0\nvm_rpc_rows_sent_total %d\nvm_rpc_connection_errors_total 0\n"+
		"vm_rpc_vmstorage_is_reachable 1\n", rows, rows)
}

func (f *fakeCluster) Info() cluster.Info {
	return cluster.Info{Name: f.name, Namespace: "monitoring", UID: "uid-" + f.name, Operational: true}
}

func (f *fakeCluster) Pods(_ context.Context, c cluster.Component) ([]corev1.Pod, error) {
	return []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: string(c) + "-" + f.name + "-0", Namespace: "monitoring"}}}, nil
}

func (f *fakeCluster) PodGet(_ context.Context, pod *corev1.Pod, _ cluster.Component, path string) ([]byte, error) {
	f.podGets = append(f.podGets, pod.Name+path)
	switch {
	case path == "/internal/resetRollupResultCache":
		return nil, nil
	case path == "/metrics" && strings.HasPrefix(pod.Name, "vminsert"):
		body := f.inserts[0]
		if len(f.inserts) > 1 {
			f.inserts = f.inserts[1:]
		}
		return []byte(body), nil
	}
	return nil, fmt.Errorf("unexpected GET %s%s", pod.Name, path)
}

func (f *fakeCluster) SelectGet(_ context.Context, path string, _ url.Values) ([]byte, error) {
	var data any
	switch path {
	case "api/v1/status/tsdb":
		data = map[string]any{"totalSeries": f.series, "seriesCountByMetricName": []any{map[string]any{"name": "up", "value": 1}}}
	case "api/v1/query":
		data = map[string]any{"resultType": "vector", "result": []any{map[string]any{"metric": map[string]string{}, "value": []any{1, fmt.Sprint(f.samples)}}}}
	default:
		return nil, fmt.Errorf("unexpected vmselect path %s", path)
	}
	return json.Marshal(map[string]any{"status": "success", "data": data})
}

func (f *fakeCluster) PDBs(context.Context) ([]policyv1.PodDisruptionBudget, error) { return nil, nil }
