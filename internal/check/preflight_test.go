package check_test

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/check"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/cluster"
)

// healthy returns a blue cluster with 2 vmstorage nodes holding 300 bytes each, and a green one with 3 nodes with
// 1000 bytes free each, deduplication on, and dual write active.
func healthy() (blue, green *fakeCluster) {
	blue = newCluster("blue", 2)
	blue.setDisk(300, 5000)
	blue.setInserted([]int{1000, 1300}, []int{0, 0})

	green = newCluster("green", 3)
	green.setDisk(0, 1000)
	green.info.Args[cluster.Storage] = map[string]string{"dedup.minScrapeInterval": "1ms"}
	green.info.Args[cluster.Select] = map[string]string{"dedup.minScrapeInterval": "1ms"}
	// Green receives the same live rows as blue, plus the tool's own native imports, which must not count.
	green.setInserted([]int{500, 800}, []int{0, 5000})
	return blue, green
}

var opts = check.PreflightOptions{
	Headroom:   0.2,
	RateWindow: 30 * time.Second,
	Sleep:      func(context.Context, time.Duration) error { return nil },
}

func result(t *testing.T, r check.Report, name string) check.Result {
	t.Helper()
	for _, res := range r.Results {
		if res.Name == name {
			return res
		}
	}
	t.Fatalf("no result %q in %+v", name, r.Results)
	return check.Result{}
}

func TestPreflightPassesForAReadySetup(t *testing.T) {
	blue, green := healthy()

	r := check.Preflight(t.Context(), blue, green, opts)

	for _, res := range r.Results {
		if res.Level != check.Pass {
			t.Errorf("%s: %s: %s", res.Name, res.Level, res.Message)
		}
	}
	for _, name := range []string{
		check.Operational, check.StorageNodes, check.Version, check.Retention, check.ReplicationFactor,
		check.Dedup, check.Capacity, check.DualWrite, check.PodLabels,
	} {
		result(t, r, name)
	}
	if !r.DualWriteActive {
		t.Error("DualWriteActive = false")
	}
}

func TestPreflightFindsProblems(t *testing.T) {
	for _, tc := range []struct {
		name   string
		break_ func(blue, green *fakeCluster)
		check  string
		level  check.Level
		msg    string
	}{
		{"green not operational", func(_, g *fakeCluster) { g.info.Operational, g.info.Status = false, "expanding" },
			check.Operational, check.Fail, "expanding"},
		{"no more nodes than blue", func(_, g *fakeCluster) { g.info.StorageReplicas = 2 },
			check.StorageNodes, check.Warn, "nothing to rebalance"},
		{"different version", func(_, g *fakeCluster) {
			g.pods[cluster.Storage][1].Spec.Containers[0].Image = "victoriametrics/vmstorage:v1.151.0-cluster"
		}, check.Version, check.Fail, "v1.151.0"},
		{"different retention", func(_, g *fakeCluster) { g.info.Retention = "60d" },
			check.Retention, check.Fail, "60d"},
		{"different replication factor", func(_, g *fakeCluster) { g.info.ReplicationFactor = 2 },
			check.ReplicationFactor, check.Fail, "2"},
		{"no dedup on vmselect", func(_, g *fakeCluster) { delete(g.info.Args, cluster.Select) },
			check.Dedup, check.Fail, "vmselect"},
		{"dedup thins real samples", func(_, g *fakeCluster) {
			g.info.Args[cluster.Storage]["dedup.minScrapeInterval"] = "30s"
			g.info.Args[cluster.Select]["dedup.minScrapeInterval"] = "30s"
		}, check.Dedup, check.Warn, "30s"},
		{"too little free disk on green", func(_, g *fakeCluster) { g.setDisk(0, 200) },
			check.Capacity, check.Fail, "600 B free"},
		{"no running blue vmstorage pods", func(b, _ *fakeCluster) { b.pods[cluster.Storage] = nil },
			check.Capacity, check.Fail, "no running vmstorage pods"},
		{"no dual write, only the tool's imports", func(_, g *fakeCluster) { g.setInserted([]int{500, 500}, []int{0, 5000}) },
			check.DualWrite, check.Fail, "no live writes"},
		{"green gets half of blue's writes", func(_, g *fakeCluster) { g.setInserted([]int{500, 650}, []int{0, 0}) },
			check.DualWrite, check.Warn, "50%"},
		{"PDB selects pods of both clusters", func(b, _ *fakeCluster) {
			b.pdbs = append(b.pdbs, pdb("vmstorage", map[string]string{"app.kubernetes.io/name": "vmstorage"}))
		}, check.PodLabels, check.Fail, "PodDisruptionBudget vmstorage"},
		{"topology spread selector of blue matches green pods", func(b, _ *fakeCluster) {
			b.pods[cluster.Storage][0].Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
				TopologyKey:   "kubernetes.io/hostname",
				LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app.kubernetes.io/name": "vmstorage"}},
			}}
		}, check.PodLabels, check.Fail, "topologySpreadConstraints"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blue, green := healthy()
			tc.break_(blue, green)

			r := check.Preflight(t.Context(), blue, green, opts)

			res := result(t, r, tc.check)
			if res.Level != tc.level || !strings.Contains(res.Message, tc.msg) {
				t.Errorf("%s = %s %q, want %s containing %q", tc.check, res.Level, res.Message, tc.level, tc.msg)
			}
		})
	}
}

func TestPreflightPodLabelsPassWhenSelectorsUseTheInstanceLabel(t *testing.T) {
	blue, green := healthy()
	blue.pdbs = append(blue.pdbs, pdb("vmstorage-blue", map[string]string{
		"app.kubernetes.io/name": "vmstorage", "app.kubernetes.io/instance": "blue",
	}))

	r := check.Preflight(t.Context(), blue, green, opts)

	if res := result(t, r, check.PodLabels); res.Level != check.Pass {
		t.Errorf("pod labels = %s %q", res.Level, res.Message)
	}
}
