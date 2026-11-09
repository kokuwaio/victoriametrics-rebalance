//go:build e2e

// Package e2e runs a full migration on a kind cluster prepared by run.sh.
package e2e

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/vm"
)

const ns = "monitoring"

type e2e struct {
	t   *testing.T
	ctx context.Context
	cs  kubernetes.Interface
	bin string
	// jobLogs holds the logs of the last Job run by runJob.
	jobLogs string
}

func TestMigration(t *testing.T) {
	cfg, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
	if err != nil {
		t.Fatal(err)
	}
	e := &e2e{t: t, ctx: t.Context(), cs: kubernetes.NewForConfigOrDie(cfg), bin: os.Getenv("VMREBALANCE_BIN")}
	if e.bin == "" {
		t.Fatal("VMREBALANCE_BIN is not set; run test/e2e/run.sh")
	}
	_ = e.cs.CoreV1().ConfigMaps(ns).Delete(e.ctx, "vmrebalance.blue.green", metav1.DeleteOptions{})

	at := time.Now().Add(-time.Hour).Truncate(time.Minute)
	e.seedHistory(at)

	t.Run("preflight", func(t *testing.T) {
		e := e.on(t)
		out, err := e.cli("preflight")
		t.Log(out)
		if err != nil {
			t.Fatalf("preflight failed: %v", err)
		}
	})

	t.Run("sync as a Job", func(t *testing.T) {
		e := e.on(t)
		e.runJob("vmrebalance-sync", "sync", "--from=blue", "--to=green", "--retries=1", "--backoff=5s",
			"--step=12h", "--adaptive", "--rate-limit=10MiB/s", "--metrics-addr=:8080")
		if !strings.Contains(e.jobLogs, "serving metrics") {
			t.Error("the sync Job didn't start its metrics server")
		}
		blue := e.query("blue", `sum(count_over_time(e2e_history[4d]))`, at)
		green := e.query("green", `sum(count_over_time(e2e_history[4d]))`, at)
		if blue == 0 || green != blue {
			t.Errorf("samples of e2e_history: blue %.0f, green %.0f", blue, green)
		}
		for _, pod := range []string{"vmstorage-green-0", "vmstorage-green-1"} {
			if rows := e.storageRows(pod); rows == 0 {
				t.Errorf("%s holds no rows: the data was not spread across green's nodes", pod)
			}
		}
	})

	t.Run("verify", func(t *testing.T) {
		e := e.on(t)
		out, err := e.cli("verify", "--top-n=5", "--random-n=5")
		t.Log(out)
		if err != nil {
			t.Fatalf("verify failed: %v", err)
		}
	})

	t.Run("cutover and final sync", func(t *testing.T) {
		e := e.on(t)
		// Cutover: VMAgent stops writing to blue.
		patch := `{"spec":{"remoteWrite":[{"url":"http://vminsert-green.monitoring.svc:8480/insert/0/prometheus/api/v1/write"}]}}`
		if _, err := e.cs.CoreV1().RESTClient().Patch(types.MergePatchType).
			AbsPath("/apis/operator.victoriametrics.com/v1beta1/namespaces", ns, "vmagents", "agent").
			Body([]byte(patch)).DoRaw(e.ctx); err != nil {
			t.Fatal(err)
		}
		// A cutover 15 minutes ago lets the final sync start right away.
		cutover := time.Now().Add(-15 * time.Minute).UTC().Format(time.RFC3339)
		e.runJob("vmrebalance-final", "sync", "--final", "--from=blue", "--to=green", "--cutover-at="+cutover,
			"--drain-interval=1m", "--drain-timeout=5m", "--top-n=5", "--random-n=5")
	})

	t.Run("status", func(t *testing.T) {
		e := e.on(t)
		out, err := e.cli("status")
		t.Log(out)
		if err != nil || !strings.Contains(out, "Final sync:") || strings.Contains(out, "Final sync:        -") {
			t.Errorf("status = %v, %q", err, out)
		}
	})
}

// cli runs the vmrebalance binary against the test clusters, through the API server proxy.
// on returns a copy of e that reports to the subtest t.
func (e *e2e) on(t *testing.T) *e2e {
	c := *e
	c.t, c.ctx = t, t.Context()
	return &c
}

func (e *e2e) cli(args ...string) (string, error) {
	args = append(args, "--namespace="+ns, "--from=blue", "--to=green", "--access=proxy")
	cmd := exec.CommandContext(e.ctx, e.bin, args...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return out.String(), err
}

// seedHistory writes 20 series of e2e_history into blue, one sample every 5 minutes for the 3 days before until.
func (e *e2e) seedHistory(until time.Time) {
	var body strings.Builder
	for ts := until.Add(-72 * time.Hour); !ts.After(until); ts = ts.Add(5 * time.Minute) {
		for s := range 20 {
			fmt.Fprintf(&body, "e2e_history{series=\"%d\"} %d %d\n", s, ts.Unix()%1000, ts.UnixMilli())
		}
	}
	if _, err := e.cs.CoreV1().RESTClient().Post().
		AbsPath("/api/v1/namespaces", ns, "services", "vminsert-blue:http", "proxy",
			"insert/0/prometheus/api/v1/import/prometheus").
		Body([]byte(body.String())).DoRaw(e.ctx); err != nil {
		e.t.Fatalf("seed blue: %v", err)
	}
	e.eventually("seeded data is searchable on blue", 2*time.Minute, func() bool {
		return e.query("blue", `count(e2e_history)`, until) == 20
	})
}

func (e *e2e) query(cluster, q string, at time.Time) float64 {
	get := func(ctx context.Context, path string, query url.Values) ([]byte, error) {
		params := map[string]string{}
		for k := range query {
			params[k] = query.Get(k)
		}
		return e.cs.CoreV1().Services(ns).ProxyGet("http", "vmselect-"+cluster, "8481", "/select/0/prometheus/"+path, params).DoRaw(ctx)
	}
	v, err := vm.QuerySum(e.ctx, get, q, at)
	if err != nil {
		e.t.Fatalf("query %s on %s: %v", q, cluster, err)
	}
	return v
}

func (e *e2e) storageRows(pod string) float64 {
	body, err := e.cs.CoreV1().Pods(ns).ProxyGet("http", pod, "8482", "/metrics", nil).DoRaw(e.ctx)
	if err != nil {
		e.t.Fatalf("scrape %s: %v", pod, err)
	}
	return vm.Sum(vm.ParseMetrics(body), "vm_rows", func(l map[string]string) bool {
		return strings.HasPrefix(l["type"], "storage/")
	})
}

// runJob runs vmrebalance as a Job with the given arguments and fails the test unless it succeeds.
func (e *e2e) runJob(name string, args ...string) {
	jobs := e.cs.BatchV1().Jobs(ns)
	_ = jobs.Delete(e.ctx, name, metav1.DeleteOptions{PropagationPolicy: ptr.To(metav1.DeletePropagationForeground)})
	e.eventually("old Job "+name+" is gone", time.Minute, func() bool {
		_, err := jobs.Get(e.ctx, name, metav1.GetOptions{})
		return err != nil
	})
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: batchv1.JobSpec{
			BackoffLimit: ptr.To[int32](0),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"job-name": name}},
				Spec: corev1.PodSpec{
					ServiceAccountName: "vmrebalance",
					RestartPolicy:      corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:            "vmrebalance",
						Image:           "vmrebalance:e2e",
						ImagePullPolicy: corev1.PullNever,
						Args:            args,
					}},
				},
			},
		},
	}
	if _, err := jobs.Create(e.ctx, job, metav1.CreateOptions{}); err != nil {
		e.t.Fatal(err)
	}
	var done *batchv1.Job
	e.eventually("Job "+name+" finishes", 15*time.Minute, func() bool {
		j, err := jobs.Get(e.ctx, name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		done = j
		return j.Status.Succeeded > 0 || j.Status.Failed > 0
	})
	pods, _ := e.cs.CoreV1().Pods(ns).List(e.ctx, metav1.ListOptions{LabelSelector: "job-name=" + name})
	e.jobLogs = ""
	for _, p := range pods.Items {
		logs, _ := e.cs.CoreV1().Pods(ns).GetLogs(p.Name, &corev1.PodLogOptions{}).DoRaw(e.ctx)
		e.t.Logf("logs of %s:\n%s", p.Name, logs)
		e.jobLogs += string(logs)
	}
	if done.Status.Succeeded == 0 {
		e.t.Fatalf("Job %s failed", name)
	}
}

func (e *e2e) eventually(what string, timeout time.Duration, cond func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			e.t.Fatalf("timed out after %s waiting until %s", timeout, what)
		}
		time.Sleep(5 * time.Second)
	}
}
