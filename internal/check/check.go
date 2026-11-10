// Package check runs the read-only checks of a migration: preflight, verify and the drain check after cutover.
package check

import (
	"context"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/cluster"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/vm"
)

// Level is the outcome of one check.
type Level int

const (
	Pass Level = iota
	Warn
	Fail
)

func (l Level) String() string {
	switch l {
	case Pass:
		return "PASS"
	case Warn:
		return "WARN"
	default:
		return "FAIL"
	}
}

// Result is the outcome of one named check.
type Result struct {
	Name    string
	Level   Level
	Message string
	// Err is the error that failed the check, if one did, so a caller can tell e.g. missing permissions apart.
	Err error
}

// Report is the outcome of a set of checks.
type Report struct {
	Results []Result
	// DualWriteActive is true when green receives live writes.
	DualWriteActive bool
}

// Failed reports whether any check failed.
func (r Report) Failed() bool { return AnyFailed(r.Results) }

// AnyFailed reports whether any of the results failed.
func AnyFailed(results []Result) bool {
	return slices.ContainsFunc(results, func(r Result) bool { return r.Level == Fail })
}

func (r *Report) add(name string, l Level, format string, args ...any) {
	r.Results = append(r.Results, Result{Name: name, Level: l, Message: fmt.Sprintf(format, args...)})
}

// fail adds a failed check for err.
func (r *Report) fail(name string, err error) {
	r.Results = append(r.Results, Result{Name: name, Level: Fail, Message: err.Error(), Err: err})
}

// rowsInserted counts the rows a vminsert pod accepted.
const rowsInserted = "vm_rows_inserted_total"

// Sleeper waits for d or until ctx is done.
type Sleeper func(ctx context.Context, d time.Duration) error

// insertRate measures the rows per second a cluster's vminsert pods accept, from two scrapes of
// vm_rows_inserted_total window apart. keep filters the counters by their labels (nil keeps all). It also reports
// what the pods still have to deliver to vmstorage, see ingestion.
//
// Pods that restarted between the scrapes (another vm_app_start_timestamp, or the counter went down), appeared or
// vanished in between, or were terminating are left out of the rate, and complete is false: what those pods
// received or still send is unknown, so a rate of 0 doesn't prove there were no writes.
func insertRate(ctx context.Context, c cluster.Client, window time.Duration, sleep Sleeper,
	keep func(map[string]string) bool) (ingestion, error) {
	type reading struct {
		rows, started, pending, sent, sendErrors float64
		unreachable                              bool
	}
	terminating := false // a vminsert pod shut down during the window, so its last rows weren't measured
	read := func() (map[string]reading, error) {
		pods, err := c.Pods(ctx, cluster.Insert)
		if err != nil {
			return nil, err
		}
		terminating = terminating || slices.ContainsFunc(pods, cluster.Terminating)
		samples, err := scrapeAll(ctx, c, cluster.Insert)
		if err != nil {
			return nil, err
		}
		out := map[string]reading{}
		for pod, s := range samples {
			out[pod] = reading{
				rows:        vm.Sum(s, rowsInserted, keep),
				started:     vm.Sum(s, appStarted, nil),
				pending:     vm.Sum(s, rowsPending, nil),
				sent:        vm.Sum(s, rowsSent, nil),
				sendErrors:  vm.Sum(s, sendErrors, nil),
				unreachable: storageUnreachable(s),
			}
		}
		return out, nil
	}
	before, err := read()
	if err != nil {
		return ingestion{}, err
	}
	if err := sleep(ctx, window); err != nil {
		return ingestion{}, err
	}
	after, err := read()
	if err != nil {
		return ingestion{}, err
	}
	var rows float64
	in := ingestion{complete: len(before) == len(after) && !terminating}
	for pod, a := range after {
		b, ok := before[pod]
		in.pending += a.pending
		in.storageUnreachable = in.storageUnreachable || a.unreachable || b.unreachable
		if !ok || a.started != b.started || a.rows < b.rows {
			in.complete = false
			continue
		}
		rows += a.rows - b.rows
		in.delivered = in.delivered || a.sent != b.sent
		in.sendFailed = in.sendFailed || a.sendErrors != b.sendErrors
	}
	in.rate = rows / window.Seconds()
	return in, nil
}

// ingestion is what insertRate measured.
type ingestion struct {
	// rate is the rows per second the vminsert pods accepted.
	rate float64
	// complete is false when a pod restarted, appeared, vanished or was terminating during the window.
	complete bool
	// pending is the rows the vminsert pods buffer for vmstorage at the end of the window.
	pending float64
	// delivered is true when the pods sent rows to vmstorage during the window: rows were still on their way.
	delivered bool
	// sendFailed is true when a send to vmstorage failed during the window. A batch whose delivery keeps failing
	// isn't counted as pending; a node that accepts connections but never answers shows only as failed sends.
	sendFailed bool
	// storageUnreachable is true when a pod saw a vmstorage node as unreachable at either scrape. Read-only nodes
	// don't count: vminsert sends their rows to the other nodes.
	storageUnreachable bool
}

// done reports whether the window proves that the pods neither accepted nor still deliver any rows.
func (in ingestion) done() bool {
	return in.rate == 0 && in.complete && in.pending == 0 && !in.delivered && !in.sendFailed && !in.storageUnreachable
}

// storageUnreachable reports whether a vminsert pod's metrics show a vmstorage node it can't reach.
func storageUnreachable(samples []vm.Sample) bool {
	for _, s := range samples {
		if s.Name == "vm_rpc_vmstorage_is_reachable" && s.Value == 0 {
			return true
		}
	}
	return false
}

const (
	// rowsPending counts rows a vminsert pod buffers for a vmstorage node. A batch the pod is sending right now
	// isn't included.
	rowsPending = "vm_rpc_rows_pending"
	// rowsSent counts rows a vminsert pod delivered to vmstorage nodes.
	rowsSent = "vm_rpc_rows_sent_total"
	// sendErrors counts failed sends of a vminsert pod to vmstorage nodes.
	sendErrors = "vm_rpc_connection_errors_total"
)

// appStarted is when a VictoriaMetrics process started (Unix seconds).
const appStarted = "vm_app_start_timestamp"

// scrape reads /metrics from every running pod of a component and returns, per pod name, the sum of the samples
// named metric that keep accepts (nil keeps all).
func scrape(ctx context.Context, c cluster.Client, comp cluster.Component, metric string,
	keep func(map[string]string) bool) (map[string]float64, error) {
	samples, err := scrapeAll(ctx, c, comp)
	if err != nil {
		return nil, err
	}
	out := map[string]float64{}
	for pod, s := range samples {
		out[pod] = vm.Sum(s, metric, keep)
	}
	return out, nil
}

// scrapeAll reads /metrics from every running pod of a component and returns the samples per pod name.
func scrapeAll(ctx context.Context, c cluster.Client, comp cluster.Component) (map[string][]vm.Sample, error) {
	pods, err := cluster.RunningPods(ctx, c, comp)
	if err != nil {
		return nil, err
	}
	out := map[string][]vm.Sample{}
	for i := range pods {
		body, err := c.PodGet(ctx, &pods[i], comp, "/metrics")
		if err != nil {
			return nil, fmt.Errorf("scrape %s: %w", pods[i].Name, err)
		}
		out[pods[i].Name] = vm.ParseMetrics(body)
	}
	return out, nil
}

func total(perPod map[string]float64) float64 {
	var sum float64
	for _, v := range perPod {
		sum += v
	}
	return sum
}

// FormatBytes formats n in IEC units.
func FormatBytes(n float64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%.0f B", n)
	}
	exp := 0
	for v := n / unit; v >= unit && exp < 5; v /= unit {
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", n/math.Pow(unit, float64(exp+1)), "KMGTPE"[exp])
}
