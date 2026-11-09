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

// rowsInserted counts the rows a vminsert pod accepted.
const rowsInserted = "vm_rows_inserted_total"

// Sleeper waits for d or until ctx is done.
type Sleeper func(ctx context.Context, d time.Duration) error

// insertRate measures the rows per second a cluster's vminsert pods accept, from two scrapes of
// vm_rows_inserted_total window apart. keep filters the counters by their labels (nil keeps all).
//
// Pods that restarted between the scrapes (another vm_app_start_timestamp, or the counter went down) or appeared or
// vanished in between are left out of the rate, and complete is false: what those pods received is unknown, so a
// rate of 0 doesn't prove there were no writes.
func insertRate(ctx context.Context, c cluster.Client, window time.Duration, sleep Sleeper,
	keep func(map[string]string) bool) (rate float64, complete bool, err error) {
	type reading struct{ rows, started float64 }
	read := func() (map[string]reading, error) {
		samples, err := scrapeAll(ctx, c, cluster.Insert)
		if err != nil {
			return nil, err
		}
		out := map[string]reading{}
		for pod, s := range samples {
			out[pod] = reading{rows: vm.Sum(s, rowsInserted, keep), started: vm.Sum(s, appStarted, nil)}
		}
		return out, nil
	}
	before, err := read()
	if err != nil {
		return 0, false, err
	}
	if err := sleep(ctx, window); err != nil {
		return 0, false, err
	}
	after, err := read()
	if err != nil {
		return 0, false, err
	}
	var rows float64
	complete = len(before) == len(after)
	for pod, a := range after {
		b, ok := before[pod]
		if !ok || a.started != b.started || a.rows < b.rows {
			complete = false
			continue
		}
		rows += a.rows - b.rows
	}
	return rows / window.Seconds(), complete, nil
}

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
