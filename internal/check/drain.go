package check

import (
	"context"
	"fmt"
	"time"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/cluster"
)

// Names of the post-cutover checks.
const (
	Drain       = "blue drained"
	RollupCache = "rollup cache"
)

// DrainOptions tune the drain check.
type DrainOptions struct {
	// Interval is the window of one measurement. Blue must receive no rows during one whole window.
	Interval time.Duration
	// Timeout is how long blue may keep receiving writes after cutover, e.g. while VMAgent flushes its queue.
	Timeout time.Duration
	Sleep   Sleeper
}

// WaitDrained measures blue's vminsert ingestion over windows of Interval until one window sees no rows at all, or
// fails when blue still receives writes after Timeout. A writer that still points at blue writes data that is lost
// once blue is deleted, so any row counts, not just a high rate. A writer that pushes less often than Interval can
// slip through, so Interval must be longer than the slowest writer's push interval. A window in which a vminsert pod
// restarted, appeared or vanished proves nothing and is measured again.
//
// Blue also isn't drained while its vminsert pods still deliver rows they accepted, which the final export couldn't
// read yet: rows buffered for vmstorage (pending), rows sent during the window (they were still on their way at its
// start), and a batch whose delivery keeps failing, which shows as failed sends or an unreachable vmstorage node.
// A single send gives up after about a minute, so Interval must be longer than that.
//
// writing reports whether the check failed because blue still received rows, so some writer still points at it. Any
// other failure (an error, rows that wait for delivery, pods that kept restarting) leaves it false.
func WaitDrained(ctx context.Context, blue cluster.Client, o DrainOptions) (res Result, writing bool) {
	if o.Interval <= 0 {
		return Result{Name: Drain, Level: Fail, Message: "drain interval must be positive"}, false
	}
	attempts := max(1, int(o.Timeout/o.Interval))
	var lastWrites float64 // the rate of the latest window that saw writes
	var last ingestion
	for range attempts {
		var err error
		last, err = insertRate(ctx, blue, o.Interval, o.Sleep, nil)
		if err != nil {
			return Result{Name: Drain, Level: Fail, Message: err.Error()}, false
		}
		if last.done() {
			return Result{Name: Drain, Level: Pass, Message: "blue receives no more writes and has delivered every row"}, false
		}
		if last.rate > 0 {
			lastWrites = last.rate
		}
	}
	switch {
	case lastWrites > 0:
		// Reported below.
	case last.storageUnreachable || last.sendFailed:
		return Result{Name: Drain, Level: Fail, Message: fmt.Sprintf(
			"blue's vminsert pods still fail to send to a vmstorage node (unreachable, or not answering) after %s, so "+
				"rows may still wait for delivery: fix blue's vmstorage nodes; the final sync runs again after that", o.Timeout)}, false
	case last.pending > 0 || last.delivered:
		return Result{Name: Drain, Level: Fail, Message: fmt.Sprintf(
			"blue's vminsert pods were still delivering rows to vmstorage after %s: check blue's vmstorage nodes",
			o.Timeout)}, false
	}
	if lastWrites == 0 {
		return Result{Name: Drain, Level: Fail, Message: fmt.Sprintf(
			"blue's vminsert pods restarted, changed or were terminating during every measurement within %s, so it "+
				"is unknown whether blue still receives writes; wait until they are stable (a pod stuck terminating "+
				"must be deleted)", o.Timeout)}, false
	}
	return Result{Name: Drain, Level: Fail, Message: fmt.Sprintf(
		"blue still received %.0f rows/s within %s: some writer still points at blue; point it at green",
		lastWrites, o.Timeout)}, true
}
