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
func WaitDrained(ctx context.Context, blue cluster.Client, o DrainOptions) Result {
	if o.Interval <= 0 {
		return Result{Name: Drain, Level: Fail, Message: "drain interval must be positive"}
	}
	attempts := max(1, int(o.Timeout/o.Interval))
	var lastWrites float64 // the rate of the latest window that saw writes
	for range attempts {
		rate, complete, err := insertRate(ctx, blue, o.Interval, o.Sleep, nil)
		if err != nil {
			return Result{Name: Drain, Level: Fail, Message: err.Error()}
		}
		if rate == 0 && complete {
			return Result{Name: Drain, Level: Pass, Message: "blue receives no more writes"}
		}
		if rate > 0 {
			lastWrites = rate
		}
	}
	if lastWrites == 0 {
		return Result{Name: Drain, Level: Fail, Message: fmt.Sprintf(
			"blue's vminsert pods restarted or changed during every measurement within %s, so it is unknown whether "+
				"blue still receives writes; wait until they are stable", o.Timeout)}
	}
	return Result{Name: Drain, Level: Fail, Message: fmt.Sprintf(
		"blue still received %.0f rows/s within %s: some writer still points at blue; point it at green",
		lastWrites, o.Timeout)}
}
