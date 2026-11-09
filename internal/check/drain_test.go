package check_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/check"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/cluster"
)

var drainOpts = check.DrainOptions{
	Interval: time.Minute,
	Timeout:  3 * time.Minute,
	Sleep:    func(context.Context, time.Duration) error { return nil },
}

func TestDrainPassesWhenBlueReceivesNoMoreWrites(t *testing.T) {
	blue := newCluster("blue", 2)
	blue.setInserted([]int{1000, 1000}, []int{0, 0})

	res := check.WaitDrained(t.Context(), blue, drainOpts)

	if res.Level != check.Pass {
		t.Errorf("drain = %s %q", res.Level, res.Message)
	}
}

func TestDrainWaitsForLateWritersToStop(t *testing.T) {
	blue := newCluster("blue", 2)
	// Writes during the first interval, none during the second.
	blue.setInserted([]int{1000, 1600, 1600, 1600}, []int{0, 0, 0, 0})

	res := check.WaitDrained(t.Context(), blue, drainOpts)

	if res.Level != check.Pass {
		t.Errorf("drain = %s %q", res.Level, res.Message)
	}
}

func TestDrainFailsWhenSomethingStillWritesToBlueAtTheTimeout(t *testing.T) {
	blue := newCluster("blue", 2)
	blue.setInserted([]int{1000, 1600, 2200, 2800, 3400, 4000, 4600, 5200}, []int{0, 0, 0, 0, 0, 0, 0, 0})

	res := check.WaitDrained(t.Context(), blue, drainOpts)

	if res.Level != check.Fail || !strings.Contains(res.Message, "10 rows/s") {
		t.Errorf("drain = %s %q, want a failure reporting 10 rows/s", res.Level, res.Message)
	}
}

func TestResetRollupCacheCallsEveryVMSelectPod(t *testing.T) {
	green := newCluster("green", 3)
	green.addPod(cluster.Select, "vmselect-green-1", "v1.150.0", nil)

	if err := cluster.ResetRollupCache(t.Context(), green); err != nil {
		t.Fatal(err)
	}

	want := []string{"vmselect-green-0/internal/resetRollupResultCache", "vmselect-green-1/internal/resetRollupResultCache"}
	if !slices.Equal(green.podGets, want) {
		t.Errorf("calls = %v, want %v", green.podGets, want)
	}
}

func TestDrainDoesNotPassWhenVMInsertPodsRestartDuringEveryMeasurement(t *testing.T) {
	blue := newCluster("blue", 2)
	// Every measurement sees the counter drop: the pod restarted, and how much it received since is unknown.
	blue.setInserted([]int{1000, 50, 1000, 50, 1000, 50}, []int{0, 0, 0, 0, 0, 0})

	res := check.WaitDrained(t.Context(), blue, drainOpts)

	if res.Level != check.Fail || !strings.Contains(res.Message, "restarted") {
		t.Errorf("drain = %s %q, want a failure saying the pods restarted", res.Level, res.Message)
	}
}

func TestDrainMeasuresAgainAfterAPodRestart(t *testing.T) {
	blue := newCluster("blue", 2)
	blue.setInserted([]int{1000, 50, 50, 50}, []int{0, 0, 0, 0})

	res := check.WaitDrained(t.Context(), blue, drainOpts)

	if res.Level != check.Pass {
		t.Errorf("drain = %s %q", res.Level, res.Message)
	}
	if n := len(blue.podGets); n != 4 {
		t.Errorf("scraped %d times, want 2 measurements of 2 scrapes on the vminsert pod", n)
	}
}

func TestDrainReportsTheWriterWhenTheLastMeasurementIsIncomplete(t *testing.T) {
	blue := newCluster("blue", 2)
	// Writes in the first two windows, then a restart in the last one.
	blue.setInserted([]int{1000, 1600, 1600, 2200, 2200, 50}, []int{0, 0, 0, 0, 0, 0})

	res := check.WaitDrained(t.Context(), blue, drainOpts)

	if res.Level != check.Fail || !strings.Contains(res.Message, "10 rows/s") {
		t.Errorf("drain = %s %q, want the writer's 10 rows/s, not an unknown result", res.Level, res.Message)
	}
}

func TestDrainDetectsARestartAlthoughTheCounterGrewBackPastItsOldValue(t *testing.T) {
	blue := newCluster("blue", 2)
	// The pod restarted at second 2000 and received 1000 rows since: the counter looks unchanged.
	blue.setInsertedWithStarts([]int{1000, 1000, 1000, 1000, 1000, 1000}, []int{0, 0, 0, 0, 0, 0},
		[]int{1000, 2000, 2000, 3000, 3000, 4000})

	res := check.WaitDrained(t.Context(), blue, drainOpts)

	if res.Level != check.Fail || !strings.Contains(res.Message, "restarted") {
		t.Errorf("drain = %s %q, want an unknown result because the pod restarted in every window", res.Level, res.Message)
	}
}
