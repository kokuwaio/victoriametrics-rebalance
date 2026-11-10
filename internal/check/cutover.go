package check

import (
	"context"
	"time"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/cluster"
)

// Writes is what WatchWrites measured over one window.
type Writes struct {
	// BlueRate and GreenRate are the rows per second the clusters' vminsert pods accepted. Green's leaves out
	// native imports, which are the tool's own copies.
	BlueRate, GreenRate float64
	// Switched is true when blue accepted no rows at all during the window, with every vminsert pod measured, while
	// green accepted live writes: the writers point at green now. Both at 0 means no writer runs (for example while
	// VMAgent is down), not a cutover.
	Switched bool
}

// WatchWrites measures the live writes of blue and green over the same window, to tell when the writers were
// switched to green. It is the signal migrate waits for; the final sync's drain check (WaitDrained) is the strict
// check that follows.
func WatchWrites(ctx context.Context, blue, green cluster.Client, window time.Duration, sleep Sleeper) (Writes, error) {
	type measured struct {
		in  ingestion
		err error
	}
	done := make(chan measured, 1)
	go func() {
		in, err := insertRate(ctx, blue, window, sleep, nil)
		done <- measured{in, err}
	}()
	g, gerr := insertRate(ctx, green, window, sleep, liveWrites)
	b := <-done
	if err := firstErr(b.err, gerr); err != nil {
		return Writes{}, err
	}
	return Writes{
		BlueRate:  b.in.rate,
		GreenRate: g.rate,
		Switched:  b.in.rate == 0 && b.in.complete && g.rate > 0,
	}, nil
}

// liveWrites keeps the vminsert counters of live writes: it leaves out native imports, which is how vmctl copies.
func liveWrites(l map[string]string) bool { return l["type"] != "native" }
