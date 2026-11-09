package migration

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// orDiscard returns l, or a logger that drops everything if l is nil.
func orDiscard(l *slog.Logger) *slog.Logger {
	if l == nil {
		return slog.New(slog.DiscardHandler)
	}
	return l
}

// Load is one reading of blue's query load, taken from its vmselect pods.
type Load struct {
	// Utilization is the share of vmselect's concurrent query slots in use, across all pods (0..1).
	Utilization float64
	// LimitReached is the total number of queries that had to wait for a free slot (a counter).
	LimitReached float64
	// QueryP99 is the highest 99th percentile latency of instant and range queries over all pods.
	QueryP99 time.Duration
}

// Governor is a Pacer that adapts the rate limit to blue's query load: it halves the rate when blue's vmselect is
// busy and raises it by a tenth of Max per chunk when it isn't. vmctl takes its rate limit once per run, so the
// rate changes between chunks; smaller chunks react faster.
type Governor struct {
	// Read takes a load reading of blue.
	Read func(ctx context.Context) (Load, error)
	// Max is the rate the governor starts at and never exceeds; Min is the floor it never drops below.
	Max, Min int64
	// MaxUtilization is the share of vmselect's query slots in use above which blue counts as busy.
	MaxUtilization float64
	// MaxQueryP99 is the query p99 latency (instant and range) above which blue counts as busy. 0 ignores latency.
	MaxQueryP99 time.Duration
	Log         *slog.Logger
	Metrics     *Metrics

	rate        int64 // 0 until the first call
	lastReached float64
	primed      bool
}

// Next reads blue's load and returns the rate for the next chunk. When the load can't be read, it keeps the rate.
func (g *Governor) Next(ctx context.Context) int64 {
	if g.rate == 0 {
		g.rate = g.Max
	}
	l, err := g.Read(ctx)
	if err != nil {
		g.log().Warn("cannot read blue's load, keeping the rate limit", "rate", g.rate, "err", err)
		return g.rate
	}
	g.Metrics.load(l)
	// A rate of 0 would mean "no limit", so the governor never goes below 1 byte/s.
	if reason := g.busy(l); reason != "" {
		g.rate = max(g.Min, g.rate/2, 1)
		g.log().Info("blue is busy, lowering the rate limit", "reason", reason, "rate", g.rate)
	} else {
		g.rate = min(g.Max, g.rate+max(g.Max/10, 1))
	}
	return g.rate
}

// busy returns why blue counts as busy, or "" if it doesn't. LimitReached is summed over all vmselect pods, so a
// new vmselect pod with its own count can make one reading look busy; a restarted pod (count went down) can't.
func (g *Governor) busy(l Load) string {
	waited := g.primed && l.LimitReached > g.lastReached
	g.lastReached, g.primed = l.LimitReached, true
	switch {
	case waited:
		return "queries waited for a free vmselect slot"
	case l.Utilization > g.MaxUtilization:
		return fmt.Sprintf("%.0f%% of vmselect's query slots in use", l.Utilization*100)
	case g.MaxQueryP99 > 0 && l.QueryP99 > g.MaxQueryP99:
		return fmt.Sprintf("query p99 latency %s", l.QueryP99)
	}
	return ""
}

func (g *Governor) log() *slog.Logger { return orDiscard(g.Log) }
