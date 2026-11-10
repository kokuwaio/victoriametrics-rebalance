package check

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/cluster"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/state"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/vm"
)

// VerifyOptions tune how a day is compared.
type VerifyOptions struct {
	// TopN is the number of metric names with the most series whose samples are counted.
	TopN int
	// RandomN is the number of further metric names, picked at random, whose samples are counted.
	RandomN int
	// Pool is how many names below the top N the random names are picked from.
	Pool int
	// Tolerance is the share by which green may fall short of blue (0.01 = 1%). Blue can count a series twice
	// when vminsert rerouted it while a vmstorage node was down, so an exact match is too strict.
	Tolerance float64
	// Rand picks the random names. Default: math/rand/v2's global source.
	Rand *rand.Rand
}

// VerifyDay compares [start, end) between blue and green; both must lie within one UTC day (end at most the next
// midnight). Green passes when it has as many samples for every sampled metric name in that range and, when the
// range is the full day, at least as many series as blue, minus the tolerance. Green may have more: it keeps
// receiving writes after cutover.
func VerifyDay(ctx context.Context, blue, green cluster.Client, start, end time.Time, o VerifyOptions) state.DayResult {
	day := dayOf(start)
	r := state.DayResult{Pass: true, CheckedAt: time.Now(), Partial: !IsFullDay(start, end), From: start, Until: end}
	problem := func(format string, args ...any) {
		r.Pass = false
		r.Problems = append(r.Problems, fmt.Sprintf(format, args...))
	}
	// queryFailed records a request that failed: the day is unverified, not known to miss data.
	queryFailed := func(side string, err error) {
		if r.Err == nil {
			r.Err = err
		}
		problem("%s: %v", side, err)
	}

	bs, err := vm.TSDB(ctx, blue.SelectGet, day, o.TopN+o.Pool)
	if err != nil {
		queryFailed("blue", err)
		return r
	}
	gs, err := vm.TSDB(ctx, green.SelectGet, day, 1)
	if err != nil {
		queryFailed("green", err)
		return r
	}
	r.BlueSeries, r.GreenSeries = bs.TotalSeries, gs.TotalSeries
	// The series count covers the whole day. On a partial day, blue can have series outside [start, end) that were
	// never copied (before start) or not yet (after end), so only the sample counts in the range are compared.
	if !r.Partial && fallsShort(float64(bs.TotalSeries), float64(gs.TotalSeries), o.Tolerance) {
		problem("series: blue %d, green %d", bs.TotalSeries, gs.TotalSeries)
	}

	// A range selector at time t covers (t - window, t]. VictoriaMetrics stores timestamps in milliseconds, so a window
	// of end - start in milliseconds, evaluated 1ms before end, covers exactly [start, end): a sample at start counts,
	// one at end doesn't.
	window := end.Sub(start).Milliseconds()
	at := end.Add(-time.Millisecond)
	for _, name := range sampleNames(bs.SeriesCountByMetricName, o) {
		q := fmt.Sprintf("sum(count_over_time(%s[%dms]))", vm.MetricSelector(name), window)
		bc, err := vm.QuerySum(ctx, blue.SelectGet, q, at)
		if err != nil {
			queryFailed("blue", err)
			continue
		}
		gc, err := vm.QuerySum(ctx, green.SelectGet, q, at)
		if err != nil {
			queryFailed("green", err)
			continue
		}
		if fallsShort(bc, gc, o.Tolerance) {
			problem("samples of %s: blue %.0f, green %.0f", name, bc, gc)
		}
	}
	return r
}

// IsFullDay reports whether [start, end) covers a whole UTC day.
func IsFullDay(start, end time.Time) bool {
	day := dayOf(start)
	return start.Equal(day) && !end.Before(day.Add(24*time.Hour))
}

// dayOf returns the UTC midnight that starts the day of t.
func dayOf(t time.Time) time.Time { return t.UTC().Truncate(24 * time.Hour) }

// fallsShort reports whether green has less than blue, minus the tolerance.
func fallsShort(blue, green, tolerance float64) bool {
	return green < blue*(1-tolerance)
}

// sampleNames returns the top N names plus up to RandomN names picked at random from the rest.
func sampleNames(names []vm.NameCount, o VerifyOptions) []string {
	var out []string
	for i := 0; i < len(names) && i < o.TopN; i++ {
		out = append(out, names[i].Name)
	}
	if len(names) <= o.TopN {
		return out
	}
	rest := names[o.TopN:]
	perm := rand.Perm
	if o.Rand != nil {
		perm = o.Rand.Perm
	}
	for i, idx := range perm(len(rest)) {
		if i >= o.RandomN {
			break
		}
		out = append(out, rest[idx].Name)
	}
	return out
}
