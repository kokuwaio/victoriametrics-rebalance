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

// VerifyDay compares the day that starts at day (UTC midnight) between blue and green, up to end (at most
// day + 24h). Green passes when it has at least as many series, and as many samples for every sampled metric
// name, as blue, minus the tolerance. Green may have more: it keeps receiving writes after cutover.
func VerifyDay(ctx context.Context, blue, green cluster.Client, day, end time.Time, o VerifyOptions) state.DayResult {
	r := state.DayResult{Pass: true, CheckedAt: time.Now()}
	problem := func(format string, args ...any) {
		r.Pass = false
		r.Problems = append(r.Problems, fmt.Sprintf(format, args...))
	}

	bs, err := vm.TSDB(ctx, blue.SelectGet, day, o.TopN+o.Pool)
	if err != nil {
		problem("blue: %v", err)
		return r
	}
	gs, err := vm.TSDB(ctx, green.SelectGet, day, 1)
	if err != nil {
		problem("green: %v", err)
		return r
	}
	r.BlueSeries, r.GreenSeries = bs.TotalSeries, gs.TotalSeries
	if fallsShort(float64(bs.TotalSeries), float64(gs.TotalSeries), o.Tolerance) {
		problem("series: blue %d, green %d", bs.TotalSeries, gs.TotalSeries)
	}

	window := end.Sub(day).Truncate(time.Second)
	for _, name := range sampleNames(bs.SeriesCountByMetricName, o) {
		q := fmt.Sprintf("sum(count_over_time(%s[%ds]))", vm.MetricSelector(name), int64(window.Seconds()))
		bc, err := vm.QuerySum(ctx, blue.SelectGet, q, end)
		if err != nil {
			problem("blue: %v", err)
			continue
		}
		gc, err := vm.QuerySum(ctx, green.SelectGet, q, end)
		if err != nil {
			problem("green: %v", err)
			continue
		}
		if fallsShort(bc, gc, o.Tolerance) {
			problem("samples of %s: blue %.0f, green %.0f", name, bc, gc)
		}
	}
	return r
}

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
