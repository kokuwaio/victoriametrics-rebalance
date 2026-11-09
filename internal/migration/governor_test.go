package migration_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/migration"
)

type reading struct {
	load migration.Load
	err  error
}

func governor(readings []reading) *migration.Governor {
	return &migration.Governor{
		Read: func(context.Context) (migration.Load, error) {
			r := readings[0]
			readings = readings[1:]
			return r.load, r.err
		},
		Max:            1000,
		Min:            100,
		MaxUtilization: 0.8,
		MaxQueryP99:    2 * time.Second,
	}
}

func healthy(reached float64) reading {
	return reading{load: migration.Load{Utilization: 0.1, LimitReached: reached, QueryP99: 500 * time.Millisecond}}
}

func assertRates(t *testing.T, g *migration.Governor, want []int64) {
	t.Helper()
	for i, w := range want {
		if got := g.Next(t.Context()); got != w {
			t.Fatalf("chunk %d: rate %d, want %d", i+1, got, w)
		}
	}
}

func TestGovernorHalvesTheRateWhenBlueIsBusyAndRecoversSlowly(t *testing.T) {
	g := governor([]reading{
		healthy(5), // starts at the maximum
		{load: migration.Load{Utilization: 0.1, LimitReached: 8}},                            // queries waited for a slot
		{load: migration.Load{Utilization: 0.9, LimitReached: 8}},                            // slots almost all in use
		{load: migration.Load{Utilization: 0.1, LimitReached: 8, QueryP99: 3 * time.Second}}, // queries slow
		{load: migration.Load{Utilization: 0.95, LimitReached: 9}},                           // still busy: stays at the floor
		healthy(9),
		healthy(9),
	})

	assertRates(t, g, []int64{1000, 500, 250, 125, 100, 200, 300})
}

func TestGovernorRecoversUpToTheMaximum(t *testing.T) {
	g := governor([]reading{
		{load: migration.Load{Utilization: 0.9}},
		healthy(0), healthy(0), healthy(0), healthy(0), healthy(0), healthy(0),
	})

	assertRates(t, g, []int64{500, 600, 700, 800, 900, 1000, 1000})
}

func TestGovernorKeepsTheRateWhenBlueCantBeRead(t *testing.T) {
	g := governor([]reading{
		{load: migration.Load{Utilization: 0.9}},
		{err: errors.New("vmselect unreachable")},
	})

	assertRates(t, g, []int64{500, 500})
}

func TestGovernorIgnoresLatencyWithoutALimitAndCounterResets(t *testing.T) {
	g := governor([]reading{
		healthy(50),
		healthy(3), // a vmselect pod restarted: the counter went down, nothing waited
		{load: migration.Load{Utilization: 0.1, LimitReached: 3, QueryP99: time.Minute}},
	})
	g.MaxQueryP99 = 0

	assertRates(t, g, []int64{1000, 1000, 1000})
}

func TestGovernorNeverDropsToUnlimitedAndRecoversFromATinyRate(t *testing.T) {
	busy := reading{load: migration.Load{Utilization: 0.9}}
	g := governor([]reading{busy, busy, busy, healthy(0), healthy(0)})
	g.Max, g.Min = 4, 0

	// 0 would mean "no limit": halving must stop at 1 byte/s, and recovery must still move.
	assertRates(t, g, []int64{2, 1, 1, 2, 3})
}

func TestGovernorMetricsShowBluesLoad(t *testing.T) {
	g := governor([]reading{{load: migration.Load{Utilization: 0.25, QueryP99: 1500 * time.Millisecond}}})
	set := metrics.NewSet()
	g.Metrics = migration.NewMetrics(set)

	g.Next(t.Context())

	var out bytes.Buffer
	set.WritePrometheus(&out)
	for _, line := range []string{
		"vmrebalance_blue_select_concurrency_ratio 0.25",
		`vmrebalance_blue_query_duration_seconds{quantile="0.99"} 1.5`,
	} {
		if !strings.Contains(out.String(), line+"\n") {
			t.Errorf("metrics lack %q:\n%s", line, out.String())
		}
	}
}
