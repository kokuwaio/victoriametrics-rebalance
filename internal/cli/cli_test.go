package cli

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/check"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/migration"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/state"
)

func ts(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestCatchUpETAAccountsForNowMovingOn(t *testing.T) {
	// Copying 2h of data per hour while 1h behind: after 1h the copy has caught up with the hour that passed too.
	eta, ok := catchUpETA(3600, 1800, 3600)
	if !ok || eta != time.Hour {
		t.Errorf("eta = %s, %v; want 1h", eta, ok)
	}
	for _, pace := range []float64{1, 0.5} {
		if _, ok := catchUpETA(3600, 3600, 3600*pace); ok {
			t.Errorf("pace %g: want no ETA, the copy never catches up", pace)
		}
	}
	// Barely faster than real time: the ETA would overflow time.Duration.
	if eta, ok := catchUpETA(86400, 3600, 3600.0036); ok {
		t.Errorf("pace 1.000001: eta = %s, want no ETA", eta)
	}
}

func TestVerifyRangeDefaultsToTheFullDaysAfterTheBackfillStartUpToTheLastSync(t *testing.T) {
	st := &state.State{BackfillStart: ts("2026-01-01T10:00:00Z"), SyncedUntil: ts("2026-01-05T06:00:00Z")}

	from, to, err := verifyRange("", "", st)

	if err != nil || !from.Equal(ts("2026-01-02T00:00:00Z")) || !to.Equal(ts("2026-01-05T06:00:00Z")) {
		t.Errorf("range = %s – %s, %v", from, to, err)
	}

	st.BackfillStart = ts("2026-01-01T00:00:00Z")
	if from, _, _ := verifyRange("", "", st); !from.Equal(ts("2026-01-01T00:00:00Z")) {
		t.Errorf("a backfill that started at midnight covers that day fully; from = %s", from)
	}
}

func TestVerifyRangeErrors(t *testing.T) {
	if _, _, err := verifyRange("", "", &state.State{}); err == nil {
		t.Error("want an error when nothing was synced and no range is given")
	}
	if _, _, err := verifyRange("2026-01-05T00:00:00Z", "2026-01-04T00:00:00Z", &state.State{}); err == nil {
		t.Error("want an error for an empty range")
	}
	if _, _, err := verifyRange("yesterday", "", &state.State{}); err == nil {
		t.Error("want an error for an unparsable --since")
	}
}

func TestNewPacer(t *testing.T) {
	e := &env{}
	base := syncOptions{adaptive: true, maxUtilization: 0.8}

	if p, err := newPacer(e, &syncOptions{}, 500, nil); err != nil || p != migration.FixedRate(500) {
		t.Errorf("without --adaptive: %v, %v; want a fixed rate of 500", p, err)
	}
	p, err := newPacer(e, &base, 1000, nil)
	gov, ok := p.(*migration.Governor)
	if err != nil || !ok || gov.Min != 100 || gov.Max != 1000 {
		t.Errorf("--adaptive: %+v, %v; want a governor from 100 to 1000", p, err)
	}

	for name, tc := range map[string]struct {
		o    syncOptions
		rate int64
	}{
		"no --rate-limit":             {base, 0},
		"utilization as a percentage": {syncOptions{adaptive: true, maxUtilization: 80}, 1000},
		"minimum above maximum":       {syncOptions{adaptive: true, maxUtilization: 0.8, minRateLimit: "2KB"}, 1000},
	} {
		if _, err := newPacer(e, &tc.o, tc.rate, nil); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestVerifyFlagsRejectNegativeCountsAndBadTolerance(t *testing.T) {
	ok := verifyFlags{check.VerifyOptions{TopN: 10, RandomN: 10, Pool: 1000, Tolerance: 0.01}}
	if err := ok.validate(); err != nil {
		t.Errorf("defaults: %v", err)
	}
	for name, change := range map[string]func(*verifyFlags){
		"negative --top-n":    func(v *verifyFlags) { v.TopN = -1 },
		"negative --random-n": func(v *verifyFlags) { v.RandomN = -1 },
		"negative --pool":     func(v *verifyFlags) { v.Pool = -1 },
		"tolerance of 100%":   func(v *verifyFlags) { v.Tolerance = 1 },
		"negative tolerance":  func(v *verifyFlags) { v.Tolerance = -0.1 },
		"NaN tolerance":       func(v *verifyFlags) { v.Tolerance = math.NaN() },
	} {
		v := ok
		change(&v)
		if err := v.validate(); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestEarlierFailuresBlockTheFinalSync(t *testing.T) {
	st := &state.State{BackfillStart: ts("2026-01-02T10:00:00Z"), SyncedUntil: ts("2026-01-06T12:00:00Z"), Verifications: map[string]state.DayResult{
		"2026-01-02": {Pass: false}, // the partly copied backfill-start day
		"2026-01-03": {Pass: false},
		"2026-01-04": {Pass: true},
		"2026-01-05": {Pass: false},
	}}
	thisRun := []check.Result{{Name: "verify 2026-01-05", Level: check.Fail}}

	r := earlierFailures(st, thisRun)

	if r.Level != check.Fail || !strings.Contains(r.Message, "2026-01-03") || strings.Contains(r.Message, "2026-01-05") ||
		strings.Contains(r.Message, "2026-01-02") {
		t.Errorf("earlier verify = %s %q, want only the failed day in the migrated range outside this run", r.Level, r.Message)
	}
	if r := earlierFailures(&state.State{}, nil); r.Level != check.Pass {
		t.Errorf("without stored failures: %s %q", r.Level, r.Message)
	}
}

func TestVersionFlagPrintsTheVersionWithoutOtherFlags(t *testing.T) {
	root := NewRoot("1.2.3")
	var out strings.Builder
	root.SetOut(&out)
	root.SetArgs([]string{"--version"})

	if err := root.Execute(); err != nil {
		t.Fatalf("--version: %v", err)
	}
	if out.String() != "vmrebalance 1.2.3\n" {
		t.Errorf("output = %q", out.String())
	}
}
