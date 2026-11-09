package cli

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/check"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/migration"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/state"
)

func TestPrintStatus(t *testing.T) {
	st := &state.State{
		From: "blue", To: "green",
		DualWriteSeenAt: ts("2026-01-01T08:00:00Z"),
		BackfillStart:   ts("2025-12-01T08:00:00Z"),
		SyncedUntil:     ts("2026-01-01T00:00:00Z"),
		Chunks: []state.Chunk{
			{Start: ts("2025-12-01T08:00:00Z"), End: ts("2025-12-02T00:00:00Z"), Bytes: 1 << 30, Seconds: 1800},
			{Start: ts("2025-12-02T00:00:00Z"), End: ts("2026-01-01T00:00:00Z"), Bytes: 3 << 30, Seconds: 1800},
		},
		Verifications: map[string]state.DayResult{
			"2025-12-02": {Pass: true},
			"2025-12-03": {Pass: false, Problems: []string{"series: blue 10, green 9"}},
		},
	}
	var out strings.Builder

	printStatus(&out, st, ts("2026-01-01T06:00:00Z"))

	want := `Migration:        blue → green
Dual write seen:  2026-01-01T08:00:00Z
Backfill start:   2025-12-01T08:00:00Z
Synced until:     2026-01-01T00:00:00Z (6h0m0s behind now)
Chunks:           2 recorded, 4.0 GiB copied in 1h0m0s
Throughput:       1.1 MiB/s
Catch-up ETA:     ~29.4s at the recorded pace
Verified days:    1 pass, 1 fail
  2025-12-03: [series: blue 10, green 9]
Cutover:          -
Final sync:       -
`
	if out.String() != want {
		t.Errorf("status:\n%s\nwant:\n%s", out.String(), want)
	}
}

func TestPrintStatusBeforeTheFirstSync(t *testing.T) {
	var out strings.Builder

	printStatus(&out, &state.State{From: "blue", To: "green"}, ts("2026-01-01T06:00:00Z"))

	if !strings.Contains(out.String(), "Synced until:     - (run sync to start the backfill)") {
		t.Errorf("status:\n%s", out.String())
	}
}

func TestParseRetention(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"30d": 30 * 24 * time.Hour,
		"2w":  14 * 24 * time.Hour,
		"1y":  365 * 24 * time.Hour,
		"12h": 12 * time.Hour,
	} {
		if got, err := parseRetention(in); err != nil || got != want {
			t.Errorf("%q = %s, %v; want %s", in, got, err, want)
		}
	}
	empty, err := parseRetention("")
	if month, _ := parseRetention("1"); err != nil || empty != month {
		t.Errorf("empty retention = %s, %v; want the VictoriaMetrics default of one month (%s)", empty, err, month)
	}
	if _, err := parseRetention("forever"); err == nil {
		t.Error("want an error for an invalid retention")
	}
}

// kubeconfig writes a kubeconfig for an unreachable API server. Commands that don't open the clusters never connect.
func kubeconfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kubeconfig")
	cfg := `apiVersion: v1
kind: Config
clusters:
  - name: test
    cluster: {server: "https://127.0.0.1:1"}
users:
  - name: test
    user: {}
contexts:
  - name: test
    context: {cluster: test, user: test, namespace: monitoring}
current-context: test
`
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func run(t *testing.T, args ...string) (string, error) {
	t.Helper()
	root := NewRoot("test")
	var out strings.Builder
	root.SetOut(&out)
	root.SetErr(io.Discard)
	root.SetArgs(args)
	err := root.ExecuteContext(t.Context())
	return out.String(), err
}

func TestStatusCommandReadsTheStateFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	m := state.Migration{From: "blue", To: "green"}
	if _, err := state.NewFile(path, m).Update(t.Context(), func(s *state.State) { s.SyncedUntil = ts("2026-01-01T00:00:00Z") }); err != nil {
		t.Fatal(err)
	}

	out, err := run(t, "status", "--kubeconfig", kubeconfig(t), "--from", "blue", "--to", "green", "--state-file", path)

	if err != nil || !strings.Contains(out, "Migration:        blue → green") || !strings.Contains(out, "2026-01-01T00:00:00Z") {
		t.Errorf("status = %q, %v", out, err)
	}
}

func TestCommandsRejectBadFlagsBeforeConnecting(t *testing.T) {
	kc := kubeconfig(t)
	common := []string{"--kubeconfig", kc, "--from", "blue", "--to", "green"}
	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"same cluster twice":       {[]string{"status", "--kubeconfig", kc, "--from", "blue", "--to", "blue"}, "different VMClusters"},
		"dotted cluster name":      {[]string{"status", "--kubeconfig", kc, "--from", "blue.x", "--to", "green"}, "dots"},
		"unknown access mode":      {append([]string{"status", "--access", "tunnel"}, common...), "--access must be"},
		"zero step":                {append([]string{"sync", "--step", "0"}, common...), "--step must be positive"},
		"final without cutover":    {append([]string{"sync", "--final"}, common...), "--final needs --cutover-at"},
		"short drain interval":     {append([]string{"sync", "--final", "--cutover-at", "2026-01-01T00:00:00Z", "--drain-interval", "30s"}, common...), "at least 1m"},
		"zero final overlap":       {append([]string{"sync", "--final", "--cutover-at", "2026-01-01T00:00:00Z", "--final-overlap", "0"}, common...), "--final-overlap"},
		"rate limit through vmctl": {append([]string{"sync", "--vmctl-arg=--vm-rate-limit=100"}, common...), "--rate-limit"},
		"negative top-n":           {append([]string{"verify", "--top-n", "-1"}, common...), "must not be negative"},
		"missing --to":             {[]string{"status", "--kubeconfig", kc, "--from", "blue"}, "to"},
	} {
		if _, err := run(t, tc.args...); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", name, err, tc.want)
		}
	}
}

func testEnv(blue, green *fakeCluster) (*env, *strings.Builder) {
	var out strings.Builder
	return &env{
		namespace: "monitoring",
		store:     &state.Memory{},
		log:       slog.New(slog.DiscardHandler),
		out:       &out,
		tenant:    "0",
		blue:      blue,
		green:     green,
	}, &out
}

var verifyOpts = verifyFlags{check.VerifyOptions{TopN: 1, Pool: 1, Tolerance: 0.01}}

func TestVerifyDaysStoresOneResultPerDayAndMarksPartialDays(t *testing.T) {
	e, _ := testEnv(newFake("blue"), newFake("green"))

	results, err := verifyDays(t.Context(), e, verifyOpts, ts("2026-01-01T12:00:00Z"), ts("2026-01-03T06:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}

	msgs := []string{
		"samples 12:00:00–00:00:00 match (partial day, series not compared)",
		"series: blue 10, green 10",
		"samples 00:00:00–06:00:00 match (partial day, series not compared)",
	}
	if len(results) != len(msgs) {
		t.Fatalf("results = %+v", results)
	}
	for i, r := range results {
		if r.Level != check.Pass || r.Message != msgs[i] {
			t.Errorf("day %d = %s %q, want PASS %q", i, r.Level, r.Message, msgs[i])
		}
	}
	st, _ := e.store.Load(t.Context())
	for day, partial := range map[string]bool{"2026-01-01": true, "2026-01-02": false, "2026-01-03": true} {
		if r, ok := st.Verifications[day]; !ok || !r.Pass || r.Partial != partial {
			t.Errorf("stored %s = %+v, want a pass with partial=%v", day, r, partial)
		}
	}
}

func TestVerifyDaysReportsAShortfall(t *testing.T) {
	green := newFake("green")
	green.samples = 50
	e, _ := testEnv(newFake("blue"), green)

	results, err := verifyDays(t.Context(), e, verifyOpts, ts("2026-01-02T00:00:00Z"), ts("2026-01-03T00:00:00Z"))

	if err != nil || len(results) != 1 || results[0].Level != check.Fail || !strings.Contains(results[0].Message, "samples of up") {
		t.Errorf("results = %+v, %v", results, err)
	}
}

// recordingCopier records the windows it copies.
type recordingCopier struct{ windows []migration.Window }

func (c *recordingCopier) Copy(_ context.Context, w migration.Window, _ int64) (migration.CopyStats, error) {
	c.windows = append(c.windows, w)
	return migration.CopyStats{Bytes: 1}, nil
}

func finalSetup(t *testing.T, blue *fakeCluster) (*env, *strings.Builder, *syncOptions, *migration.Syncer, *recordingCopier, *fakeCluster) {
	t.Helper()
	green := newFake("green")
	e, out := testEnv(blue, green)
	if _, err := e.store.Update(t.Context(), func(s *state.State) { s.SyncedUntil = ts("2026-01-05T10:00:00Z") }); err != nil {
		t.Fatal(err)
	}
	noSleep := func(context.Context, time.Duration) error { return nil }
	o := &syncOptions{
		cutoverAt: "2026-01-05T10:30:00Z",
		drain:     check.DrainOptions{Interval: time.Minute, Timeout: 3 * time.Minute, Sleep: noSleep},
		verify:    verifyOpts,
	}
	c := &recordingCopier{}
	syncer := &migration.Syncer{
		Copier: c, Store: e.store, Step: time.Hour,
		Now:   func() time.Time { return ts("2026-01-05T11:00:00Z") },
		Sleep: noSleep,
	}
	return e, out, o, syncer, c, green
}

func TestRunFinalCopiesResetsTheCacheAndVerifies(t *testing.T) {
	e, out, o, syncer, c, green := finalSetup(t, newFake("blue"))

	if err := runFinal(t.Context(), e, o, syncer); err != nil {
		t.Fatalf("runFinal: %v\n%s", err, out)
	}

	if len(c.windows) == 0 || !c.windows[0].Start.Equal(ts("2026-01-05T09:00:00Z")) {
		t.Errorf("copied %v, want a window from one hour before the last synced point", c.windows)
	}
	if !strings.Contains(strings.Join(green.podGets, " "), "vmselect-green-0/internal/resetRollupResultCache") {
		t.Errorf("green calls = %v, want the rollup cache reset", green.podGets)
	}
	for _, line := range []string{"PASS  blue drained", "PASS  rollup cache", "PASS  verify 2026-01-05", "PASS  earlier verify", "Done."} {
		if !strings.Contains(out.String(), line) {
			t.Errorf("output lacks %q:\n%s", line, out)
		}
	}
}

func TestRunFinalCopiesNothingWhileBlueReceivesWrites(t *testing.T) {
	blue := newFake("blue")
	blue.inserts = []string{insertMetrics(1000), insertMetrics(1600), insertMetrics(2200), insertMetrics(2800),
		insertMetrics(3400), insertMetrics(4000)}
	e, out, o, syncer, c, _ := finalSetup(t, blue)

	err := runFinal(t.Context(), e, o, syncer)

	if !errors.Is(err, errChecksFailed) || len(c.windows) != 0 {
		t.Errorf("err = %v, copied %v; want a failed check and no copy", err, c.windows)
	}
	if !strings.Contains(out.String(), "FAIL  blue drained") || !strings.Contains(out.String(), "Nothing copied") {
		t.Errorf("output:\n%s", out)
	}
}

func TestRunFinalRejectsNowAsTheCutover(t *testing.T) {
	e, _, o, syncer, _, _ := finalSetup(t, newFake("blue"))
	o.cutoverAt = "now"

	if err := runFinal(t.Context(), e, o, syncer); err == nil || !strings.Contains(err.Error(), "not \"now\"") {
		t.Errorf("err = %v", err)
	}
}
