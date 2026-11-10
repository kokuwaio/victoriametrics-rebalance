package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/check"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/cluster"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/migration"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/state"
)

// clock is a fake clock: Sleep moves it forward instead of waiting. Several goroutines may use it at once.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
	return nil
}

// timedCopier records the windows it copies, and every copy takes per on the clock.
type timedCopier struct {
	clock   *clock
	per     time.Duration
	windows []migration.Window
	onCopy  func(migration.Window)
}

func (c *timedCopier) Copy(ctx context.Context, w migration.Window, _ int64) (migration.CopyStats, error) {
	c.windows = append(c.windows, w)
	if c.onCopy != nil {
		c.onCopy(w)
	}
	return migration.CopyStats{Bytes: 1}, c.clock.Sleep(ctx, c.per)
}

// insertSeries returns n vminsert /metrics bodies whose live row counter grows by 600 with every scrape.
func insertSeries(start, n int) []string {
	var out []string
	for i := range n {
		out = append(out, insertMetrics(start+600*i))
	}
	return out
}

type migrateTest struct {
	m          *migrator
	e          *env
	out        *strings.Builder
	clock      *clock
	copier     *timedCopier
	blue       *fakeCluster
	green      *fakeCluster
	metrics    *metrics.Set
	preflights int
}

func newMigrateTest(t *testing.T) *migrateTest {
	t.Helper()
	tc := &migrateTest{clock: &clock{t: ts("2026-01-05T10:00:00Z")}, blue: newFake("blue"), green: newFake("green")}
	// Green receives live writes all the time (dual write, later all writes).
	tc.green.inserts = insertSeries(1000, 50)
	tc.e, tc.out = testEnv(tc.blue, tc.green)
	tc.copier = &timedCopier{clock: tc.clock, per: time.Minute}
	syncer := &migration.Syncer{
		Copier: tc.copier, Store: tc.e.store, Step: 24 * time.Hour,
		Now: tc.clock.Now, Sleep: tc.clock.Sleep,
	}
	tc.metrics = metrics.NewSet()
	o := &migrateOptions{
		sync: syncOptions{
			drain:  check.DrainOptions{Interval: time.Minute, Timeout: 3 * time.Minute, Sleep: tc.clock.Sleep},
			verify: verifyOpts,
		},
		poll:       time.Minute,
		readyAfter: 10 * time.Minute,
	}
	tc.m = &migrator{
		e: tc.e, o: o, phase: migration.NewPhase(tc.metrics),
		open: func(context.Context) error { return nil },
		preflight: func(context.Context) check.Report {
			tc.preflights++
			if tc.preflights == 1 {
				return check.Report{Results: []check.Result{{Name: check.DualWrite, Level: check.Fail, Message: "green receives no live writes"}}}
			}
			return check.Report{Results: []check.Result{{Name: check.DualWrite, Level: check.Pass, Message: "ok"}}, DualWriteActive: true}
		},
		newSyncer: func() (*migration.Syncer, error) { return syncer, nil },
		now:       tc.clock.Now,
		sleep:     tc.clock.Sleep,
	}
	return tc
}

func (tc *migrateTest) phase(t *testing.T) string {
	t.Helper()
	var b strings.Builder
	tc.metrics.WritePrometheus(&b)
	for line := range strings.SplitSeq(b.String(), "\n") {
		if strings.HasSuffix(line, " 1") && strings.HasPrefix(line, "vmrebalance_phase{") {
			return strings.TrimSuffix(strings.TrimPrefix(line, `vmrebalance_phase{phase="`), `"} 1`)
		}
	}
	return ""
}

func TestMigrateRunsEveryStepAndNoticesTheCutover(t *testing.T) {
	tc := newMigrateTest(t)
	// Blue receives writes during the first measurement, none during the second (the cutover), and stays drained.
	tc.blue.inserts = []string{insertMetrics(1000), insertMetrics(1600), insertMetrics(1600)}

	if err := tc.m.run(t.Context()); err != nil {
		t.Fatalf("run: %v\n%s", err, tc.out)
	}

	if tc.preflights != 2 {
		t.Errorf("preflight ran %d times, want 2 (failed once, then passed)", tc.preflights)
	}
	st, _ := tc.e.store.Load(t.Context())
	if st.DualWriteSeenAt.IsZero() || st.ReadyAt.IsZero() || st.CutoverAt.IsZero() || st.FinalSyncAt.IsZero() || st.CompletedAt.IsZero() {
		t.Errorf("state = %+v, want every step recorded", st)
	}
	// The backfill took longer than --ready-after, so the first catch-up makes green ready, right before the
	// measurement that sees the cutover.
	if !st.ReadyAt.Equal(st.CutoverAt) {
		t.Errorf("ready at %s, cutover at %s", st.ReadyAt, st.CutoverAt)
	}
	// The backfill starts one month (blue's default retention) before preflight passed, a poll interval after the
	// first try.
	if first := tc.copier.windows[0].Start; !first.Equal(ts("2026-01-05T10:01:00Z").AddDate(0, -1, 0)) {
		t.Errorf("backfill started at %s", first)
	}
	if len(st.Verifications) < 30 {
		t.Errorf("verified %d days, want every full day of the backfill", len(st.Verifications))
	}
	for _, line := range []string{"PASS  dual write", "PASS  blue drained", "PASS  rollup cache", "PASS  earlier verify", "Done."} {
		if !strings.Contains(tc.out.String(), line) {
			t.Errorf("output lacks %q:\n%s", line, tc.out)
		}
	}
	if p := tc.phase(t); p != migration.PhaseDone {
		t.Errorf("phase = %q, want done", p)
	}

	// A second run only reports that the migration is done.
	copies := len(tc.copier.windows)
	tc.out.Reset()
	if err := tc.m.run(t.Context()); err != nil || !strings.Contains(tc.out.String(), "The migration finished") || len(tc.copier.windows) != copies {
		t.Errorf("second run = %v, copied %d more windows:\n%s", err, len(tc.copier.windows)-copies, tc.out)
	}
}

func TestMigrateCatchesUpAgainWhenBlueStillReceivesWrites(t *testing.T) {
	tc := newMigrateTest(t)
	// The first measurement sees no writes on blue, but the drain check does (some writer still points at blue):
	// 3 windows of the drain check with writes, then a measurement with writes, one without, and a drained blue.
	tc.blue.inserts = append([]string{insertMetrics(1000), insertMetrics(1000)}, insertSeries(1600, 8)...)
	last := tc.blue.inserts[len(tc.blue.inserts)-1]
	tc.blue.inserts = append(tc.blue.inserts, last)

	if err := tc.m.run(t.Context()); err != nil {
		t.Fatalf("run: %v\n%s", err, tc.out)
	}

	out := tc.out.String()
	if strings.Count(out, "PASS  blue drained") != 1 || strings.Contains(out, "FAIL") {
		t.Errorf("output:\n%s", out)
	}
	st, _ := tc.e.store.Load(t.Context())
	if st.CompletedAt.IsZero() || !st.FinalStart.Before(st.CutoverAt) {
		t.Errorf("state = %+v", st)
	}
}

func TestMigrateWaitsForTheClustersOnlyWhileThatCanHelp(t *testing.T) {
	gr := schema.GroupResource{Group: "operator.victoriametrics.com", Resource: "vmclusters"}
	for name, tt := range map[string]struct {
		err  func(*migrateTest) error
		fail bool
	}{
		"green doesn't exist yet": {err: func(*migrateTest) error {
			return fmt.Errorf("get VMCluster monitoring/green: %w", apierrors.NewNotFound(gr, "green"))
		}},
		"blue was created again": {fail: true, err: func(tc *migrateTest) error {
			return state.BindClusters(t.Context(), tc.e.store, "uid-blue-new", "uid-green")
		}},
		"no permission": {fail: true, err: func(*migrateTest) error {
			return fmt.Errorf("get VMCluster monitoring/green: %w", apierrors.NewForbidden(gr, "green", errors.New("rbac")))
		}},
	} {
		t.Run(name, func(t *testing.T) {
			tc := newMigrateTest(t)
			tc.blue.inserts = []string{insertMetrics(1000), insertMetrics(1600), insertMetrics(1600)}
			if err := state.BindClusters(t.Context(), tc.e.store, "uid-blue", "uid-green"); err != nil {
				t.Fatal(err)
			}
			opens := 0
			tc.m.open = func(context.Context) error {
				if opens++; opens == 1 {
					return tt.err(tc)
				}
				return nil
			}

			err := tc.m.run(t.Context())

			if tt.fail && (err == nil || opens != 1) {
				t.Errorf("run = %v after %d tries, want it to fail at once", err, opens)
			}
			if !tt.fail && err != nil {
				t.Errorf("run = %v, want it to wait and go on", err)
			}
		})
	}
}

func TestMigrateKeepsTheFirstCutoverWhenTheDrainCheckFailsWithoutWrites(t *testing.T) {
	tc := newMigrateTest(t)
	unreachable := strings.Replace(insertMetrics(1600), "vm_rpc_vmstorage_is_reachable 1", "vm_rpc_vmstorage_is_reachable 0", 1)
	// The cutover at the second measurement, then blue can't deliver its rows during the whole drain check.
	tc.blue.inserts = []string{insertMetrics(1000), insertMetrics(1600), insertMetrics(1600), unreachable}

	if err := tc.m.run(t.Context()); !errors.Is(err, errChecksFailed) {
		t.Fatalf("run = %v, want failed checks\n%s", err, tc.out)
	}
	failedAt := tc.clock.Now()

	// The restarted pod, once blue's vmstorage is back: it catches up and notices the cutover again.
	tc.blue.inserts = []string{insertMetrics(1600)}
	if err := tc.m.run(t.Context()); err != nil {
		t.Fatalf("second run: %v\n%s", err, tc.out)
	}

	// Rows blue delivered late carry timestamps from around the first cutover, so the final sync must start there.
	st, _ := tc.e.store.Load(t.Context())
	if !st.CutoverAt.Before(failedAt) || !st.FinalStart.Before(st.CutoverAt) || st.CompletedAt.IsZero() {
		t.Errorf("cutover at %s, final window from %s; want the first cutover, before the failure at %s",
			st.CutoverAt, st.FinalStart, failedAt)
	}
}

func TestMigrateResumesTheFinalSyncOfARecordedCutover(t *testing.T) {
	tc := newMigrateTest(t)
	cutover := ts("2026-01-05T09:00:00Z")
	if _, err := tc.e.store.Update(t.Context(), func(s *state.State) {
		s.BackfillStart, s.SyncedUntil, s.CutoverAt = ts("2025-12-05T10:00:00Z"), ts("2026-01-05T08:50:00Z"), cutover
	}); err != nil {
		t.Fatal(err)
	}

	if err := tc.m.run(t.Context()); err != nil {
		t.Fatalf("run: %v\n%s", err, tc.out)
	}

	if tc.preflights != 0 {
		t.Errorf("preflight ran %d times after the backfill had started", tc.preflights)
	}
	if w := tc.copier.windows[0]; !w.Start.Equal(ts("2026-01-05T07:50:00Z")) {
		t.Errorf("first copy = %s, want the final window, from one hour before the last synced point", w)
	}
	if st, _ := tc.e.store.Load(t.Context()); !st.CutoverAt.Equal(cutover) || st.CompletedAt.IsZero() {
		t.Errorf("state = %+v", st)
	}
}

func TestMigrateCopiesAFailedDayAgain(t *testing.T) {
	for name, heals := range map[string]bool{"and goes on when it passes then": true, "and stops when it still fails": false} {
		t.Run(name, func(t *testing.T) {
			tc := newMigrateTest(t)
			if _, err := tc.e.store.Update(t.Context(), func(s *state.State) {
				s.BackfillStart, s.SyncedUntil = ts("2026-01-02T10:00:00Z"), ts("2026-01-05T10:00:00Z")
				s.Verifications = map[string]state.DayResult{"2026-01-03": {Pass: true}}
			}); err != nil {
				t.Fatal(err)
			}
			tc.green.samples = 50
			tc.copier.onCopy = func(migration.Window) {
				if heals {
					tc.green.samples = 100
				}
			}

			err := tc.m.verifyCopiedDays(t.Context(), &migration.Syncer{
				Copier: tc.copier, Store: tc.e.store, Step: time.Hour, Now: tc.clock.Now, Sleep: tc.clock.Sleep,
			})

			// 2026-01-03 passed before; only 2026-01-04 is a full day without a pass.
			if len(tc.copier.windows) != 24 || !tc.copier.windows[0].Start.Equal(ts("2026-01-04T00:00:00Z")) {
				t.Errorf("copied %v, want 2026-01-04 again in 1h chunks", tc.copier.windows)
			}
			if heals && err != nil {
				t.Errorf("err = %v", err)
			}
			if !heals && (!errors.Is(err, errChecksFailed) || !strings.Contains(tc.out.String(), "FAIL  verify 2026-01-04")) {
				t.Errorf("err = %v, output:\n%s", err, tc.out)
			}
		})
	}
}

func TestUnverifiedDays(t *testing.T) {
	st := &state.State{
		BackfillStart: ts("2026-01-01T10:00:00Z"),
		SyncedUntil:   ts("2026-01-05T06:00:00Z"),
		Verifications: map[string]state.DayResult{
			"2026-01-02": {Pass: true},
			"2026-01-03": {Pass: true, Partial: true},
			"2026-01-04": {Pass: false},
		},
	}

	var got []string
	for _, d := range unverifiedDays(st) {
		got = append(got, state.DayKey(d))
	}

	// 2026-01-01 was only partly copied, 2026-01-05 isn't over yet.
	if strings.Join(got, ",") != "2026-01-03,2026-01-04" {
		t.Errorf("unverified days = %v", got)
	}
}

// aliasesOn makes both aliases select the pods of the cluster active returns, asking it on every lookup.
func aliasesOn(active func() string) check.Aliases {
	return check.Aliases{
		Names: map[cluster.Component]string{cluster.Select: "vmselect-active", cluster.Insert: "vminsert-active"},
		Selector: func(_ context.Context, name string) (map[string]string, error) {
			return map[string]string{
				"app.kubernetes.io/name":     strings.TrimSuffix(name, "-active"),
				"app.kubernetes.io/instance": active(),
			}, nil
		},
	}
}

func TestMigrateWaitsUntilTheAliasesSelectGreen(t *testing.T) {
	tc := newMigrateTest(t)
	tc.blue.inserts = []string{insertMetrics(1000), insertMetrics(1600), insertMetrics(1600)}
	// The writers move at the second measurement, the aliases only after it (4 lookups: 2 per measurement).
	lookups := 0
	tc.e.aliases = aliasesOn(func() string {
		lookups++
		if lookups <= 4 {
			return "blue"
		}
		return "green"
	})

	if err := tc.m.run(t.Context()); err != nil {
		t.Fatalf("run: %v\n%s", err, tc.out)
	}

	st, _ := tc.e.store.Load(t.Context())
	if !st.CutoverAt.After(st.ReadyAt) {
		t.Errorf("cutover at %s, ready at %s: want the cutover one measurement later, once the aliases moved", st.CutoverAt, st.ReadyAt)
	}
	for _, line := range []string{"PASS  alias vmselect", "PASS  alias vminsert", "Done."} {
		if !strings.Contains(tc.out.String(), line) {
			t.Errorf("output lacks %q:\n%s", line, tc.out)
		}
	}
}

func TestFinalSyncFailsWhileAnAliasStillSelectsBlue(t *testing.T) {
	e, _, o, syncer, _, _ := finalSetup(t, newFake("blue"))
	e.aliases = aliasesOn(func() string { return "blue" })

	results, err := finalSync(t.Context(), e, o, syncer, cutover)

	if err != nil || !check.AnyFailed(results) {
		t.Fatalf("finalSync = %v, %+v", err, results)
	}
	for _, r := range results {
		if strings.HasPrefix(r.Name, "alias ") && r.Level != check.Fail {
			t.Errorf("%s = %s %q, want a failure", r.Name, r.Level, r.Message)
		}
	}
}
