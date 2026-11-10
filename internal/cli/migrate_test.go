package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
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
		uid:  func(_ context.Context, name string) (string, error) { return "uid-" + name, nil },
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

func TestMigrateStopsWhenPreflightLacksAPermission(t *testing.T) {
	tc := newMigrateTest(t)
	forbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "services"}, "vmselect-active", errors.New("rbac"))
	tc.m.preflight = func(context.Context) check.Report {
		tc.preflights++
		return check.Report{Results: []check.Result{
			{Name: "alias vmselect", Level: check.Fail, Message: forbidden.Error(), Err: forbidden},
		}}
	}

	err := tc.m.run(t.Context())

	if !apierrors.IsForbidden(err) || tc.preflights != 1 {
		t.Errorf("run = %v after %d preflights, want it to stop at the first", err, tc.preflights)
	}
}

func TestMigrateStopsAResumedRunThatLacksAPermission(t *testing.T) {
	forbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "", errors.New("rbac"))
	for name, deny := range map[string]func(*migrateTest){
		"listing blue's pods to measure the writes": func(tc *migrateTest) { tc.blue.podsErr = forbidden },
		"reading an alias": func(tc *migrateTest) {
			tc.blue.inserts = []string{insertMetrics(1000), insertMetrics(1000)}
			tc.e.aliases = check.Aliases{
				Names:    map[cluster.Component]string{cluster.Select: "vmselect-active"},
				Selector: func(context.Context, string) (map[string]string, error) { return nil, forbidden },
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			tc := newMigrateTest(t)
			// The backfill ran before, so the restarted pod skips preflight.
			if _, err := tc.e.store.Update(t.Context(), func(s *state.State) {
				s.BackfillStart, s.SyncedUntil = ts("2026-01-04T10:00:00Z"), ts("2026-01-05T09:50:00Z")
				s.Verifications = map[string]state.DayResult{"2026-01-04": {Pass: true, Partial: true}}
			}); err != nil {
				t.Fatal(err)
			}
			deny(tc)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			err := tc.m.run(ctx)

			if !apierrors.IsForbidden(err) {
				t.Errorf("run = %v, want it to stop on the missing permission", err)
			}
		})
	}
}

func TestMigrateCountsTheVerifyOfACatchUpTowardsReady(t *testing.T) {
	tc := newMigrateTest(t)
	tc.m.o.readyAfter = 90 * time.Second
	if _, err := tc.e.store.Update(t.Context(), func(s *state.State) {
		s.BackfillStart, s.SyncedUntil = ts("2026-01-02T10:00:00Z"), ts("2026-01-05T09:59:00Z")
		s.Verifications = map[string]state.DayResult{"2026-01-03": {Pass: true}}
	}); err != nil {
		t.Fatal(err)
	}
	// 2026-01-04 fails verify until it is copied again: the first catch-up copies for a minute, then copies that day
	// again for another minute. Blue receives writes until the second measurement.
	tc.green.samples = 50
	tc.copier.onCopy = func(w migration.Window) {
		if w.Start.Equal(ts("2026-01-04T00:00:00Z")) {
			tc.green.samples = 100
		}
	}
	tc.blue.inserts = []string{insertMetrics(1000), insertMetrics(1600), insertMetrics(1600)}

	if err := tc.m.run(t.Context()); err != nil {
		t.Fatalf("run: %v\n%s", err, tc.out)
	}

	// Ready only after the second catch-up: the first one took 3 minutes in all (copy, settle, copy again, settle),
	// longer than --ready-after.
	if st, _ := tc.e.store.Load(t.Context()); !st.ReadyAt.After(ts("2026-01-05T10:04:00Z")) {
		t.Errorf("ready at %s, want it after the second catch-up", st.ReadyAt)
	}
}

func TestMigrateStopsWhenGreenIsCreatedAgainDuringTheMigration(t *testing.T) {
	tc := newMigrateTest(t)
	tc.blue.inserts = []string{insertMetrics(1000), insertMetrics(1600), insertMetrics(1600)}
	// Green is deleted and created again once the backfill copied data into it.
	tc.m.open = func(context.Context) error {
		if len(tc.copier.windows) > 0 {
			return &state.ClusterReplacedError{Name: "green", SavedUID: "uid-green", UID: "uid-green-new"}
		}
		return nil
	}

	err := tc.m.run(t.Context())

	var replaced *state.ClusterReplacedError
	if st, _ := tc.e.store.Load(t.Context()); !errors.As(err, &replaced) || !st.CompletedAt.IsZero() {
		t.Errorf("run = %v, completed at %s; want it stopped before completing", err, st.CompletedAt)
	}
}

func TestMigrateReportsTheMigrationDoneOnlyForTheSameClusters(t *testing.T) {
	notFound := apierrors.NewNotFound(schema.GroupResource{Resource: "vmclusters"}, "blue")
	for name, tt := range map[string]struct {
		uids map[string]string // UID per existing VMCluster
		fail bool
	}{
		"both still there":                        {uids: map[string]string{"blue": "uid-blue", "green": "uid-green"}},
		"blue deleted after the migration":        {uids: map[string]string{"green": "uid-green"}},
		"green created again since, blue is gone": {uids: map[string]string{"green": "uid-green-new"}, fail: true},
	} {
		t.Run(name, func(t *testing.T) {
			tc := newMigrateTest(t)
			if _, err := tc.e.store.Update(t.Context(), func(s *state.State) {
				s.From, s.To, s.FromUID, s.ToUID = "blue", "green", "uid-blue", "uid-green"
				s.CompletedAt = ts("2026-01-05T09:00:00Z")
			}); err != nil {
				t.Fatal(err)
			}
			tc.m.open = func(context.Context) error { return errors.New("don't open both: blue may be gone") }
			tc.m.uid = func(_ context.Context, name string) (string, error) {
				if uid, ok := tt.uids[name]; ok {
					return uid, nil
				}
				return "", notFound
			}

			err := tc.m.run(t.Context())

			var replaced *state.ClusterReplacedError
			if tt.fail != errors.As(err, &replaced) || !tt.fail && err != nil {
				t.Errorf("run = %v, want a replaced VMCluster: %v", err, tt.fail)
			}
		})
	}
}

func TestMigrateForgetsANoticedCutoverOnceBlueReceivesWritesAgain(t *testing.T) {
	tc := newMigrateTest(t)
	// A cutover noticed days ago, then rolled back: blue receives writes again at the first measurement.
	old := ts("2026-01-01T00:00:00Z")
	if _, err := tc.e.store.Update(t.Context(), func(s *state.State) { s.CutoverSeenAt = old }); err != nil {
		t.Fatal(err)
	}
	tc.blue.inserts = []string{insertMetrics(1000), insertMetrics(1600), insertMetrics(1600)}

	if err := tc.m.run(t.Context()); err != nil {
		t.Fatalf("run: %v\n%s", err, tc.out)
	}

	if st, _ := tc.e.store.Load(t.Context()); !st.CutoverAt.After(old.AddDate(0, 0, 1)) {
		t.Errorf("cutover at %s, want the new one, not the one before blue received writes again", st.CutoverAt)
	}
}

func TestMigrateKeepsTheCutoverWhenThePodStopsDuringTheFinalSync(t *testing.T) {
	tc := newMigrateTest(t)
	tc.blue.inserts = []string{insertMetrics(1000), insertMetrics(1600), insertMetrics(1600)}
	newSyncer := tc.m.newSyncer
	ctx, evict := context.WithCancel(t.Context())
	tc.m.newSyncer = func() (*migration.Syncer, error) {
		s, err := newSyncer()
		if err != nil {
			return nil, err
		}
		evicted := *s
		// The pod is evicted while the final sync waits for --final-tail.
		evicted.Sleep = func(ctx context.Context, d time.Duration) error {
			if d >= 10*time.Minute {
				evict()
				return ctx.Err()
			}
			return s.Sleep(ctx, d)
		}
		return &evicted, nil
	}

	if err := tc.m.run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v, want it stopped\n%s", err, tc.out)
	}
	stoppedAt := tc.clock.Now()

	// The restarted pod comes up an hour later, catches up and notices the cutover again.
	tc.clock.Sleep(t.Context(), time.Hour)
	tc.m.newSyncer = newSyncer
	if err := tc.m.run(t.Context()); err != nil {
		t.Fatalf("second run: %v\n%s", err, tc.out)
	}

	if st, _ := tc.e.store.Load(t.Context()); !st.CutoverAt.Before(stoppedAt) {
		t.Errorf("cutover at %s, want the one noticed before the pod stopped at %s", st.CutoverAt, stoppedAt)
	}
}

func TestMigrateIsNotReadyWhileADayIsUnverified(t *testing.T) {
	tc := newMigrateTest(t)
	if _, err := tc.e.store.Update(t.Context(), func(s *state.State) {
		s.BackfillStart, s.SyncedUntil = ts("2026-01-03T00:00:00Z"), ts("2026-01-05T09:59:00Z")
	}); err != nil {
		t.Fatal(err)
	}
	// Every query to green times out, and blue keeps receiving writes. Stop after a few catch-ups.
	tc.green.selectErr = errors.New("context deadline exceeded")
	tc.blue.inserts = insertSeries(1000, 50)
	ctx, cancel := context.WithCancel(t.Context())
	var sleeps atomic.Int32 // blue and green are measured at once
	tc.m.sleep = func(ctx context.Context, d time.Duration) error {
		if sleeps.Add(1) > 10 {
			cancel()
			return ctx.Err()
		}
		return tc.clock.Sleep(ctx, d)
	}

	if err := tc.m.run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v\n%s", err, tc.out)
	}

	if st, _ := tc.e.store.Load(t.Context()); !st.ReadyAt.IsZero() {
		t.Errorf("ready at %s, want not ready while days are unverified", st.ReadyAt)
	}
}

func TestMigrateVerifiesEveryDayBeforeItCompletes(t *testing.T) {
	tc := newMigrateTest(t)
	// A resumed final sync, while 2026-01-03 and 2026-01-04 have no whole-day pass yet (e.g. their verify timed out).
	if _, err := tc.e.store.Update(t.Context(), func(s *state.State) {
		s.BackfillStart, s.SyncedUntil = ts("2026-01-02T10:00:00Z"), ts("2026-01-05T08:50:00Z")
		s.CutoverAt = ts("2026-01-05T09:00:00Z")
	}); err != nil {
		t.Fatal(err)
	}

	if err := tc.m.run(t.Context()); err != nil {
		t.Fatalf("run: %v\n%s", err, tc.out)
	}

	st, _ := tc.e.store.Load(t.Context())
	if days := unverifiedDays(st); len(days) != 0 || st.CompletedAt.IsZero() {
		t.Errorf("unverified days %v, completed at %s; want every day verified before completing", days, st.CompletedAt)
	}
}

func TestMigrateRepairsEarlierDaysBeforeTheFinalSync(t *testing.T) {
	tc := newMigrateTest(t)
	// A resumed final sync. 2026-01-03 failed verify before; its cause is fixed now, but green still misses data
	// on 2026-01-04 until that day is copied again.
	if _, err := tc.e.store.Update(t.Context(), func(s *state.State) {
		s.BackfillStart, s.SyncedUntil = ts("2026-01-02T10:00:00Z"), ts("2026-01-05T08:50:00Z")
		s.CutoverAt = ts("2026-01-05T09:00:00Z")
		s.Verifications = map[string]state.DayResult{"2026-01-03": {Pass: false, Problems: []string{"up: green 50"}}}
	}); err != nil {
		t.Fatal(err)
	}
	tc.green.samples = 50
	resetsAtRepair := -1
	tc.copier.onCopy = func(w migration.Window) {
		if w.Start.Equal(ts("2026-01-03T00:00:00Z")) || w.Start.Equal(ts("2026-01-04T00:00:00Z")) {
			tc.green.samples = 100
			resetsAtRepair = strings.Count(strings.Join(tc.green.podGets, " "), "resetRollupResultCache")
		}
	}

	if err := tc.m.run(t.Context()); err != nil {
		t.Fatalf("run: %v\n%s", err, tc.out)
	}

	// Readers use green already, so the cache reset must come after the repair, or it serves the old results.
	if resets := strings.Count(strings.Join(tc.green.podGets, " "), "resetRollupResultCache"); resetsAtRepair < 0 || resets <= resetsAtRepair {
		t.Errorf("rollup cache reset %d times, %d of them before the repair; want one after it", resets, resetsAtRepair)
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

func TestMigrateDoesNotCopyADayAgainWhenItsVerifyCouldNotQuery(t *testing.T) {
	for name, tt := range map[string]struct {
		err  error
		stop bool
	}{
		"a timeout: verify it again after the next catch-up": {err: errors.New("context deadline exceeded")},
		"refused by green's vmselect":                        {err: &cluster.HTTPError{URL: "http://vmselect-green:8481", StatusCode: 403, Status: "403 Forbidden"}, stop: true},
	} {
		t.Run(name, func(t *testing.T) {
			tc := newMigrateTest(t)
			if _, err := tc.e.store.Update(t.Context(), func(s *state.State) {
				s.BackfillStart, s.SyncedUntil = ts("2026-01-03T00:00:00Z"), ts("2026-01-05T10:00:00Z")
			}); err != nil {
				t.Fatal(err)
			}
			tc.green.selectErr = tt.err

			err := tc.m.verifyCopiedDays(t.Context(), &migration.Syncer{
				Copier: tc.copier, Store: tc.e.store, Step: time.Hour, Now: tc.clock.Now, Sleep: tc.clock.Sleep,
			})

			if len(tc.copier.windows) != 0 {
				t.Errorf("copied %v again, want nothing: the data may be fine", tc.copier.windows)
			}
			if tt.stop != (err != nil) {
				t.Errorf("err = %v, want stop = %v", err, tt.stop)
			}
			if st, _ := tc.e.store.Load(t.Context()); len(unverifiedDays(st)) != 2 {
				t.Errorf("unverified days = %v, want both days still unverified", unverifiedDays(st))
			}
		})
	}
}

func TestMigrateGivesUpOnADayWhoseVerifyFailsEveryTime(t *testing.T) {
	tc := newMigrateTest(t)
	if _, err := tc.e.store.Update(t.Context(), func(s *state.State) {
		s.BackfillStart, s.SyncedUntil = ts("2026-01-03T00:00:00Z"), ts("2026-01-05T10:00:00Z")
	}); err != nil {
		t.Fatal(err)
	}
	// Green's vmselect answers every query with an error that waiting won't fix, e.g. a limit.
	tc.green.selectErr = errors.New("422 Unprocessable Entity: the number of matching timeseries exceeds 300000")

	err := tc.m.verifyEveryDay(t.Context(), &migration.Syncer{
		Copier: tc.copier, Store: tc.e.store, Step: time.Hour, Now: tc.clock.Now, Sleep: tc.clock.Sleep,
	})

	if err == nil || !strings.Contains(err.Error(), "after 10 attempts") || len(tc.copier.windows) != 0 {
		t.Errorf("verifyEveryDay = %v, copied %d windows; want it to give up without copying", err, len(tc.copier.windows))
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
