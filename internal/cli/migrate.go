package cli

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/check"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/migration"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/state"
)

type migrateOptions struct {
	sync       syncOptions
	since      string
	preflight  check.PreflightOptions
	poll       time.Duration
	readyAfter time.Duration
}

func newMigrate(g *globalOptions) *cobra.Command {
	o := &migrateOptions{
		sync:      syncOptions{drain: check.DrainOptions{Sleep: sleep}},
		preflight: check.PreflightOptions{Sleep: sleep},
	}
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Run the whole migration: preflight, backfill, verify, catch-up, and the final sync after the cutover",
		Long: `migrate runs every step of the tool in one process, from the first preflight to the final sync. You still
create green, enable dual write, switch the aliases and delete blue yourself (usually in Git); migrate waits for
each of these changes and notices them by itself.

1. Preflight: waits until both VMClusters exist and every preflight check passes (green receives dual writes, has
   enough disk, ...), retrying every --poll-interval and logging the failed checks. With --select-alias and
   --insert-alias, it also waits until those Services select only blue's pods.
2. Backfill: copies [now - retention, now] (or from --since), then verifies every full day it copied. A day that
   fails is copied again once; if it still fails, migrate stops.
3. Catch-up: copies from the last synced point to now, again and again. After each run it measures blue's and
   green's writes for --drain-interval. Once a run takes less than --ready-after, it logs that green is ready for
   the cutover (and records it in the state and the vmrebalance_phase metric).
4. Cutover: you switch the read and write aliases to green. migrate sees blue receive no more rows while green
   still does, and takes that moment as the cutover time. With --select-alias and --insert-alias, it also waits
   until both Services select green's pods.
5. Final sync: waits until cutover + --final-tail and checks that blue is drained; if blue still receives writes,
   it copies nothing and goes back to catching up. Then it copies [last synced point - --final-overlap, the moment
   blue was drained], waits until green can search the rows, resets green's rollup result cache, verifies the days
   it copied, and fails while any stored day failed verify and hasn't passed since. With the aliases set, it also
   fails while one of them still selects blue.

Every step is recorded in the state, so a restarted run continues where the last one stopped. After the final sync
has passed, migrate only reports that the migration is done.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runMigrate(cmd, g, o)
		},
	}
	o.sync.register(cmd)
	o.sync.registerFinal(cmd)
	f := cmd.Flags()
	f.StringVar(&o.since, "since", "", "start the backfill here instead of now - retention (RFC 3339)")
	f.Float64Var(&o.preflight.Headroom, "headroom", 0.2, "preflight: free space green needs on top of blue's used bytes (0.2 = 20%)")
	f.DurationVar(&o.preflight.RateWindow, "rate-window", 30*time.Second, "preflight: time between the two scrapes that measure ingestion rates")
	f.DurationVar(&o.poll, "poll-interval", time.Minute, "how often to try again while a VMCluster is missing or a preflight check fails")
	f.DurationVar(&o.readyAfter, "ready-after", 10*time.Minute, "a catch-up that takes less than this makes green ready for the cutover")
	return cmd
}

func runMigrate(cmd *cobra.Command, g *globalOptions, o *migrateOptions) error {
	if err := o.sync.validate(); err != nil {
		return err
	}
	if err := o.sync.validateFinal(); err != nil {
		return err
	}
	if o.preflight.RateWindow <= 0 || o.poll <= 0 || o.readyAfter <= 0 {
		return errors.New("--rate-window, --poll-interval and --ready-after must be positive")
	}
	if o.since != "" {
		if _, err := parseTime("--since", o.since, time.Now()); err != nil {
			return err
		}
	}
	e, err := g.setup(cmd, false)
	if err != nil {
		return err
	}
	metrics, phase, stop, err := o.sync.serveMetrics(e)
	if err != nil {
		return err
	}
	defer stop()
	m := &migrator{
		e:    e,
		o:    o,
		open: e.openClusters,
		preflight: func(ctx context.Context) check.Report {
			opts := o.preflight
			opts.Aliases = e.aliases
			return check.Preflight(ctx, e.blue, e.green, opts)
		},
		phase: phase,
		now:   time.Now,
		sleep: sleep,
		newSyncer: func() (*migration.Syncer, error) {
			return newSyncer(cmd, e, &o.sync, metrics)
		},
	}
	return m.run(cmd.Context())
}

// migrator runs a whole migration. Its fields are what the tests replace.
type migrator struct {
	e     *env
	o     *migrateOptions
	phase *migration.Phase
	// open opens both VMClusters into e.blue and e.green.
	open      func(context.Context) error
	preflight func(context.Context) check.Report
	newSyncer func() (*migration.Syncer, error)
	now       func() time.Time
	sleep     func(context.Context, time.Duration) error
}

func (m *migrator) run(ctx context.Context) error {
	st, err := m.e.store.Load(ctx)
	if err != nil {
		return err
	}
	if !st.CompletedAt.IsZero() {
		m.phase.Set(migration.PhaseDone)
		fmt.Fprintf(m.e.out, "The migration finished at %s.\n%s\n", st.CompletedAt.UTC().Format(time.RFC3339), done)
		return nil
	}
	if st.BackfillStart.IsZero() {
		err = m.waitForPreflight(ctx)
	} else {
		// Preflight passed before the backfill started. Running it again would fail the capacity check, since green
		// holds the copied data by now.
		err = m.waitForClusters(ctx)
	}
	if err != nil {
		return err
	}
	syncer, err := m.newSyncer()
	if err != nil {
		return err
	}

	// A cutover in the state means the final sync started earlier: blue was drained then.
	cutover := st.CutoverAt
	for {
		if cutover.IsZero() {
			if cutover, err = m.catchUpUntilCutover(ctx, syncer); err != nil {
				return err
			}
			// Keep the first cutover noticed, so a final sync after a restart or a failed drain check still starts
			// there: rows blue delivers late carry its timestamps.
			noticed := cutover
			saved, err := m.e.store.Update(ctx, func(s *state.State) {
				if s.CutoverSeenAt.IsZero() || noticed.Before(s.CutoverSeenAt) {
					s.CutoverSeenAt = noticed
				}
			})
			if err != nil {
				return err
			}
			cutover = saved.CutoverSeenAt
			if cutover.Before(noticed) {
				m.e.log.Info("starting the final sync at the cutover noticed before the restart or the failed drain check",
					"cutover", cutover.UTC().Format(time.RFC3339))
			}
		}
		m.phase.Set(migration.PhaseFinal)
		m.e.log.Info("running the final sync", "cutover", cutover.UTC().Format(time.RFC3339))
		results, err := finalSync(ctx, m.e, &m.o.sync, syncer, cutover)
		if errors.Is(err, errNotDrained) {
			m.e.log.Warn("blue still receives writes, so the cutover isn't done: catching up until the writers point at green",
				"check", results[0].Message)
			// The writers hadn't all moved, so this was no cutover.
			if _, err := m.e.store.Update(ctx, func(st *state.State) { st.CutoverSeenAt = time.Time{} }); err != nil {
				return err
			}
			cutover = time.Time{}
			continue
		}
		printReport(m.e.out, results)
		if err != nil {
			return err
		}
		if check.AnyFailed(results) {
			fmt.Fprintln(m.e.out, "\n"+notDone)
			return errChecksFailed
		}
		break
	}
	// The final checks passed, but they cover only the final window: make sure neither VMCluster was created again.
	if err := m.waitForClusters(ctx); err != nil {
		return err
	}
	if _, err := m.e.store.Update(ctx, func(st *state.State) { st.CompletedAt = m.now() }); err != nil {
		return err
	}
	m.phase.Set(migration.PhaseDone)
	fmt.Fprintln(m.e.out, "\n"+done)
	return nil
}

// waitForClusters opens both VMClusters, and tries again every --poll-interval while that fails, for example
// because green doesn't exist yet. It gives up at once on errors that waiting can't fix: a VMCluster created again
// since the state was saved, or missing permissions.
func (m *migrator) waitForClusters(ctx context.Context) error {
	for {
		err := m.open(ctx)
		if err == nil {
			return nil
		}
		if permanent(err) {
			return err
		}
		m.e.log.Warn("waiting for both VMClusters", "err", err, "retry", m.o.poll)
		if err := m.sleep(ctx, m.o.poll); err != nil {
			return err
		}
	}
}

// waitForPreflight runs preflight every --poll-interval until no check fails. It reads the VMClusters again every
// time, so it sees the fixes made in the meantime.
func (m *migrator) waitForPreflight(ctx context.Context) error {
	m.phase.Set(migration.PhasePreflight)
	for {
		if err := m.waitForClusters(ctx); err != nil {
			return err
		}
		r := m.preflight(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
		if r.DualWriteActive {
			if _, err := m.e.store.Update(ctx, func(st *state.State) {
				if st.DualWriteSeenAt.IsZero() {
					st.DualWriteSeenAt = m.now().UTC()
				}
			}); err != nil {
				return err
			}
		}
		if !r.Failed() {
			printReport(m.e.out, r.Results)
			return nil
		}
		for _, res := range r.Results {
			if res.Level != check.Fail {
				continue
			}
			if permanent(res.Err) {
				printReport(m.e.out, r.Results)
				return fmt.Errorf("preflight check %s: %w", res.Name, res.Err)
			}
			m.e.log.Warn("preflight check failed", "check", res.Name, "message", res.Message)
		}
		m.e.log.Info("waiting until every preflight check passes", "retry", m.o.poll)
		if err := m.sleep(ctx, m.o.poll); err != nil {
			return err
		}
	}
}

// catchUpUntilCutover copies up to now, verifies the copied days and measures the writes, again and again, until
// blue receives no more rows while green still does. It returns the start of that measurement as the cutover.
func (m *migrator) catchUpUntilCutover(ctx context.Context, syncer *migration.Syncer) (time.Time, error) {
	for {
		// Open the VMClusters again before every copy: one created again since would make the state wrong.
		if err := m.waitForClusters(ctx); err != nil {
			return time.Time{}, err
		}
		st, err := m.e.store.Load(ctx)
		if err != nil {
			return time.Time{}, err
		}
		switch {
		case st.SyncedUntil.IsZero():
			m.phase.Set(migration.PhaseBackfill)
		case st.ReadyAt.IsZero():
			m.phase.Set(migration.PhaseCatchUp)
		default:
			m.phase.Set(migration.PhaseReady)
		}
		start, err := m.backfillStart()
		if err != nil {
			return time.Time{}, err
		}
		began := m.now()
		if err := syncer.Sync(ctx, start, began); err != nil {
			return time.Time{}, err
		}
		took := m.now().Sub(began)
		m.e.log.Info("synced up to now", "took", took.Round(time.Second))
		if err := m.verifyCopiedDays(ctx, syncer); err != nil {
			return time.Time{}, err
		}
		if took < m.o.readyAfter && st.ReadyAt.IsZero() {
			if _, err := m.e.store.Update(ctx, func(st *state.State) { st.ReadyAt = m.now() }); err != nil {
				return time.Time{}, err
			}
			m.phase.Set(migration.PhaseReady)
			m.e.log.Info("green is ready for the cutover: switch the read and write aliases to green; " +
				"the final sync starts by itself once blue receives no more writes")
		}

		at := m.now()
		w, err := check.WatchWrites(ctx, m.e.blue, m.e.green, m.o.sync.drain.Interval, m.sleep)
		if err := ctx.Err(); err != nil {
			return time.Time{}, err
		}
		switch {
		case err != nil:
			m.e.log.Warn("can't measure the writes, trying again after the next catch-up", "err", err, "retry", m.o.poll)
			if err := m.sleep(ctx, m.o.poll); err != nil {
				return time.Time{}, err
			}
		case w.Switched:
			if pending := m.aliasesNotOnGreen(ctx); len(pending) > 0 {
				m.e.log.Info("the writers point at green, waiting for the aliases", "aliases", pending)
				continue
			}
			m.e.log.Info("the writers point at green: blue receives no more rows",
				"greenRowsPerSecond", int64(w.GreenRate))
			return at, nil
		default:
			args := []any{"blueRowsPerSecond", int64(w.BlueRate), "greenRowsPerSecond", int64(w.GreenRate)}
			if w.BlueRate > 0 && !st.CutoverSeenAt.IsZero() {
				// Blue receives writes again (e.g. after a rollback): the cutover noticed earlier no longer holds.
				if _, err := m.e.store.Update(ctx, func(s *state.State) { s.CutoverSeenAt = time.Time{} }); err != nil {
					return time.Time{}, err
				}
			}
			if m.e.aliases.Configured() && len(m.aliasesNotOnGreen(ctx)) == 0 {
				m.e.log.Warn("the aliases select green, but blue still receives rows: some writer bypasses the "+
					"aliases; point it at green", args...)
				continue
			}
			m.e.log.Info("waiting for the cutover", args...)
		}
	}
}

// aliasesNotOnGreen returns why each configured alias doesn't select only green's pods yet. Readers that still
// use blue would lose their data when blue is deleted, so migrate takes the cutover as done only when both the
// writes and the aliases have moved.
func (m *migrator) aliasesNotOnGreen(ctx context.Context) []string {
	var pending []string
	for _, r := range check.CheckAliases(ctx, m.e.aliases, m.e.blue, m.e.green, m.e.green, check.Fail) {
		if r.Level != check.Pass {
			pending = append(pending, r.Message)
		}
	}
	return pending
}

// backfillStart is where the first sync starts: --since, or now - blue's retention.
func (m *migrator) backfillStart() (time.Time, error) {
	if m.o.since != "" {
		return parseTime("--since", m.o.since, m.now())
	}
	r, err := parseRetention(m.e.blue.Info().Retention)
	if err != nil {
		return time.Time{}, err
	}
	return m.now().Add(-r), nil
}

// verifyCopiedDays verifies every full day up to the last synced point that hasn't passed a whole-day verify yet. A
// day that fails is copied again once and verified again; if it still fails, migrate stops, because green would
// miss data after the cutover.
func (m *migrator) verifyCopiedDays(ctx context.Context, syncer *migration.Syncer) error {
	st, err := m.e.store.Load(ctx)
	if err != nil {
		return err
	}
	days := unverifiedDays(st)
	if len(days) == 0 {
		return nil
	}
	if err := m.sleep(ctx, importSettle); err != nil {
		return err
	}
	failed, _, err := m.verify(ctx, days)
	if err != nil || len(failed) == 0 {
		return err
	}
	for _, d := range failed {
		m.e.log.Warn("green misses data, copying the day again", "day", state.DayKey(d))
		if err := syncer.Copy(ctx, migration.Window{Start: d, End: d.Add(day)}); err != nil {
			return err
		}
	}
	if err := m.sleep(ctx, importSettle); err != nil {
		return err
	}
	failed, results, err := m.verify(ctx, failed)
	if err != nil || len(failed) == 0 {
		return err
	}
	printReport(m.e.out, results)
	fmt.Fprintln(m.e.out, "\nGreen still misses data on these days after copying them again. Find the cause (see the "+
		"troubleshooting guide), then start migrate again: it verifies and copies the failed days again.")
	return errChecksFailed
}

// verify verifies the given days and returns those that failed, with the results of those.
func (m *migrator) verify(ctx context.Context, days []time.Time) ([]time.Time, []check.Result, error) {
	var failed []time.Time
	var results []check.Result
	for _, d := range days {
		r, err := verifyDays(ctx, m.e, m.o.sync.verify, d, d.Add(day))
		if err != nil {
			return nil, nil, err
		}
		if check.AnyFailed(r) {
			failed, results = append(failed, d), append(results, r...)
		}
	}
	return failed, results, nil
}

// unverifiedDays returns the full UTC days from the first full day of the backfill up to the last synced point
// that have no stored whole-day pass.
func unverifiedDays(st *state.State) []time.Time {
	if st.BackfillStart.IsZero() {
		return nil
	}
	var days []time.Time
	for d := firstFullDay(st); !d.Add(day).After(st.SyncedUntil); d = d.Add(day) {
		if r, ok := st.Verifications[state.DayKey(d)]; !ok || !r.Pass || r.Partial {
			days = append(days, d)
		}
	}
	return days
}

// permanent reports whether err is one that waiting can't fix: a VMCluster created again since the state was saved,
// or missing permissions.
func permanent(err error) bool {
	var replaced *state.ClusterReplacedError
	return errors.As(err, &replaced) || apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err)
}
