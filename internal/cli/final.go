package cli

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/check"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/cluster"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/migration"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/state"
)

var (
	errNotDrained = errors.New("blue still receives writes")
	// errDrainFailed stops the final sync when the drain check failed for another reason.
	errDrainFailed = errors.New("drain check failed")
)

const (
	done    = "Done. Keep blue for a few days so you can roll back, then delete it and its PVCs."
	notDone = "Don't delete blue yet: fix the failed checks first. Rollback: switch the aliases back to blue."
)

// finalSync checks that blue is drained, runs the last sync for the cutover, then the post-cutover checks, and
// returns their results. When blue still receives writes, it copies nothing and returns errNotDrained with the
// drain check's result. When the drain check fails for another reason (blue can't deliver its rows, its pods keep
// restarting), it copies nothing either and returns only that failed result.
func finalSync(ctx context.Context, e *env, o *syncOptions, syncer *migration.Syncer, cutover time.Time) ([]check.Result, error) {
	var drain check.Result
	w, err := syncer.Final(ctx, cutover, func(ctx context.Context) (time.Time, error) {
		e.log.Info("checking that blue receives no more writes", "interval", o.drain.Interval, "timeout", o.drain.Timeout)
		var writing bool
		switch drain, writing = check.WaitDrained(ctx, e.blue, o.drain); {
		case writing:
			return time.Time{}, errNotDrained
		case drain.Level == check.Fail:
			return time.Time{}, errDrainFailed
		}
		return syncer.Now(), nil
	})
	switch {
	case errors.Is(err, errNotDrained):
		return []check.Result{drain}, err
	case errors.Is(err, errDrainFailed):
		return []check.Result{drain}, nil
	}
	if err != nil {
		return nil, err
	}
	e.log.Info("final sync copied", "window", w.String())
	// vminsert acknowledges rows before vmstorage makes them searchable. Reset the cache and verify only after that,
	// so neither a cached result nor the count misses the last rows.
	e.log.Info("waiting for green to make the imported rows searchable", "wait", importSettle)
	if err := syncer.Sleep(ctx, importSettle); err != nil {
		return nil, err
	}

	results := []check.Result{drain, {Name: check.RollupCache, Level: check.Pass, Message: "reset on every green vmselect pod"}}
	if err := cluster.ResetRollupCache(ctx, e.green); err != nil {
		results[1].Level, results[1].Message = check.Fail, err.Error()
	}
	// Readers that still use blue would lose their data when blue is deleted.
	results = append(results, check.CheckAliases(ctx, e.aliases, e.blue, e.green, e.green, check.Fail)...)
	days, err := verifyDays(ctx, e, o.verify, w.Start, w.End)
	results = append(results, days...)
	if err == nil {
		var st *state.State
		if st, err = e.store.Load(ctx); err == nil {
			results = append(results, earlierFailures(st, days))
		}
	}
	return results, err
}

// importSettle is how long the final sync waits after copying before it resets green's rollup cache and verifies.
// vmstorage makes new rows searchable within seconds.
const importSettle = 30 * time.Second

// earlierFailures reports days in the migrated range (the first full day after the backfill start up to the last
// synced point) whose stored verify result failed, except those this run already reports as failed. A pass that
// didn't cover the failed time range keeps the failure (see state.SetVerification), so such a day shows up here
// until a verify of at least that range passes.
// Days outside the range, such as the partly copied day the backfill started on, don't count.
func earlierFailures(st *state.State, thisRun []check.Result) check.Result {
	reported := map[string]bool{}
	for _, r := range thisRun {
		if r.Level == check.Fail {
			reported[strings.TrimPrefix(r.Name, "verify ")] = true
		}
	}
	first, last := state.DayKey(firstFullDay(st)), state.DayKey(st.SyncedUntil)
	var days []string
	for day, r := range st.Verifications {
		// Day keys are YYYY-MM-DD, so they compare in date order.
		if !r.Pass && !reported[day] && day >= first && day <= last {
			days = append(days, day)
		}
	}
	if len(days) == 0 {
		return check.Result{Name: "earlier verify", Level: check.Pass, Message: "no failed days stored"}
	}
	slices.Sort(days)
	return check.Result{Name: "earlier verify", Level: check.Fail, Message: fmt.Sprintf(
		"these days failed verify and no later verify of that time range passed: %s; verify them again, and if green "+
			"really misses data there, copy those days again (a sync with its own --state-file, --since and --until)",
		strings.Join(days, ", "))}
}
