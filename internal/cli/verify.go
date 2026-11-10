package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/check"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/state"
)

const day = 24 * time.Hour

type verifyFlags struct {
	check.VerifyOptions
}

func (v *verifyFlags) register(cmd *cobra.Command) {
	f := cmd.Flags()
	f.IntVar(&v.TopN, "top-n", 10, "verify: count the samples of this many metric names with the most series")
	f.IntVar(&v.RandomN, "random-n", 10, "verify: count the samples of this many further metric names, picked at random")
	f.IntVar(&v.Pool, "pool", 1000, "verify: pick the random names from this many names below the top N")
	f.Float64Var(&v.Tolerance, "tolerance", 0.01, "verify: share by which green may fall short of blue (0.01 = 1%)")
}

func (v *verifyFlags) validate() error {
	if v.TopN < 0 || v.RandomN < 0 || v.Pool < 0 {
		return errors.New("--top-n, --random-n and --pool must not be negative")
	}
	// Written as the condition that must hold, so NaN (for which every comparison is false) is rejected too.
	if !(v.Tolerance >= 0 && v.Tolerance < 1) {
		return errors.New("--tolerance must be a share between 0 and 1, e.g. 0.01")
	}
	return nil
}

func newVerify(g *globalOptions) *cobra.Command {
	var v verifyFlags
	var since, until string
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Compare blue and green per day: series counts and sample counts of sampled metrics",
		Long: `verify compares blue and green for every UTC day in [--since, --until): the number of series in the day's
index, and the number of samples for the top N metric names plus a random set. It only reads from the clusters and
can run at any time, also during a sync. Results are stored in the state.

By default it checks every day from the first full day after the backfill start up to the last synced point. The
last day is usually partial: there only the sample counts up to the last synced point are compared, because the
day's series count also includes series written afterwards.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := v.validate(); err != nil {
				return err
			}
			e, err := g.setup(cmd, true)
			if err != nil {
				return err
			}
			st, err := e.store.Load(cmd.Context())
			if err != nil {
				return err
			}
			from, to, err := verifyRange(since, until, st)
			if err != nil {
				return err
			}
			results, err := verifyDays(cmd.Context(), e, v, from, to)
			if err != nil {
				return err
			}
			printReport(e.out, results)
			if check.AnyFailed(results) {
				return errChecksFailed
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&since, "since", "", "first day to check (RFC 3339; default: the first full day after the backfill start)")
	cmd.Flags().StringVar(&until, "until", "", "check up to here (RFC 3339; default: the last synced point)")
	v.register(cmd)
	return cmd
}

// firstFullDay is the first UTC day the backfill copied completely: the day it started on is only partly copied,
// unless it started at midnight.
func firstFullDay(st *state.State) time.Time {
	from := st.BackfillStart.UTC().Truncate(day)
	if !from.Equal(st.BackfillStart) {
		from = from.Add(day)
	}
	return from
}

func verifyRange(since, until string, st *state.State) (time.Time, time.Time, error) {
	var from, to time.Time
	var err error
	now := time.Now()
	if since != "" {
		if from, err = parseTime("--since", since, now); err != nil {
			return from, to, err
		}
	} else {
		if st.BackfillStart.IsZero() {
			return from, to, errors.New("nothing synced yet: pass --since and --until, or run migrate first")
		}
		from = firstFullDay(st)
	}
	if until != "" {
		if to, err = parseTime("--until", until, now); err != nil {
			return from, to, err
		}
	} else {
		to = st.SyncedUntil
	}
	if !from.Before(to) {
		return from, to, fmt.Errorf("nothing to verify between %s and %s", from.Format(time.RFC3339), to.Format(time.RFC3339))
	}
	return from, to, nil
}

// verifyDays verifies every UTC day that overlaps [from, to), up to to, stores the results in the state and returns
// one check result per day.
func verifyDays(ctx context.Context, e *env, v verifyFlags, from, to time.Time) ([]check.Result, error) {
	var results []check.Result
	var days []time.Time
	var dayResults []state.DayResult
	for d := from.UTC().Truncate(day); d.Before(to); d = d.Add(day) {
		start, end := maxTime(d, from), minTime(d.Add(day), to)
		e.log.Info("verifying", "day", state.DayKey(d), "from", start.UTC().Format(time.RFC3339), "until", end.UTC().Format(time.RFC3339))
		r := check.VerifyDay(ctx, e.blue, e.green, start, end, v.VerifyOptions)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		days, dayResults = append(days, d), append(dayResults, r)
		msg := fmt.Sprintf("series: blue %d, green %d", r.BlueSeries, r.GreenSeries)
		if r.Partial {
			msg = fmt.Sprintf("samples %s–%s match (partial day, series not compared)",
				start.UTC().Format(time.TimeOnly), end.UTC().Format(time.TimeOnly))
		}
		res := check.Result{Name: "verify " + state.DayKey(d), Level: check.Pass, Message: msg}
		if !r.Pass {
			res.Level, res.Message, res.Err = check.Fail, strings.Join(r.Problems, "; "), r.Err
		}
		results = append(results, res)
	}
	if _, err := e.store.Update(ctx, func(st *state.State) {
		for i := range days {
			// A day whose queries failed is unverified, not known to miss data: keep what the state had.
			if dayResults[i].Err == nil {
				st.SetVerification(days[i], dayResults[i])
			}
		}
	}); err != nil {
		return nil, err
	}
	return results, nil
}
