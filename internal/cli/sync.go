package cli

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/check"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/cluster"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/copier"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/migration"
)

type syncOptions struct {
	until, since string
	final        bool
	cutoverAt    string
	step         time.Duration
	retries      int
	backoff      time.Duration

	vmctl       string
	vmctlArgs   []string
	concurrency int
	rateLimit   string
	srcURL      string
	dstURL      string

	drain  check.DrainOptions
	verify verifyFlags
}

func newSync(g *globalOptions) *cobra.Command {
	o := &syncOptions{drain: check.DrainOptions{Sleep: sleep}}
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Copy data from blue to green in resumable chunks; with --final, run the last sync after cutover",
		Long: `sync copies blue's data into green through green's vminsert, one chunk (--step) at a time. Every finished
chunk is recorded in the state, so a restarted run continues where the last one stopped.

The first run starts at now - retention. Later runs continue from the last finished chunk up to --until. Repeat
sync (catch-up) until one run takes only minutes, then switch the read and write aliases to green.

sync --final runs after the cutover. It copies [last synced point - 1h, cutover + 15m], waiting until that time
has passed, resets the rollup result cache on every green vmselect pod, checks that blue no longer receives
writes, and verifies the days it copied.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSync(cmd, g, o)
		},
	}
	f := cmd.Flags()
	f.StringVar(&o.until, "until", "now", `copy up to this time ("now" or RFC 3339)`)
	f.StringVar(&o.since, "since", "", "on the first run, start here instead of now - retention (RFC 3339)")
	f.BoolVar(&o.final, "final", false, "run the final sync after cutover")
	f.StringVar(&o.cutoverAt, "cutover-at", "", `with --final (required): when the aliases were switched to green (RFC 3339, or "now")`)
	f.DurationVar(&o.step, "step", 24*time.Hour, "chunk size")
	f.IntVar(&o.retries, "retries", 3, "retries per failed chunk")
	f.DurationVar(&o.backoff, "backoff", time.Minute, "wait before the first retry; doubles with every further retry")
	f.StringVar(&o.vmctl, "vmctl", "vmctl", "path to the vmctl binary")
	f.StringArrayVar(&o.vmctlArgs, "vmctl-arg", nil, "extra argument for vmctl, e.g. --vmctl-arg=--vm-native-disable-http-keep-alive (repeatable)")
	f.IntVar(&o.concurrency, "concurrency", 2, "vmctl import workers (--vm-concurrency)")
	f.StringVar(&o.rateLimit, "rate-limit", "", "transfer rate limit, e.g. 50MiB/s (default: none)")
	f.StringVar(&o.srcURL, "from-select-url", "", "blue's vmselect URL for vmctl, e.g. after a port-forward (default: the Service URL)")
	f.StringVar(&o.dstURL, "to-insert-url", "", "green's vminsert URL for vmctl, e.g. after a port-forward (default: the Service URL)")
	f.DurationVar(&o.drain.Timeout, "drain-timeout", 20*time.Minute, "with --final: how long blue may still receive writes after the final sync")
	f.DurationVar(&o.drain.Interval, "drain-interval", 5*time.Minute, "with --final: blue must receive no writes for this long; make it longer than the push interval of your slowest writer")
	o.verify.register(cmd)
	return cmd
}

func runSync(cmd *cobra.Command, g *globalOptions, o *syncOptions) error {
	if o.step <= 0 {
		return errors.New("--step must be positive")
	}
	if o.final && (o.cutoverAt == "" || o.drain.Interval <= 0 || o.drain.Timeout <= 0) {
		return errors.New("--final needs --cutover-at, and positive --drain-interval and --drain-timeout")
	}
	e, err := g.setup(cmd, true)
	if err != nil {
		return err
	}
	syncer, err := newSyncer(cmd, e, o)
	if err != nil {
		return err
	}
	if o.final {
		return runFinal(cmd.Context(), e, o, syncer)
	}

	now := time.Now()
	until, err := parseTime("--until", o.until, now)
	if err != nil {
		return err
	}
	start := now
	if o.since != "" {
		if start, err = parseTime("--since", o.since, now); err != nil {
			return err
		}
	} else {
		r, err := parseRetention(e.blue.Info().Retention)
		if err != nil {
			return err
		}
		start = now.Add(-r)
	}
	began := time.Now()
	if err := syncer.Sync(cmd.Context(), start, minTime(until, now)); err != nil {
		return err
	}
	st, err := e.store.Load(cmd.Context())
	if err != nil {
		return err
	}
	fmt.Fprintf(e.out, "Synced until %s (this run took %s).\n", st.SyncedUntil.UTC().Format(time.RFC3339), time.Since(began).Round(time.Second))
	fmt.Fprintln(e.out, "Repeat sync until a run takes only minutes, then switch the aliases to green and run sync --final.")
	return nil
}

func newSyncer(cmd *cobra.Command, e *env, o *syncOptions) (*migration.Syncer, error) {
	rate, err := copier.ParseRate(o.rateLimit)
	if err != nil {
		return nil, err
	}
	src, dst := o.srcURL, o.dstURL
	if src == "" {
		src = e.blue.Info().URL[cluster.Select]
	}
	if dst == "" {
		dst = e.green.Info().URL[cluster.Insert]
	}
	if src == "" || dst == "" {
		return nil, errors.New("blue needs a vmselect and green a vminsert")
	}
	return &migration.Syncer{
		Copier: &copier.VMCtl{
			Binary:      o.vmctl,
			SrcAddr:     src + cluster.SelectPrefix(e.tenant),
			DstAddr:     dst + cluster.InsertPrefix(e.tenant),
			Concurrency: o.concurrency,
			RateLimit:   rate,
			ExtraArgs:   o.vmctlArgs,
			Output:      cmd.ErrOrStderr(),
		},
		Store:   e.store,
		Step:    o.step,
		Retries: o.retries,
		Backoff: o.backoff,
		Now:     time.Now,
		Sleep:   sleep,
		Log:     e.log,
	}, nil
}

// runFinal runs the last sync after cutover, then the post-cutover checks.
func runFinal(ctx context.Context, e *env, o *syncOptions, syncer *migration.Syncer) error {
	cutover, err := parseTime("--cutover-at", o.cutoverAt, time.Now())
	if err != nil {
		return err
	}
	w, err := syncer.Final(ctx, cutover)
	if err != nil {
		return err
	}
	e.log.Info("final sync copied", "window", w.String())

	results := []check.Result{{Name: check.RollupCache, Level: check.Pass, Message: "reset on every green vmselect pod"}}
	if err := cluster.ResetRollupCache(ctx, e.green); err != nil {
		results[0].Level, results[0].Message = check.Fail, err.Error()
	}
	results = append(results, check.WaitDrained(ctx, e.blue, o.drain))
	days, err := verifyDays(ctx, e, o.verify, w.Start, w.End)
	if err != nil {
		return err
	}
	results = append(results, days...)
	printReport(e.out, results)
	if check.AnyFailed(results) {
		fmt.Fprintln(e.out, "\nDon't delete blue yet: fix the failed checks first. Rollback: switch the aliases back to blue.")
		return errChecksFailed
	}
	fmt.Fprintln(e.out, "\nDone. Keep blue for a few days so you can roll back, then delete it and its PVCs.")
	return nil
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
