package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/spf13/cobra"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/check"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/cluster"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/copier"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/migration"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/state"
)

type syncOptions struct {
	until, since string
	final        bool
	cutoverAt    string
	step         time.Duration
	retries      int
	backoff      time.Duration
	finalOverlap time.Duration

	vmctl       string
	vmctlArgs   []string
	concurrency int
	rateLimit   string
	srcURL      string
	dstURL      string

	adaptive       bool
	minRateLimit   string
	maxUtilization float64
	maxQueryP99    time.Duration
	metricsAddr    string

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

sync --final runs after the cutover. It waits until cutover + 15m and checks that blue no longer receives writes;
if it still does, it stops without copying. Then it copies [last synced point - --final-overlap (1h), the moment
blue was drained] (from the cutover - --final-overlap if a catch-up already ran past the cutover). It waits 30s
until green can search the copied rows, resets the rollup result cache on every green vmselect pod, verifies the
days it copied, and fails while any day stored in the state failed verify and hasn't passed since. A rerun with the
same --cutover-at covers the first run's window again.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSync(cmd, g, o)
		},
	}
	f := cmd.Flags()
	f.StringVar(&o.until, "until", "now", `copy up to this time ("now" or RFC 3339)`)
	f.StringVar(&o.since, "since", "", "on the first run, start here instead of now - retention (RFC 3339)")
	f.BoolVar(&o.final, "final", false, "run the final sync after cutover")
	f.StringVar(&o.cutoverAt, "cutover-at", "", "with --final (required): when the aliases were switched to green (RFC 3339)")
	f.DurationVar(&o.step, "step", 24*time.Hour, "chunk size")
	f.IntVar(&o.retries, "retries", 3, "retries per failed chunk")
	f.DurationVar(&o.backoff, "backoff", time.Minute, "wait before the first retry; doubles with every further retry")
	f.DurationVar(&o.finalOverlap, "final-overlap", migration.DefaultFinalOverlap, "with --final: start this long before the last synced point; must cover the oldest timestamps that writers bypassing dual write still send")
	f.StringVar(&o.vmctl, "vmctl", "vmctl", "path to the vmctl binary")
	f.StringArrayVar(&o.vmctlArgs, "vmctl-arg", nil, "extra argument for vmctl, e.g. --vmctl-arg=--vm-native-disable-http-keep-alive (repeatable)")
	f.IntVar(&o.concurrency, "concurrency", 2, "vmctl import workers (--vm-concurrency)")
	f.StringVar(&o.rateLimit, "rate-limit", "", "total transfer rate limit across all vmctl workers, e.g. 50MiB/s (default: none); with --adaptive the highest rate")
	f.BoolVar(&o.adaptive, "adaptive", false, "lower the rate limit while blue's vmselect is busy, checked before every chunk (needs --rate-limit; use a small --step such as 1h)")
	f.StringVar(&o.minRateLimit, "min-rate-limit", "", "with --adaptive: the lowest rate (default: a tenth of --rate-limit)")
	f.Float64Var(&o.maxUtilization, "max-select-utilization", 0.8, "with --adaptive: share of blue's vmselect query slots in use above which blue counts as busy")
	f.DurationVar(&o.maxQueryP99, "max-query-latency", 0, "with --adaptive: p99 latency of blue's queries above which blue counts as busy (default: ignore latency)")
	f.StringVar(&o.metricsAddr, "metrics-addr", "", "serve the tool's Prometheus metrics on this address, e.g. :8080 (default: off)")
	f.StringVar(&o.srcURL, "from-select-url", "", "blue's vmselect URL for vmctl, e.g. after a port-forward (default: the Service URL)")
	f.StringVar(&o.dstURL, "to-insert-url", "", "green's vminsert URL for vmctl, e.g. after a port-forward (default: the Service URL)")
	f.DurationVar(&o.drain.Timeout, "drain-timeout", 20*time.Minute, "with --final: how long blue may still receive writes after cutover + 15m")
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
	if o.final && o.drain.Interval < time.Minute {
		return errors.New("--drain-interval must be at least 1m: a send to a hung vmstorage node takes up to a minute to fail")
	}
	if o.final {
		if err := o.verify.validate(); err != nil {
			return err
		}
		if o.finalOverlap <= 0 {
			return errors.New("--final-overlap must be positive")
		}
	}
	for _, a := range o.vmctlArgs {
		if strings.HasPrefix(a, "--vm-concurrency") || strings.HasPrefix(a, "--vm-rate-limit") {
			return fmt.Errorf("--vmctl-arg=%s: use --concurrency and --rate-limit, which split the limit across workers", a)
		}
	}
	e, err := g.setup(cmd, true)
	if err != nil {
		return err
	}
	var m *migration.Metrics
	if o.metricsAddr != "" {
		set := metrics.NewSet()
		m = migration.NewMetrics(set)
		stop, err := serveMetrics(o.metricsAddr, set, e.log)
		if err != nil {
			return err
		}
		defer stop()
	}
	syncer, err := newSyncer(cmd, e, o, m)
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
	var start time.Time
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

func newSyncer(cmd *cobra.Command, e *env, o *syncOptions, m *migration.Metrics) (*migration.Syncer, error) {
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
	pacer, err := newPacer(e, o, rate, m)
	if err != nil {
		return nil, err
	}
	return &migration.Syncer{
		Copier: &copier.VMCtl{
			Binary:      o.vmctl,
			SrcAddr:     src + cluster.SelectPrefix(e.tenant),
			DstAddr:     dst + cluster.InsertPrefix(e.tenant),
			Concurrency: o.concurrency,
			ExtraArgs:   o.vmctlArgs,
			Output:      cmd.ErrOrStderr(),
		},
		Pacer:        pacer,
		Metrics:      m,
		Store:        e.store,
		Step:         o.step,
		Retries:      o.retries,
		Backoff:      o.backoff,
		FinalOverlap: o.finalOverlap,
		Now:          time.Now,
		Sleep:        sleep,
		Log:          e.log,
	}, nil
}

// newPacer returns a fixed rate, or with --adaptive a Governor that follows blue's vmselect load.
func newPacer(e *env, o *syncOptions, rate int64, m *migration.Metrics) (migration.Pacer, error) {
	if !o.adaptive {
		return migration.FixedRate(rate), nil
	}
	if rate == 0 {
		return nil, errors.New("--adaptive needs --rate-limit as the highest rate")
	}
	if o.maxUtilization <= 0 || o.maxUtilization > 1 {
		return nil, errors.New("--max-select-utilization must be a share between 0 and 1, e.g. 0.8")
	}
	minRate, err := copier.ParseRate(o.minRateLimit)
	if err != nil {
		return nil, fmt.Errorf("--min-rate-limit: %w", err)
	}
	if minRate == 0 {
		minRate = rate / 10
	}
	if minRate > rate {
		return nil, errors.New("--min-rate-limit is above --rate-limit")
	}
	return &migration.Governor{
		Read:           func(ctx context.Context) (migration.Load, error) { return check.SelectLoad(ctx, e.blue) },
		Max:            rate,
		Min:            minRate,
		MaxUtilization: o.maxUtilization,
		MaxQueryP99:    o.maxQueryP99,
		Log:            e.log,
		Metrics:        m,
	}, nil
}

// serveMetrics serves the metrics in set, plus Go and process metrics, on addr until stop is called.
func serveMetrics(addr string, set *metrics.Set, log *slog.Logger) (stop func(), err error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("--metrics-addr: %w", err)
	}
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			set.WritePrometheus(w)
			metrics.WriteProcessMetrics(w)
		}),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server stopped", "err", err)
		}
	}()
	log.Info("serving metrics", "addr", ln.Addr().String())
	return func() { _ = srv.Close() }, nil
}

var errNotDrained = errors.New("blue still receives writes")

// runFinal checks that blue is drained, runs the last sync, then the post-cutover checks.
func runFinal(ctx context.Context, e *env, o *syncOptions, syncer *migration.Syncer) error {
	if o.cutoverAt == "now" {
		// A rerun must name the same cutover to cover the first run's window again.
		return errors.New(`--cutover-at needs the RFC 3339 time you switched the aliases, not "now"`)
	}
	cutover, err := parseTime("--cutover-at", o.cutoverAt, time.Now())
	if err != nil {
		return err
	}
	var drain check.Result
	w, err := syncer.Final(ctx, cutover, func(ctx context.Context) (time.Time, error) {
		e.log.Info("checking that blue receives no more writes", "interval", o.drain.Interval, "timeout", o.drain.Timeout)
		drain = check.WaitDrained(ctx, e.blue, o.drain)
		if drain.Level == check.Fail {
			return time.Time{}, errNotDrained
		}
		return syncer.Now(), nil
	})
	if errors.Is(err, errNotDrained) {
		printReport(e.out, []check.Result{drain})
		fmt.Fprintln(e.out, "\nNothing copied. Fix the cause above, then run sync --final again with the same --cutover-at.")
		return errChecksFailed
	}
	if err != nil {
		return err
	}
	e.log.Info("final sync copied", "window", w.String())
	// vminsert acknowledges rows before vmstorage makes them searchable. Reset the cache and verify only after that,
	// so neither a cached result nor the count misses the last rows.
	e.log.Info("waiting for green to make the imported rows searchable", "wait", importSettle)
	if err := sleep(ctx, importSettle); err != nil {
		return err
	}

	results := []check.Result{drain, {Name: check.RollupCache, Level: check.Pass, Message: "reset on every green vmselect pod"}}
	if err := cluster.ResetRollupCache(ctx, e.green); err != nil {
		results[1].Level, results[1].Message = check.Fail, err.Error()
	}
	days, err := verifyDays(ctx, e, o.verify, w.Start, w.End)
	results = append(results, days...)
	if err == nil {
		var st *state.State
		if st, err = e.store.Load(ctx); err == nil {
			results = append(results, earlierFailures(st, days))
		}
	}
	printReport(e.out, results)
	if err != nil {
		return err
	}
	if check.AnyFailed(results) {
		fmt.Fprintln(e.out, "\nDon't delete blue yet: fix the failed checks first. Rollback: switch the aliases back to blue.")
		return errChecksFailed
	}
	fmt.Fprintln(e.out, "\nDone. Keep blue for a few days so you can roll back, then delete it and its PVCs.")
	return nil
}

// importSettle is how long the final sync waits after copying before it resets green's rollup cache and verifies.
// vmstorage makes new rows searchable within seconds.
const importSettle = 30 * time.Second

// earlierFailures reports days in the migrated range (the first full day after the backfill start up to the last
// synced point) whose stored verify result failed, except those this run already reports as failed. A pass of only
// part of a day keeps an earlier whole-day failure, so such a day shows up here until a whole-day verify passes.
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
		"these days failed verify and haven't passed a whole-day verify since: %s; verify them again, and if green "+
			"really misses data there, copy those days again (a sync with its own --state-file, --since and --until)",
		strings.Join(days, ", "))}
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
