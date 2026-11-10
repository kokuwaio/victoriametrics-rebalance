package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/spf13/cobra"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/check"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/cluster"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/copier"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/migration"
)

type syncOptions struct {
	until, since string
	step         time.Duration
	retries      int
	backoff      time.Duration
	finalOverlap time.Duration
	finalTail    time.Duration

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
	o := &syncOptions{}
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Copy a time range from --from to --to in resumable chunks, e.g. back to blue after a rollback",
		Long: `sync copies data from --from into --to through the target's vminsert, one chunk (--step) at a time. Every
finished chunk is recorded in the state, so a restarted run continues where the last one stopped.

migrate runs the backfill, the catch-ups and the final sync by itself. Use sync for copies outside a migration,
or to copy a range again with its own --state-file, --since and --until.

After a rollback, copy what green received since the switch back to blue: swap --from and --to, and set --since
to a time before the switch, such as Ready in status if you switched after it. The copy overlaps what blue already
has, so give blue's vmstorage and vmselect -dedup.minScrapeInterval=1ms first.

The first run starts at --since (default: now - retention). Later runs continue from the last finished chunk up to
--until.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSync(cmd, g, o)
		},
	}
	f := cmd.Flags()
	f.StringVar(&o.until, "until", "now", `copy up to this time ("now" or RFC 3339)`)
	f.StringVar(&o.since, "since", "", "on the first run, start here instead of now - retention (RFC 3339)")
	o.register(cmd)
	return cmd
}

// register adds the copy flags that sync and migrate share.
func (o *syncOptions) register(cmd *cobra.Command) {
	f := cmd.Flags()
	f.DurationVar(&o.step, "step", 24*time.Hour, "chunk size")
	f.IntVar(&o.retries, "retries", 3, "retries per failed chunk")
	f.DurationVar(&o.backoff, "backoff", time.Minute, "wait before the first retry; doubles with every further retry")
	f.StringVar(&o.vmctl, "vmctl", "vmctl", "path to the vmctl binary")
	f.StringArrayVar(&o.vmctlArgs, "vmctl-arg", nil, "extra argument for vmctl, e.g. --vmctl-arg=--vm-native-disable-http-keep-alive (repeatable)")
	f.IntVar(&o.concurrency, "concurrency", 2, "vmctl import workers (--vm-concurrency)")
	f.StringVar(&o.rateLimit, "rate-limit", "", "total transfer rate limit across all vmctl workers, e.g. 50MiB/s (default: none); with --adaptive the highest rate")
	f.BoolVar(&o.adaptive, "adaptive", false, "lower the rate limit while the source's vmselect is busy, checked before every chunk (needs --rate-limit; use a small --step such as 1h)")
	f.StringVar(&o.minRateLimit, "min-rate-limit", "", "with --adaptive: the lowest rate (default: a tenth of --rate-limit)")
	f.Float64Var(&o.maxUtilization, "max-select-utilization", 0.8, "with --adaptive: share of the source's vmselect query slots in use above which it counts as busy")
	f.DurationVar(&o.maxQueryP99, "max-query-latency", 0, "with --adaptive: p99 latency of the source's queries above which it counts as busy (default: ignore latency)")
	f.StringVar(&o.metricsAddr, "metrics-addr", "", "serve the tool's Prometheus metrics on this address, e.g. :8080 (default: off)")
	f.StringVar(&o.srcURL, "from-select-url", "", "the source's vmselect URL for vmctl, e.g. after a port-forward (default: the Service URL)")
	f.StringVar(&o.dstURL, "to-insert-url", "", "the target's vminsert URL for vmctl, e.g. after a port-forward (default: the Service URL)")
}

// registerFinal adds the flags of the final sync and its verify.
func (o *syncOptions) registerFinal(cmd *cobra.Command) {
	f := cmd.Flags()
	f.DurationVar(&o.finalOverlap, "final-overlap", migration.DefaultFinalOverlap, "final sync: start this long before the last synced point; must cover the oldest timestamps that writers bypassing dual write still send")
	f.DurationVar(&o.finalTail, "final-tail", migration.DefaultFinalTail, "final sync: wait this long after the cutover before checking that blue is drained, so writers that switch late have stopped")
	f.DurationVar(&o.drain.Timeout, "drain-timeout", 20*time.Minute, "final sync: how long blue may still receive writes after cutover + --final-tail")
	f.DurationVar(&o.drain.Interval, "drain-interval", 5*time.Minute, "final sync: blue must receive no writes for this long; make it longer than the push interval of your slowest writer; also the window over which migrate measures the writes to notice the cutover")
	o.verify.register(cmd)
}

// validate checks the flags every sync needs.
func (o *syncOptions) validate() error {
	if o.step <= 0 {
		return errors.New("--step must be positive")
	}
	for _, a := range o.vmctlArgs {
		if strings.HasPrefix(a, "--vm-concurrency") || strings.HasPrefix(a, "--vm-rate-limit") {
			return fmt.Errorf("--vmctl-arg=%s: use --concurrency and --rate-limit, which split the limit across workers", a)
		}
	}
	return nil
}

// validateFinal checks the flags of the final sync.
func (o *syncOptions) validateFinal() error {
	if o.drain.Interval <= 0 || o.drain.Timeout <= 0 {
		return errors.New("--drain-interval and --drain-timeout must be positive")
	}
	if o.drain.Interval < time.Minute {
		return errors.New("--drain-interval must be at least 1m: a send to a hung vmstorage node takes up to a minute to fail")
	}
	if err := o.verify.validate(); err != nil {
		return err
	}
	if o.finalOverlap <= 0 || o.finalTail <= 0 {
		return errors.New("--final-overlap and --final-tail must be positive")
	}
	return nil
}

func runSync(cmd *cobra.Command, g *globalOptions, o *syncOptions) error {
	if err := o.validate(); err != nil {
		return err
	}
	e, err := g.setup(cmd, true)
	if err != nil {
		return err
	}
	m, _, stop, err := o.serveMetrics(e)
	if err != nil {
		return err
	}
	defer stop()
	syncer, err := newSyncer(cmd, e, o, m)
	if err != nil {
		return err
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
		FinalTail:    o.finalTail,
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

// serveMetrics serves the tool's metrics on --metrics-addr until stop is called. Without --metrics-addr it serves
// nothing, and the returned metrics are nil, which record nothing.
func (o *syncOptions) serveMetrics(e *env) (*migration.Metrics, *migration.Phase, func(), error) {
	if o.metricsAddr == "" {
		return nil, nil, func() {}, nil
	}
	set := metrics.NewSet()
	m := migration.NewMetrics(set)
	p := migration.NewPhase(set)
	stop, err := serveMetrics(o.metricsAddr, set, e.log)
	if err != nil {
		return nil, nil, nil, err
	}
	return m, p, stop, nil
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
