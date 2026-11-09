// Package cli wires the vmrebalance commands.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/spf13/cobra"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/check"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/cluster"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/state"
)

// errChecksFailed makes the command exit with status 1 after the report was printed.
var errChecksFailed = errors.New("checks failed")

type globalOptions struct {
	kubeconfig string
	context    string
	namespace  string
	from, to   string
	tenant     string
	access     string
	stateFile  string
	verbose    bool
}

// NewRoot returns the vmrebalance root command.
func NewRoot() *cobra.Command {
	g := &globalOptions{}
	root := &cobra.Command{
		Use:   "vmrebalance",
		Short: "Move a VictoriaMetrics cluster's data to a new cluster with more vmstorage nodes",
		Long: `vmrebalance copies the data of a VMCluster ("blue") into a new VMCluster ("green") with more vmstorage
nodes, through green's vminsert, so the data ends up spread evenly across all green nodes.

It never deletes data and never changes VMCluster, VMAgent or Service objects: you create green, enable dual
write and switch the aliases (usually in Git); the tool copies the data, checks every step and says what's due.`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	f := root.PersistentFlags()
	f.StringVar(&g.kubeconfig, "kubeconfig", "", "path to the kubeconfig file (default: $KUBECONFIG, ~/.kube/config, or in-cluster)")
	f.StringVar(&g.context, "context", "", "kubeconfig context to use")
	f.StringVarP(&g.namespace, "namespace", "n", "", "namespace of both VMClusters (default: the context's namespace)")
	f.StringVar(&g.from, "from", "", "name of the old VMCluster (blue)")
	f.StringVar(&g.to, "to", "", "name of the new VMCluster (green)")
	f.StringVar(&g.tenant, "tenant", "0", "tenant ID to copy and check")
	f.StringVar(&g.access, "access", "auto", "how to reach vmselect and pods: proxy (through the API server), direct (Service DNS and pod IPs), or auto (direct in a pod, proxy otherwise)")
	f.StringVar(&g.stateFile, "state-file", "", "keep the state in this local file instead of the ConfigMap vmrebalance.<from>.<to> (with .tenant-<tenant> for tenants other than 0)")
	f.BoolVarP(&g.verbose, "verbose", "v", false, "log debug output")
	_ = root.MarkPersistentFlagRequired("from")
	_ = root.MarkPersistentFlagRequired("to")

	root.AddCommand(newPreflight(g), newSync(g), newVerify(g), newStatus(g))
	return root
}

// Execute runs the root command and returns the process exit code.
func Execute(ctx context.Context) int {
	if err := NewRoot().ExecuteContext(ctx); err != nil {
		if !errors.Is(err, errChecksFailed) {
			fmt.Fprintln(os.Stderr, "error:", err)
		}
		return 1
	}
	return 0
}

// env is what every command works with.
type env struct {
	namespace   string
	kube        kubernetes.Interface
	store       state.Store
	log         *slog.Logger
	out         io.Writer
	access      cluster.Access
	tenant      string
	blue, green cluster.Client
}

// setup connects to Kubernetes and opens the state. With clusters, it also opens both VMClusters.
func (g *globalOptions) setup(cmd *cobra.Command, clusters bool) (*env, error) {
	level := slog.LevelInfo
	if g.verbose {
		level = slog.LevelDebug
	}
	e := &env{
		log:    slog.New(slog.NewTextHandler(cmd.ErrOrStderr(), &slog.HandlerOptions{Level: level})),
		out:    cmd.OutOrStdout(),
		tenant: g.tenant,
	}
	if g.from == g.to {
		return nil, fmt.Errorf("--from and --to must name different VMClusters")
	}
	if strings.Contains(g.from+g.to, ".") {
		return nil, fmt.Errorf("--from and --to must not contain dots: the operator names Services after the VMCluster")
	}

	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = g.kubeconfig
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{CurrentContext: g.context})
	cfg, err := cc.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}
	e.namespace = g.namespace
	if e.namespace == "" {
		if e.namespace, _, err = cc.Namespace(); err != nil {
			return nil, fmt.Errorf("namespace: %w", err)
		}
	}
	if e.kube, err = kubernetes.NewForConfig(cfg); err != nil {
		return nil, err
	}

	switch g.access {
	case "auto":
		e.access = cluster.Proxy
		if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
			e.access = cluster.Direct
		}
	case string(cluster.Proxy), string(cluster.Direct):
		e.access = cluster.Access(g.access)
	default:
		return nil, fmt.Errorf("--access must be auto, proxy or direct, not %q", g.access)
	}

	m := state.Migration{From: g.from, To: g.to, Tenant: g.tenant}
	if g.stateFile != "" {
		e.store = state.NewFile(g.stateFile, m)
	} else {
		e.store = state.NewConfigMap(e.kube, e.namespace, m)
	}

	if clusters {
		k := &cluster.Kube{Client: e.kube, Access: e.access, Tenant: g.tenant}
		if e.blue, err = k.Open(cmd.Context(), e.namespace, g.from); err != nil {
			return nil, err
		}
		if e.green, err = k.Open(cmd.Context(), e.namespace, g.to); err != nil {
			return nil, err
		}
		if err := state.BindClusters(cmd.Context(), e.store, e.blue.Info().UID, e.green.Info().UID); err != nil {
			return nil, err
		}
	}
	e.log.Debug("connected", "namespace", e.namespace, "access", e.access, "from", g.from, "to", g.to)
	return e, nil
}

// sleep waits for d or until ctx is done.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// parseRetention parses a VMCluster retentionPeriod the way VictoriaMetrics does (empty means one month).
func parseRetention(s string) (time.Duration, error) {
	if s == "" {
		s = "1"
	}
	var d flagutil.RetentionDuration
	if err := d.Set(s); err != nil {
		return 0, fmt.Errorf("retentionPeriod %q: %w", s, err)
	}
	return d.Duration(), nil
}

// parseTime parses the value of flag: "now" or an RFC 3339 timestamp.
func parseTime(flag, s string, now time.Time) (time.Time, error) {
	if s == "now" {
		return now, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s: want \"now\" or an RFC 3339 time like 2026-01-02T15:04:05Z: %w", flag, err)
	}
	return t, nil
}

func printReport(w io.Writer, results []check.Result) {
	for _, r := range results {
		fmt.Fprintf(w, "%-4s  %-18s  %s\n", r.Level, r.Name, r.Message)
	}
}
