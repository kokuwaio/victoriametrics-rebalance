package cli

import (
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/check"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/state"
)

func newPreflight(g *globalOptions) *cobra.Command {
	o := check.PreflightOptions{Sleep: sleep}
	cmd := &cobra.Command{
		Use:   "preflight",
		Short: "Check that green is ready: versions, flags, capacity, dual write and pod labels",
		RunE: func(cmd *cobra.Command, _ []string) error {
			e, err := g.setup(cmd, true)
			if err != nil {
				return err
			}
			if o.RateWindow <= 0 {
				return errors.New("--rate-window must be positive")
			}
			r := check.Preflight(cmd.Context(), e.blue, e.green, o)
			printReport(e.out, r.Results)

			if r.DualWriteActive {
				if _, err := e.store.Update(cmd.Context(), func(st *state.State) {
					if st.DualWriteSeenAt.IsZero() {
						st.DualWriteSeenAt = time.Now().UTC()
					}
				}); err != nil {
					return err
				}
			}
			if r.Failed() {
				fmt.Fprintln(e.out, "\nFix the failed checks before running sync.")
				return errChecksFailed
			}
			fmt.Fprintln(e.out, "\nReady. Next: vmrebalance sync (backfill).")
			return nil
		},
	}
	cmd.Flags().Float64Var(&o.Headroom, "headroom", 0.2, "free space green needs on top of blue's used bytes (0.2 = 20%)")
	cmd.Flags().DurationVar(&o.RateWindow, "rate-window", 30*time.Second, "time between the two scrapes that measure ingestion rates")
	return cmd
}
