package cli

import (
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/spf13/cobra"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/check"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/state"
)

func newStatus(g *globalOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show the progress of the migration from the state",
		RunE: func(cmd *cobra.Command, _ []string) error {
			e, err := g.setup(cmd, false)
			if err != nil {
				return err
			}
			st, err := e.store.Load(cmd.Context())
			if err != nil {
				return err
			}
			printStatus(e.out, st, time.Now())
			return nil
		},
	}
}

func printStatus(w io.Writer, st *state.State, now time.Time) {
	row := func(k, format string, args ...any) { fmt.Fprintf(w, "%-17s %s\n", k+":", fmt.Sprintf(format, args...)) }
	at := func(t time.Time) string {
		if t.IsZero() {
			return "-"
		}
		return t.UTC().Format(time.RFC3339)
	}

	row("Migration", "%s → %s", st.From, st.To)
	row("Dual write seen", "%s", at(st.DualWriteSeenAt))
	row("Backfill start", "%s", at(st.BackfillStart))
	if st.SyncedUntil.IsZero() {
		row("Synced until", "- (run sync to start the backfill)")
	} else {
		row("Synced until", "%s (%s behind now)", at(st.SyncedUntil), now.Sub(st.SyncedUntil).Round(time.Second))
	}

	if n := len(st.Chunks); n > 0 {
		var bytes int64
		var secs, covered float64
		for _, c := range st.Chunks {
			bytes += c.Bytes
			secs += c.Seconds
			covered += c.End.Sub(c.Start).Seconds()
		}
		row("Chunks", "%d recorded, %s copied in %s", n, formatBytes(bytes), durationOf(secs))
		if secs > 0 && bytes > 0 {
			row("Throughput", "%s/s", formatBytes(int64(float64(bytes)/secs)))
		}
		// Each second of copy work has covered covered/secs seconds of data so far.
		if secs > 0 && st.FinalSyncAt.IsZero() {
			behind := now.Sub(st.SyncedUntil).Seconds()
			row("Catch-up ETA", "~%s at the recorded pace", durationOf(behind*secs/covered))
		}
	}

	if len(st.Verifications) > 0 {
		var pass int
		var failed []string
		for d, r := range st.Verifications {
			if r.Pass {
				pass++
			} else {
				failed = append(failed, d)
			}
		}
		slices.Sort(failed)
		row("Verified days", "%d pass, %d fail", pass, len(failed))
		for _, d := range failed {
			fmt.Fprintf(w, "  %s: %v\n", d, st.Verifications[d].Problems)
		}
	}
	row("Cutover", "%s", at(st.CutoverAt))
	if st.FinalSyncAt.IsZero() {
		row("Final sync", "-")
	} else {
		row("Final sync", "%s (copied %s – %s)", at(st.FinalSyncAt), at(st.FinalStart), at(st.FinalEnd))
	}
}

// durationOf converts seconds into a duration rounded to 100ms.
func durationOf(seconds float64) time.Duration {
	return time.Duration(seconds * float64(time.Second)).Round(100 * time.Millisecond)
}

func formatBytes(n int64) string { return check.FormatBytes(float64(n)) }
