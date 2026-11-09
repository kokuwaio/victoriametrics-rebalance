// Package migration copies data from blue to green in resumable, time-ordered chunks.
package migration

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/state"
)

// Window is a half-open time range [Start, End).
type Window struct {
	Start, End time.Time
}

func (w Window) String() string {
	return w.Start.UTC().Format(time.RFC3339) + " – " + w.End.UTC().Format(time.RFC3339)
}

// CopyStats describes one finished copy.
type CopyStats struct {
	Bytes int64
}

// Copier copies all series in a window from blue to green, at most rateLimit bytes per second (0: no limit).
type Copier interface {
	Copy(ctx context.Context, w Window, rateLimit int64) (CopyStats, error)
}

// Pacer decides the rate limit for the next chunk, in bytes per second (0: no limit).
type Pacer interface {
	Next(ctx context.Context) int64
}

// FixedRate is a Pacer that always returns the same rate.
type FixedRate int64

// Next returns the fixed rate.
func (r FixedRate) Next(context.Context) int64 { return int64(r) }

// Syncer copies time ranges chunk by chunk and records every finished chunk, so an interrupted run resumes at the
// first unfinished one.
type Syncer struct {
	Copier Copier
	Store  state.Store
	// Step is the chunk size. Inner chunk boundaries are aligned to Step (see Split).
	Step time.Duration
	// Retries is how often a failed chunk is retried before Sync gives up.
	Retries int
	// Backoff is the wait before the first retry. It doubles with every further retry.
	Backoff time.Duration
	// Pacer sets each chunk's rate limit. Default: no limit.
	Pacer Pacer
	// Metrics records the progress. May be nil.
	Metrics *Metrics

	Now   func() time.Time
	Sleep func(ctx context.Context, d time.Duration) error
	Log   *slog.Logger
}

// Sync copies everything from where the last sync stopped up to until. On the first run (nothing synced yet) it
// starts at start and records it as the backfill start; a first run that stopped before finishing a chunk is
// resumed from that recorded start, so the backfill start always matches what was copied.
func (s *Syncer) Sync(ctx context.Context, start, until time.Time) error {
	if err := s.validate(); err != nil {
		return err
	}
	st, err := s.Store.Load(ctx)
	if err != nil {
		return fmt.Errorf("load state: %w", err)
	}
	switch {
	case !st.SyncedUntil.IsZero():
		start = st.SyncedUntil
	default:
		// Record the backfill start once; a first run that stopped before finishing a chunk resumes from it.
		updated, err := s.Store.Update(ctx, func(st *state.State) {
			if st.BackfillStart.IsZero() {
				st.BackfillStart = start
			}
		})
		if err != nil {
			return fmt.Errorf("save state: %w", err)
		}
		if !updated.BackfillStart.Equal(start) {
			s.log().Warn("resuming the first sync from the recorded backfill start, not the requested one",
				"recorded", updated.BackfillStart, "requested", start)
			start = updated.BackfillStart
		}
	}
	return s.copyRange(ctx, Window{Start: start, End: until})
}

func (s *Syncer) validate() error {
	if s.Step <= 0 {
		return fmt.Errorf("step must be positive, got %s", s.Step)
	}
	return nil
}

const (
	// FinalOverlap is how far before the last synced point the final sync starts, to pick up samples that arrived
	// late on blue. Overlaps are safe because green deduplicates samples with equal timestamps.
	FinalOverlap = time.Hour
	// FinalTail is how long after the cutover the final sync waits before it checks that blue is drained, so
	// writers that switched late have stopped.
	FinalTail = 15 * time.Minute
)

// Final runs the last sync after cutover. It waits until cutover + FinalTail, then calls drained, which returns
// once blue receives no more writes and when that was. Only then does it copy
// [SyncedUntil - FinalOverlap, max(cutover + FinalTail, drained)], so every sample blue received is copied. When a
// catch-up already ran past the cutover, the window starts at cutover - FinalOverlap instead, so the time around
// the cutover is copied and verified either way. A rerun for the same cutover keeps the start of the first run's
// window, so days that failed to verify are copied and verified again. If drained fails, Final copies nothing and
// returns its error. It records the cutover and the window in the state and returns the window.
func (s *Syncer) Final(ctx context.Context, cutover time.Time, drained func(context.Context) (time.Time, error)) (Window, error) {
	if err := s.validate(); err != nil {
		return Window{}, err
	}
	if cutover.After(s.Now()) {
		return Window{}, fmt.Errorf("cutover %s is in the future: run the final sync after switching the aliases",
			cutover.UTC().Format(time.RFC3339))
	}
	st, err := s.Store.Load(ctx)
	if err != nil {
		return Window{}, fmt.Errorf("load state: %w", err)
	}
	if st.SyncedUntil.IsZero() {
		return Window{}, fmt.Errorf("nothing synced yet: run sync (backfill and catch-up) before the final sync")
	}
	from := st.SyncedUntil
	if cutover.Before(from) {
		from = cutover
	}
	w := Window{Start: from.Add(-FinalOverlap), End: cutover.Add(FinalTail)}
	if st.CutoverAt.Equal(cutover) && !st.FinalStart.IsZero() && st.FinalStart.Before(w.Start) {
		w.Start = st.FinalStart
	}
	if wait := w.End.Sub(s.Now()); wait > 0 {
		s.log().Info("waiting for late writers to switch to green", "until", w.End, "wait", wait)
		if err := s.Sleep(ctx, wait); err != nil {
			return Window{}, err
		}
	}
	at, err := drained(ctx)
	if err != nil {
		return Window{}, err
	}
	if at.After(w.End) {
		w.End = at
	}
	if _, err := s.Store.Update(ctx, func(st *state.State) {
		st.CutoverAt = cutover
		st.FinalStart, st.FinalEnd = w.Start, w.End
	}); err != nil {
		return Window{}, fmt.Errorf("save state: %w", err)
	}
	if err := s.copyRange(ctx, w); err != nil {
		return Window{}, err
	}
	if _, err := s.Store.Update(ctx, func(st *state.State) { st.FinalSyncAt = s.Now() }); err != nil {
		return Window{}, fmt.Errorf("save state: %w", err)
	}
	return w, nil
}

func (s *Syncer) copyRange(ctx context.Context, r Window) error {
	for _, w := range Split(r, s.Step) {
		var rate int64
		if s.Pacer != nil {
			rate = s.Pacer.Next(ctx)
		}
		s.Metrics.rate(rate)
		began := s.Now()
		stats, err := s.copyWithRetries(ctx, w, rate)
		if err != nil {
			return fmt.Errorf("copy %s: %w", w, err)
		}
		c := state.Chunk{
			Start:      w.Start,
			End:        w.End,
			Bytes:      stats.Bytes,
			Seconds:    s.Now().Sub(began).Seconds(),
			FinishedAt: s.Now(),
		}
		if _, err := s.Store.Update(ctx, func(st *state.State) { st.AddChunk(c) }); err != nil {
			return fmt.Errorf("save state after %s: %w", w, err)
		}
		s.Metrics.chunkDone(stats, c.Seconds, float64(w.End.Unix()))
	}
	return nil
}

func (s *Syncer) copyWithRetries(ctx context.Context, w Window, rate int64) (CopyStats, error) {
	wait := s.Backoff
	for attempt := 0; ; attempt++ {
		stats, err := s.Copier.Copy(ctx, w, rate)
		if err == nil {
			return stats, nil
		}
		if ctx.Err() != nil {
			return CopyStats{}, ctx.Err()
		}
		if attempt >= s.Retries {
			return stats, err
		}
		s.Metrics.retry()
		s.log().Warn("chunk failed, retrying", "window", w.String(), "attempt", attempt+1, "wait", wait, "err", err)
		if err := s.Sleep(ctx, wait); err != nil {
			return CopyStats{}, err
		}
		wait *= 2
	}
}

func (s *Syncer) log() *slog.Logger { return orDiscard(s.Log) }

// Split cuts r into windows of at most step. Inner boundaries are aligned with time.Truncate, so a step of 24h
// cuts at UTC midnight and one of 1h at full hours.
func Split(r Window, step time.Duration) []Window {
	var out []Window
	for start := r.Start; start.Before(r.End); {
		end := start.Truncate(step).Add(step)
		if end.After(r.End) {
			end = r.End
		}
		out = append(out, Window{Start: start, End: end})
		start = end
	}
	return out
}
