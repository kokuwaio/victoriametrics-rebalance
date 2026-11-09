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

// Copier copies all series in a window from blue to green.
type Copier interface {
	Copy(ctx context.Context, w Window) (CopyStats, error)
}

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

	Now   func() time.Time
	Sleep func(ctx context.Context, d time.Duration) error
	Log   *slog.Logger
}

// Sync copies everything from where the last sync stopped up to until. On the first run (nothing synced yet) it
// starts at start.
func (s *Syncer) Sync(ctx context.Context, start, until time.Time) error {
	if s.Step <= 0 {
		return fmt.Errorf("step must be positive, got %s", s.Step)
	}
	st, err := s.Store.Load(ctx)
	if err != nil {
		return fmt.Errorf("load state: %w", err)
	}
	if !st.SyncedUntil.IsZero() {
		start = st.SyncedUntil
	} else if _, err := s.Store.Update(ctx, func(st *state.State) {
		if st.BackfillStart.IsZero() {
			st.BackfillStart = start
		}
	}); err != nil {
		return fmt.Errorf("save state: %w", err)
	}
	return s.copyRange(ctx, Window{Start: start, End: until})
}

const (
	// FinalOverlap is how far before the last synced point the final sync starts, to pick up samples that arrived
	// late on blue. Overlaps are safe because green deduplicates samples with equal timestamps.
	FinalOverlap = time.Hour
	// FinalTail is how far past the cutover the final sync reaches, so writers that switched late are included.
	FinalTail = 15 * time.Minute
)

// Final runs the last sync after cutover: [SyncedUntil - FinalOverlap, cutover + FinalTail]. It waits until the
// end of that window has passed, records the cutover and the window in the state and returns the window.
func (s *Syncer) Final(ctx context.Context, cutover time.Time) (Window, error) {
	if s.Step <= 0 {
		return Window{}, fmt.Errorf("step must be positive, got %s", s.Step)
	}
	st, err := s.Store.Load(ctx)
	if err != nil {
		return Window{}, fmt.Errorf("load state: %w", err)
	}
	if st.SyncedUntil.IsZero() {
		return Window{}, fmt.Errorf("nothing synced yet: run sync (backfill and catch-up) before the final sync")
	}
	w := Window{Start: st.SyncedUntil.Add(-FinalOverlap), End: cutover.Add(FinalTail)}
	if wait := w.End.Sub(s.Now()); wait > 0 {
		s.log().Info("waiting for the end of the final window", "until", w.End, "wait", wait)
		if err := s.Sleep(ctx, wait); err != nil {
			return Window{}, err
		}
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
		began := s.Now()
		stats, err := s.copyWithRetries(ctx, w)
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
	}
	return nil
}

func (s *Syncer) copyWithRetries(ctx context.Context, w Window) (CopyStats, error) {
	wait := s.Backoff
	for attempt := 0; ; attempt++ {
		stats, err := s.Copier.Copy(ctx, w)
		if err == nil || attempt >= s.Retries || ctx.Err() != nil {
			return stats, err
		}
		s.log().Warn("chunk failed, retrying", "window", w.String(), "attempt", attempt+1, "wait", wait, "err", err)
		if err := s.Sleep(ctx, wait); err != nil {
			return CopyStats{}, err
		}
		wait *= 2
	}
}

func (s *Syncer) log() *slog.Logger {
	if s.Log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return s.Log
}

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
