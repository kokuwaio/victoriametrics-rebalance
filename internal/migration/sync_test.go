package migration_test

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/migration"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/state"
)

func ts(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

// fakeCopier records every window it is asked to copy. failures[i] is the number of times the i-th distinct
// window fails before it succeeds.
type fakeCopier struct {
	calls    []migration.Window
	rates    []int64
	failures map[migration.Window]int
}

func (f *fakeCopier) Copy(_ context.Context, w migration.Window, rateLimit int64) (migration.CopyStats, error) {
	f.calls = append(f.calls, w)
	f.rates = append(f.rates, rateLimit)
	if f.failures[w] > 0 {
		f.failures[w]--
		return migration.CopyStats{}, errTransient
	}
	return migration.CopyStats{Bytes: 100}, nil
}

type transientError struct{}

func (transientError) Error() string { return "transient" }

var errTransient = transientError{}

func newSyncer(c migration.Copier, st state.Store, now time.Time) *migration.Syncer {
	return &migration.Syncer{
		Copier:  c,
		Store:   st,
		Step:    24 * time.Hour,
		Retries: 2,
		Backoff: time.Second,
		Now:     func() time.Time { return now },
		Sleep:   func(context.Context, time.Duration) error { return nil },
	}
}

func windows(pairs ...string) []migration.Window {
	var out []migration.Window
	for i := 0; i < len(pairs); i += 2 {
		out = append(out, migration.Window{Start: ts(pairs[i]), End: ts(pairs[i+1])})
	}
	return out
}

func assertWindows(t *testing.T, got, want []migration.Window) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d windows %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if !got[i].Start.Equal(want[i].Start) || !got[i].End.Equal(want[i].End) {
			t.Errorf("window %d: got %v, want %v", i, got[i], want[i])
		}
	}
}

func TestFirstSyncCopiesFromStartToUntilInDayAlignedChunks(t *testing.T) {
	st := &state.Memory{}
	c := &fakeCopier{}
	s := newSyncer(c, st, ts("2026-01-03T06:00:00Z"))

	if err := s.Sync(t.Context(), ts("2026-01-01T10:00:00Z"), ts("2026-01-03T05:00:00Z")); err != nil {
		t.Fatal(err)
	}

	assertWindows(t, c.calls, windows(
		"2026-01-01T10:00:00Z", "2026-01-02T00:00:00Z",
		"2026-01-02T00:00:00Z", "2026-01-03T00:00:00Z",
		"2026-01-03T00:00:00Z", "2026-01-03T05:00:00Z",
	))
	got, _ := st.Load(t.Context())
	if !got.BackfillStart.Equal(ts("2026-01-01T10:00:00Z")) {
		t.Errorf("BackfillStart = %v", got.BackfillStart)
	}
	if !got.SyncedUntil.Equal(ts("2026-01-03T05:00:00Z")) {
		t.Errorf("SyncedUntil = %v", got.SyncedUntil)
	}
	if len(got.Chunks) != 3 || got.Chunks[0].Bytes != 100 {
		t.Errorf("chunks = %+v", got.Chunks)
	}
}

func TestSyncResumesAfterLastFinishedChunkAndIgnoresStart(t *testing.T) {
	st := &state.Memory{}
	_, _ = st.Update(t.Context(), func(s *state.State) {
		*s = state.State{
			BackfillStart: ts("2026-01-01T10:00:00Z"),
			SyncedUntil:   ts("2026-01-02T00:00:00Z"),
		}
	})
	c := &fakeCopier{}
	s := newSyncer(c, st, ts("2026-01-03T06:00:00Z"))

	if err := s.Sync(t.Context(), ts("2025-12-01T00:00:00Z"), ts("2026-01-02T12:00:00Z")); err != nil {
		t.Fatal(err)
	}

	assertWindows(t, c.calls, windows("2026-01-02T00:00:00Z", "2026-01-02T12:00:00Z"))
	got, _ := st.Load(t.Context())
	if !got.BackfillStart.Equal(ts("2026-01-01T10:00:00Z")) {
		t.Errorf("BackfillStart changed to %v", got.BackfillStart)
	}
}

func TestSyncCrashMidwayKeepsFinishedChunksForTheNextRun(t *testing.T) {
	st := &state.Memory{}
	failing := migration.Window{Start: ts("2026-01-02T00:00:00Z"), End: ts("2026-01-03T00:00:00Z")}
	c := &fakeCopier{failures: map[migration.Window]int{failing: 99}}
	s := newSyncer(c, st, ts("2026-01-03T06:00:00Z"))

	if err := s.Sync(t.Context(), ts("2026-01-01T00:00:00Z"), ts("2026-01-03T00:00:00Z")); err == nil {
		t.Fatal("want error for a chunk that keeps failing")
	}

	got, _ := st.Load(t.Context())
	if !got.SyncedUntil.Equal(ts("2026-01-02T00:00:00Z")) {
		t.Errorf("SyncedUntil = %v, want the end of the first chunk", got.SyncedUntil)
	}
}

func TestSyncRetriesAFailedChunkWithDoublingBackoff(t *testing.T) {
	st := &state.Memory{}
	w := migration.Window{Start: ts("2026-01-01T00:00:00Z"), End: ts("2026-01-02T00:00:00Z")}
	c := &fakeCopier{failures: map[migration.Window]int{w: 2}}
	s := newSyncer(c, st, ts("2026-01-03T06:00:00Z"))
	var waits []time.Duration
	s.Sleep = func(_ context.Context, d time.Duration) error { waits = append(waits, d); return nil }

	if err := s.Sync(t.Context(), w.Start, w.End); err != nil {
		t.Fatal(err)
	}

	if len(c.calls) != 3 {
		t.Errorf("copy called %d times, want 3", len(c.calls))
	}
	if len(waits) != 2 || waits[0] != time.Second || waits[1] != 2*time.Second {
		t.Errorf("waits = %v, want [1s 2s]", waits)
	}
}

func TestSyncGivesUpAfterRetriesAreExhausted(t *testing.T) {
	w := migration.Window{Start: ts("2026-01-01T00:00:00Z"), End: ts("2026-01-02T00:00:00Z")}
	c := &fakeCopier{failures: map[migration.Window]int{w: 3}}
	s := newSyncer(c, &state.Memory{}, ts("2026-01-03T06:00:00Z"))

	if err := s.Sync(t.Context(), w.Start, w.End); err == nil {
		t.Fatal("want error")
	}
	if len(c.calls) != 3 {
		t.Errorf("copy called %d times, want 1 + 2 retries", len(c.calls))
	}
}

func TestFinalCopiesFromOneHourBeforeLastSyncUntilBlueDrained(t *testing.T) {
	st := &state.Memory{}
	_, _ = st.Update(t.Context(), func(s *state.State) { *s = state.State{SyncedUntil: ts("2026-01-05T10:20:00Z")} })
	c := &fakeCopier{}
	s := newSyncer(c, st, ts("2026-01-05T11:00:00Z"))

	w, err := s.Final(t.Context(), ts("2026-01-05T10:30:00Z"), drainedAt(ts("2026-01-05T11:00:00Z")))
	if err != nil {
		t.Fatal(err)
	}

	want := windows("2026-01-05T09:20:00Z", "2026-01-05T11:00:00Z")
	assertWindows(t, []migration.Window{w}, want)
	assertWindows(t, c.calls, want)
	got, _ := st.Load(t.Context())
	if !got.CutoverAt.Equal(ts("2026-01-05T10:30:00Z")) || !got.FinalStart.Equal(want[0].Start) ||
		!got.FinalEnd.Equal(want[0].End) || got.FinalSyncAt.IsZero() {
		t.Errorf("final not recorded: %+v", got)
	}
	if !got.SyncedUntil.Equal(want[0].End) {
		t.Errorf("SyncedUntil = %v", got.SyncedUntil)
	}
}

func TestFinalWaitsUntilFifteenMinutesAfterCutover(t *testing.T) {
	st := &state.Memory{}
	_, _ = st.Update(t.Context(), func(s *state.State) { *s = state.State{SyncedUntil: ts("2026-01-05T10:20:00Z")} })
	s := newSyncer(&fakeCopier{}, st, ts("2026-01-05T10:35:00Z"))
	var waits []time.Duration
	s.Sleep = func(_ context.Context, d time.Duration) error { waits = append(waits, d); return nil }

	if _, err := s.Final(t.Context(), ts("2026-01-05T10:30:00Z"), drainedAt(ts("2026-01-05T10:45:00Z"))); err != nil {
		t.Fatal(err)
	}

	if len(waits) != 1 || waits[0] != 10*time.Minute {
		t.Errorf("waits = %v, want [10m]", waits)
	}
}

func TestFinalRefusesToRunBeforeAnySync(t *testing.T) {
	s := newSyncer(&fakeCopier{}, &state.Memory{}, ts("2026-01-05T11:00:00Z"))

	if _, err := s.Final(t.Context(), ts("2026-01-05T10:30:00Z"), drainedAt(ts("2026-01-05T10:45:00Z"))); err == nil {
		t.Fatal("want error when nothing was synced yet")
	}
}

func TestSyncRejectsANonPositiveStep(t *testing.T) {
	s := newSyncer(&fakeCopier{}, &state.Memory{}, ts("2026-01-03T06:00:00Z"))
	s.Step = 0

	if err := s.Sync(t.Context(), ts("2026-01-01T00:00:00Z"), ts("2026-01-02T00:00:00Z")); err == nil {
		t.Fatal("want an error for step 0 instead of an endless loop")
	}
}

// pacer hands out the given rates in order.
type pacer struct{ rates []int64 }

func (p *pacer) Next(context.Context) int64 {
	r := p.rates[0]
	p.rates = p.rates[1:]
	return r
}

func TestSyncCopiesEachChunkAtTheRateThePacerGives(t *testing.T) {
	c := &fakeCopier{}
	s := newSyncer(c, &state.Memory{}, ts("2026-01-03T06:00:00Z"))
	s.Pacer = &pacer{rates: []int64{100, 50, 75}}

	if err := s.Sync(t.Context(), ts("2026-01-01T00:00:00Z"), ts("2026-01-04T00:00:00Z")); err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(c.rates, []int64{100, 50, 75}) {
		t.Errorf("rates = %v, want [100 50 75]", c.rates)
	}
}

func TestSyncWithoutPacerCopiesUnlimited(t *testing.T) {
	c := &fakeCopier{}
	s := newSyncer(c, &state.Memory{}, ts("2026-01-03T06:00:00Z"))

	if err := s.Sync(t.Context(), ts("2026-01-01T00:00:00Z"), ts("2026-01-02T00:00:00Z")); err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(c.rates, []int64{0}) {
		t.Errorf("rates = %v, want [0]", c.rates)
	}
}

func TestSyncMetricsShowTheRun(t *testing.T) {
	retried := migration.Window{Start: ts("2026-01-02T00:00:00Z"), End: ts("2026-01-03T00:00:00Z")}
	c := &fakeCopier{failures: map[migration.Window]int{retried: 1}}
	s := newSyncer(c, &state.Memory{}, ts("2026-01-03T06:00:00Z"))
	set := metrics.NewSet()
	s.Metrics = migration.NewMetrics(set)
	s.Pacer = &pacer{rates: []int64{100, 100, 75}}

	if err := s.Sync(t.Context(), ts("2026-01-01T00:00:00Z"), ts("2026-01-04T00:00:00Z")); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	set.WritePrometheus(&out)
	for _, line := range []string{
		"vmrebalance_chunks_total 3",
		"vmrebalance_copied_bytes_total 300",
		"vmrebalance_chunk_retries_total 1",
		"vmrebalance_synced_until_timestamp_seconds 1767484800",
		"vmrebalance_rate_limit_bytes_per_second 75",
	} {
		if !strings.Contains(out.String(), line+"\n") {
			t.Errorf("metrics lack %q:\n%s", line, out.String())
		}
	}
}

func TestSyncAfterACrashBeforeTheFirstChunkStartsAtTheRecordedBackfillStart(t *testing.T) {
	st := &state.Memory{}
	_, _ = st.Update(t.Context(), func(s *state.State) { s.BackfillStart = ts("2026-01-01T10:00:00Z") })
	c := &fakeCopier{}
	s := newSyncer(c, st, ts("2026-01-03T06:00:00Z"))

	// now - retention has moved on since the first run.
	if err := s.Sync(t.Context(), ts("2026-01-01T16:00:00Z"), ts("2026-01-02T00:00:00Z")); err != nil {
		t.Fatal(err)
	}

	assertWindows(t, c.calls, windows("2026-01-01T10:00:00Z", "2026-01-02T00:00:00Z"))
}

func TestSyncReturnsTheCancellationNotVMCtlsError(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	w := migration.Window{Start: ts("2026-01-01T00:00:00Z"), End: ts("2026-01-02T00:00:00Z")}
	c := &cancellingCopier{cancel: cancel}
	s := newSyncer(c, &state.Memory{}, ts("2026-01-03T06:00:00Z"))

	err := s.Sync(ctx, w.Start, w.End)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

// cancellingCopier cancels the context and fails like a killed vmctl.
type cancellingCopier struct{ cancel context.CancelFunc }

func (c *cancellingCopier) Copy(context.Context, migration.Window, int64) (migration.CopyStats, error) {
	c.cancel()
	return migration.CopyStats{}, errors.New("vmctl: signal: killed")
}

func TestFinalRejectsACutoverInTheFuture(t *testing.T) {
	st := &state.Memory{}
	_, _ = st.Update(t.Context(), func(s *state.State) { s.SyncedUntil = ts("2026-01-05T10:20:00Z") })
	c := &fakeCopier{}
	s := newSyncer(c, st, ts("2026-01-05T11:00:00Z"))

	if _, err := s.Final(t.Context(), ts("2026-01-15T10:30:00Z"), drainedAt(ts("2026-01-05T11:00:00Z"))); err == nil {
		t.Fatal("want an error for a cutover in the future instead of sleeping for days")
	}
	if len(c.calls) != 0 {
		t.Errorf("copied %v", c.calls)
	}
}

func TestFinalAfterACatchUpPastTheCutoverStillCopiesAroundTheCutover(t *testing.T) {
	st := &state.Memory{}
	// A catch-up ran long after the cutover.
	_, _ = st.Update(t.Context(), func(s *state.State) { s.SyncedUntil = ts("2026-01-05T14:00:00Z") })
	c := &fakeCopier{}
	s := newSyncer(c, st, ts("2026-01-05T15:00:00Z"))

	w, err := s.Final(t.Context(), ts("2026-01-05T10:30:00Z"), drainedAt(ts("2026-01-05T10:45:00Z")))
	if err != nil {
		t.Fatal(err)
	}

	want := windows("2026-01-05T09:30:00Z", "2026-01-05T10:45:00Z")
	assertWindows(t, []migration.Window{w}, want)
	assertWindows(t, c.calls, want)
	got, _ := st.Load(t.Context())
	if !got.SyncedUntil.Equal(ts("2026-01-05T14:00:00Z")) {
		t.Errorf("SyncedUntil moved back to %v", got.SyncedUntil)
	}
}

// drainedAt is a drain check that passes at t.
func drainedAt(t time.Time) func(context.Context) (time.Time, error) {
	return func(context.Context) (time.Time, error) { return t, nil }
}

func TestFinalChecksTheDrainAfterWaitingAndBeforeCopying(t *testing.T) {
	st := &state.Memory{}
	_, _ = st.Update(t.Context(), func(s *state.State) { s.SyncedUntil = ts("2026-01-05T10:20:00Z") })
	var events []string
	c := &fakeCopier{}
	s := newSyncer(copierFunc(func(ctx context.Context, w migration.Window, r int64) (migration.CopyStats, error) {
		events = append(events, "copy")
		return c.Copy(ctx, w, r)
	}), st, ts("2026-01-05T10:35:00Z"))
	s.Sleep = func(_ context.Context, d time.Duration) error {
		events = append(events, "wait "+d.String())
		return nil
	}

	w, err := s.Final(t.Context(), ts("2026-01-05T10:30:00Z"), func(context.Context) (time.Time, error) {
		events = append(events, "drain")
		return ts("2026-01-05T10:52:00Z"), nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(events, []string{"wait 10m0s", "drain", "copy"}) {
		t.Errorf("events = %v, want the wait, then the drain check, then the copy", events)
	}
	// Writers that stopped late are covered up to the moment blue was drained.
	assertWindows(t, []migration.Window{w}, windows("2026-01-05T09:20:00Z", "2026-01-05T10:52:00Z"))
}

func TestFinalCopiesNothingWhileBlueStillReceivesWrites(t *testing.T) {
	st := &state.Memory{}
	_, _ = st.Update(t.Context(), func(s *state.State) { s.SyncedUntil = ts("2026-01-05T10:20:00Z") })
	c := &fakeCopier{}
	s := newSyncer(c, st, ts("2026-01-05T11:00:00Z"))
	notDrained := errors.New("blue still receives writes")

	_, err := s.Final(t.Context(), ts("2026-01-05T10:30:00Z"), func(context.Context) (time.Time, error) {
		return time.Time{}, notDrained
	})

	if !errors.Is(err, notDrained) {
		t.Errorf("err = %v, want the drain error", err)
	}
	if got, _ := st.Load(t.Context()); len(c.calls) != 0 || !got.FinalStart.IsZero() {
		t.Errorf("copied %v and recorded %+v before blue drained", c.calls, got)
	}
}

func TestFinalRetryWithTheSameCutoverKeepsTheFirstWindowStart(t *testing.T) {
	st := &state.Memory{}
	_, _ = st.Update(t.Context(), func(s *state.State) { s.SyncedUntil = ts("2026-01-03T00:00:00Z") })
	cutover := ts("2026-01-05T10:30:00Z")
	s := newSyncer(&fakeCopier{}, st, ts("2026-01-05T11:00:00Z"))
	if _, err := s.Final(t.Context(), cutover, drainedAt(ts("2026-01-05T11:00:00Z"))); err != nil {
		t.Fatal(err)
	}

	// Verify failed on an earlier day; the rerun must cover that day again although SyncedUntil moved on.
	w, err := s.Final(t.Context(), cutover, drainedAt(ts("2026-01-05T11:30:00Z")))
	if err != nil {
		t.Fatal(err)
	}

	assertWindows(t, []migration.Window{w}, windows("2026-01-02T23:00:00Z", "2026-01-05T11:30:00Z"))
}

type copierFunc func(context.Context, migration.Window, int64) (migration.CopyStats, error)

func (f copierFunc) Copy(ctx context.Context, w migration.Window, r int64) (migration.CopyStats, error) {
	return f(ctx, w, r)
}
