// Package state records the progress of one migration, so that a run restarted after a crash or pod eviction
// continues where it stopped.
package state

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"
)

// MaxChunks caps the chunk history kept in the state. SyncedUntil is what resuming relies on; the history only
// feeds `status`, and an unbounded list would outgrow a ConfigMap on long retentions with small steps.
const MaxChunks = 500

// State is everything the tool remembers about one migration from one cluster to another.
type State struct {
	From string `json:"from"`
	To   string `json:"to"`
	// Tenant is the tenant the migration copies. Empty means tenant 0.
	Tenant string `json:"tenant,omitempty"`
	// FromUID and ToUID are the VMClusters' UIDs, recorded on first use, so a VMCluster that was deleted and
	// created again under the same name isn't taken for the one the progress belongs to.
	FromUID string `json:"fromUID,omitempty"`
	ToUID   string `json:"toUID,omitempty"`

	// DualWriteSeenAt is when preflight first saw green receive live writes (T0, or a little after it).
	DualWriteSeenAt time.Time `json:"dualWriteSeenAt,omitzero"`
	// BackfillStart is where the first sync started (now - retention at that time).
	BackfillStart time.Time `json:"backfillStart,omitzero"`
	// SyncedUntil is the end of the last finished chunk. The next sync continues from here.
	SyncedUntil time.Time `json:"syncedUntil,omitzero"`
	// Chunks is the history of finished chunks, oldest first, capped at MaxChunks.
	Chunks []Chunk `json:"chunks,omitempty"`

	// Verifications holds the latest verify result per day (key: YYYY-MM-DD, UTC).
	Verifications map[string]DayResult `json:"verifications,omitempty"`

	CutoverAt   time.Time `json:"cutoverAt,omitzero"`
	FinalSyncAt time.Time `json:"finalSyncAt,omitzero"`
	// FinalStart and FinalEnd are the time range the final sync copied.
	FinalStart time.Time `json:"finalStart,omitzero"`
	FinalEnd   time.Time `json:"finalEnd,omitzero"`
}

// Chunk is one finished copy of a time window.
type Chunk struct {
	Start      time.Time `json:"start"`
	End        time.Time `json:"end"`
	Bytes      int64     `json:"bytes,omitempty"`
	Seconds    float64   `json:"seconds"`
	FinishedAt time.Time `json:"finishedAt"`
}

// DayResult is the outcome of comparing one day between blue and green.
type DayResult struct {
	Pass        bool      `json:"pass"`
	BlueSeries  int64     `json:"blueSeries"`
	GreenSeries int64     `json:"greenSeries"`
	Problems    []string  `json:"problems,omitempty"`
	CheckedAt   time.Time `json:"checkedAt"`
	// Partial is true when only part of the day was checked (sample counts only, no series count).
	Partial bool `json:"partial,omitempty"`
}

// AddChunk appends a finished chunk, moves SyncedUntil forward and trims the history to MaxChunks.
func (s *State) AddChunk(c Chunk) {
	s.Chunks = append(s.Chunks, c)
	if len(s.Chunks) > MaxChunks {
		s.Chunks = s.Chunks[len(s.Chunks)-MaxChunks:]
	}
	if c.End.After(s.SyncedUntil) {
		s.SyncedUntil = c.End
	}
}

// SetVerification stores the result for the day that starts at day (UTC). A passing check of only part of the day
// doesn't replace a result for the whole day, which also compared the series count.
func (s *State) SetVerification(day time.Time, r DayResult) {
	if s.Verifications == nil {
		s.Verifications = map[string]DayResult{}
	}
	if old, ok := s.Verifications[DayKey(day)]; ok && !old.Partial && r.Partial && r.Pass {
		return
	}
	s.Verifications[DayKey(day)] = r
}

// DayKey formats the UTC day of t as YYYY-MM-DD.
func DayKey(t time.Time) string { return t.UTC().Format(time.DateOnly) }

// Memory is a Store that keeps the state in memory, for tests.
type Memory struct {
	mu sync.Mutex
	s  State
}

// Load returns a copy of the stored state.
func (m *Memory) Load(context.Context) (*State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.s.clone(), nil
}

// Update applies change to the stored state.
func (m *Memory) Update(_ context.Context, change func(*State)) (*State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	change(&m.s)
	return m.s.clone(), nil
}

func (s *State) clone() *State {
	c := *s
	c.Chunks = append([]Chunk(nil), s.Chunks...)
	c.Verifications = maps.Clone(s.Verifications)
	return &c
}

// BindClusters records the VMClusters' UIDs on first use and fails when either changed since: a VMCluster that was
// deleted and created again under the same name starts empty, so the saved progress doesn't apply to it.
func BindClusters(ctx context.Context, store Store, fromUID, toUID string) error {
	if fromUID == "" || toUID == "" {
		return fmt.Errorf("VMCluster UID unknown (from %q, to %q)", fromUID, toUID)
	}
	st, err := store.Load(ctx)
	if err != nil {
		return err
	}
	for _, c := range []struct{ name, saved, now string }{{st.From, st.FromUID, fromUID}, {st.To, st.ToUID, toUID}} {
		if c.saved != "" && c.saved != c.now {
			return fmt.Errorf("VMCluster %s was deleted and created again since the state was saved (UID %s, now %s): "+
				"its progress no longer applies; delete the state to start over", c.name, c.saved, c.now)
		}
	}
	if st.FromUID == fromUID && st.ToUID == toUID {
		return nil
	}
	_, err = store.Update(ctx, func(st *State) { st.FromUID, st.ToUID = fromUID, toUID })
	return err
}
