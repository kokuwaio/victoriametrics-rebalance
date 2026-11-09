package state_test

import (
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/state"
)

var synced = time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)

var bg = state.Migration{From: "blue", To: "green", Tenant: "0"}

func sample() *state.State {
	s := &state.State{From: "blue", To: "green", BackfillStart: synced.Add(-24 * time.Hour)}
	s.AddChunk(state.Chunk{Start: synced.Add(-24 * time.Hour), End: synced, Bytes: 42, Seconds: 1.5, FinishedAt: synced})
	s.SetVerification(synced, state.DayResult{Pass: true, BlueSeries: 10, GreenSeries: 10, CheckedAt: synced})
	return s
}

// apiServer returns a fake clientset that, like a real API server, sets a new resourceVersion on every write and
// rejects an update whose resourceVersion is stale. client-go's fake tracker does neither.
func apiServer() *fake.Clientset { return apiServerWithInterleave(nil) }

// apiServerWithInterleave is apiServer, but right before the first update it lets another writer change the stored
// ConfigMap, so that update carries a stale resourceVersion.
func apiServerWithInterleave(other func(cm *corev1.ConfigMap)) *fake.Clientset {
	cs := fake.NewClientset()
	gvr := corev1.SchemeGroupVersion.WithResource("configmaps")
	rv := 0
	write := func(a k8stesting.Action) (bool, runtime.Object, error) {
		cm := a.(interface{ GetObject() runtime.Object }).GetObject().(*corev1.ConfigMap).DeepCopy()
		if a.GetVerb() == "update" {
			obj, err := cs.Tracker().Get(gvr, cm.Namespace, cm.Name)
			if err != nil {
				return true, nil, err
			}
			cur := obj.(*corev1.ConfigMap)
			if other != nil {
				cur = cur.DeepCopy()
				other(cur)
				other = nil
				rv++
				cur.ResourceVersion = strconv.Itoa(rv)
				if err := cs.Tracker().Update(gvr, cur, cur.Namespace); err != nil {
					return true, nil, err
				}
			}
			if cur.ResourceVersion != cm.ResourceVersion {
				return true, nil, apierrors.NewConflict(gvr.GroupResource(), cm.Name, nil)
			}
		}
		rv++
		cm.ResourceVersion = strconv.Itoa(rv)
		if a.GetVerb() == "create" {
			return true, cm, cs.Tracker().Create(gvr, cm, cm.Namespace)
		}
		return true, cm, cs.Tracker().Update(gvr, cm, cm.Namespace)
	}
	cs.PrependReactor("create", "configmaps", write)
	cs.PrependReactor("update", "configmaps", write)
	return cs
}

func assertRoundTrip(t *testing.T, got *state.State) {
	t.Helper()
	if got.From != "blue" || got.To != "green" || !got.SyncedUntil.Equal(synced) || len(got.Chunks) != 1 ||
		got.Chunks[0].Bytes != 42 || !got.Verifications["2026-01-02"].Pass {
		t.Errorf("state did not survive the round trip: %+v", got)
	}
}

func TestConfigMapStoreLoadsEmptyStateBeforeTheFirstSave(t *testing.T) {
	s := state.NewConfigMap(apiServer(), "monitoring", bg)

	got, err := s.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if got.From != "blue" || got.To != "green" || !got.SyncedUntil.IsZero() {
		t.Errorf("got %+v, want an empty state for blue → green", got)
	}
}

func TestConfigMapStoreRoundTrip(t *testing.T) {
	cs := apiServer()
	s := state.NewConfigMap(cs, "monitoring", bg)
	if _, err := s.Load(t.Context()); err != nil {
		t.Fatal(err)
	}
	replace := func(st *state.State) { *st = *sample() }
	if _, err := s.Update(t.Context(), replace); err != nil {
		t.Fatal(err)
	}
	// The second update changes the existing ConfigMap.
	if _, err := s.Update(t.Context(), replace); err != nil {
		t.Fatal(err)
	}

	got, err := state.NewConfigMap(cs, "monitoring", bg).Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	assertRoundTrip(t, got)
	cm, err := cs.CoreV1().ConfigMaps("monitoring").Get(t.Context(), "vmrebalance.blue.green", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("ConfigMap vmrebalance.blue.green: %v", err)
	}
	if cm.Labels["app.kubernetes.io/managed-by"] != "vmrebalance" {
		t.Errorf("labels = %v", cm.Labels)
	}
}

func TestConfigMapStoreUpdateKeepsAConcurrentWritersChanges(t *testing.T) {
	// Another run (say, verify during a sync Job) records a verification between our read and our write.
	cs := apiServerWithInterleave(func(cm *corev1.ConfigMap) {
		cm.Data["state.json"] = `{"from":"blue","to":"green","verifications":{"2026-01-01":{"pass":true,"blueSeries":1,"greenSeries":1,"checkedAt":"2026-01-02T00:00:00Z"}}}`
	})
	s := state.NewConfigMap(cs, "monitoring", bg)
	if _, err := s.Update(t.Context(), func(*state.State) {}); err != nil {
		t.Fatal(err)
	}

	got, err := s.Update(t.Context(), func(st *state.State) {
		st.AddChunk(state.Chunk{Start: synced.Add(-time.Hour), End: synced})
	})
	if err != nil {
		t.Fatal(err)
	}

	if !got.SyncedUntil.Equal(synced) || !got.Verifications["2026-01-01"].Pass {
		t.Errorf("want both the chunk and the other writer's verification, got %+v", got)
	}
	stored, _ := state.NewConfigMap(cs, "monitoring", bg).Load(t.Context())
	if !stored.SyncedUntil.Equal(synced) || !stored.Verifications["2026-01-01"].Pass {
		t.Errorf("stored state lost a change: %+v", stored)
	}
}

func TestFileStoreRoundTripAndEmptyStateForAMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := state.NewFile(path, bg)

	empty, err := s.Load(t.Context())
	if err != nil || empty.From != "blue" || empty.To != "green" || !empty.SyncedUntil.IsZero() {
		t.Fatalf("Load of a missing file = %+v, %v", empty, err)
	}
	if _, err := s.Update(t.Context(), func(st *state.State) { *st = *sample() }); err != nil {
		t.Fatal(err)
	}

	got, err := state.NewFile(path, bg).Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	assertRoundTrip(t, got)
}

func TestEachTenantHasItsOwnState(t *testing.T) {
	cs := apiServer()
	if _, err := state.NewConfigMap(cs, "monitoring", bg).Update(t.Context(), func(st *state.State) { st.SyncedUntil = synced }); err != nil {
		t.Fatal(err)
	}

	other := state.Migration{From: "blue", To: "green", Tenant: "1:2"}
	got, err := state.NewConfigMap(cs, "monitoring", other).Load(t.Context())
	if err != nil || !got.SyncedUntil.IsZero() {
		t.Errorf("tenant 1:2 loaded %+v, %v; want an empty state, not tenant 0's progress", got, err)
	}
	if name := state.Name(other); name != "vmrebalance.blue.green.tenant-1-2" {
		t.Errorf("name = %s", name)
	}
	for tenant, want := range map[string]string{"0:0": "vmrebalance.blue.green", "": "vmrebalance.blue.green", "1:0": "vmrebalance.blue.green.tenant-1"} {
		if name := state.Name(state.Migration{From: "blue", To: "green", Tenant: tenant}); name != want {
			t.Errorf("tenant %q: name = %s, want %s (the same tenant as its short form)", tenant, name, want)
		}
	}

	path := filepath.Join(t.TempDir(), "state.json")
	if _, err := state.NewFile(path, bg).Update(t.Context(), func(st *state.State) { st.SyncedUntil = synced }); err != nil {
		t.Fatal(err)
	}
	if _, err := state.NewFile(path, other).Load(t.Context()); err == nil || !strings.Contains(err.Error(), "tenant 0") {
		t.Errorf("loading tenant 0's state file as tenant 1:2: err = %v, want a mismatch error", err)
	}
}

func TestAPassReplacesOnlyResultsItCovers(t *testing.T) {
	day := synced
	at := func(h int) time.Time { return day.Add(time.Duration(h) * time.Hour) }
	check := func(pass bool, from, until int) state.DayResult {
		return state.DayResult{Pass: pass, From: at(from), Until: at(until), Partial: from != 0 || until != 24}
	}
	stored := func(s *state.State) state.DayResult { return s.Verifications[state.DayKey(day)] }

	s := &state.State{}
	s.SetVerification(day, check(false, 0, 24))
	s.SetVerification(day, check(true, 23, 24))
	if stored(s).Pass {
		t.Errorf("a pass for 23:00–24:00 replaced the failed whole-day check: %+v", stored(s))
	}

	s = &state.State{}
	s.SetVerification(day, check(false, 0, 12))
	s.SetVerification(day, check(true, 11, 13))
	if stored(s).Pass {
		t.Errorf("a pass for 11:00–13:00 replaced the failed 00:00–12:00 check: %+v", stored(s))
	}
	s.SetVerification(day, check(true, 0, 13))
	if !stored(s).Pass {
		t.Errorf("a pass for 00:00–13:00 covers the failed range and must replace it: %+v", stored(s))
	}

	s.SetVerification(day, check(false, 20, 22))
	if stored(s).Pass {
		t.Errorf("a failure must always be recorded: %+v", stored(s))
	}
	s.SetVerification(day, check(true, 0, 24))
	if !stored(s).Pass {
		t.Errorf("a whole-day pass must replace any result: %+v", stored(s))
	}
}

func TestBindClustersRejectsARecreatedVMCluster(t *testing.T) {
	st := &state.Memory{}
	_, _ = st.Update(t.Context(), func(s *state.State) { s.From, s.To, s.SyncedUntil = "blue", "green", synced })

	if err := state.BindClusters(t.Context(), st, "uid-blue", "uid-green"); err != nil {
		t.Fatalf("first use: %v", err)
	}
	if err := state.BindClusters(t.Context(), st, "uid-blue", "uid-green"); err != nil {
		t.Errorf("same clusters: %v", err)
	}

	err := state.BindClusters(t.Context(), st, "uid-blue", "uid-green-2")
	if err == nil || !strings.Contains(err.Error(), "green was deleted and created again") {
		t.Errorf("err = %v, want a refusal to reuse green's old progress", err)
	}
}

func TestStateNamesDontCollide(t *testing.T) {
	a := state.Name(state.Migration{From: "blue", To: "green-1"})
	b := state.Name(state.Migration{From: "blue", To: "green", Tenant: "1"})
	c := state.Name(state.Migration{From: "blue-green", To: "x"})
	d := state.Name(state.Migration{From: "blue", To: "green-x"})
	if a == b || c == d {
		t.Errorf("names collide: %s / %s, %s / %s", a, b, c, d)
	}
}

func TestAFailureKeepsTheRangeOfAnEarlierFailure(t *testing.T) {
	day := synced
	at := func(h int) time.Time { return day.Add(time.Duration(h) * time.Hour) }
	s := &state.State{}
	s.SetVerification(day, state.DayResult{Pass: false, From: at(0), Until: at(24), Problems: []string{"samples of up: blue 10, green 9"}})
	s.SetVerification(day, state.DayResult{Pass: false, From: at(20), Until: at(22), Problems: []string{"blue: timeout"}})
	if got := s.Verifications[state.DayKey(day)].Problems; !slices.Contains(got, "samples of up: blue 10, green 9") {
		t.Errorf("problems = %q, want the earlier one kept", got)
	}

	s.SetVerification(day, state.DayResult{Pass: true, From: at(20), Until: at(22)})

	if got := s.Verifications[state.DayKey(day)]; got.Pass {
		t.Errorf("a pass for 20:00–22:00 hid the earlier whole-day failure: %+v", got)
	}
}

func TestAResultWithoutARangeCountsAsTheWholeDay(t *testing.T) {
	s := &state.State{Verifications: map[string]state.DayResult{state.DayKey(synced): {Pass: false}}}

	s.SetVerification(synced, state.DayResult{Pass: true, From: synced, Until: synced.Add(24 * time.Hour)})

	if got := s.Verifications[state.DayKey(synced)]; !got.Pass {
		t.Errorf("a whole-day pass must replace a failure saved without a range: %+v", got)
	}
}
