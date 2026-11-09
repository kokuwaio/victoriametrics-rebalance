package copier_test

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/copier"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/migration"
)

type fakeRunner struct {
	name   string
	args   []string
	output string
	err    error
}

func (f *fakeRunner) run(_ context.Context, name string, args []string, out io.Writer) error {
	f.name, f.args = name, args
	_, _ = io.WriteString(out, f.output)
	return f.err
}

var window = migration.Window{
	Start: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
	End:   time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC),
}

func TestCopyRunsVMCtlNativeModeForTheWindow(t *testing.T) {
	r := &fakeRunner{}
	v := &copier.VMCtl{
		SrcAddr:     "http://vmselect-blue.monitoring.svc:8481/select/0/prometheus",
		DstAddr:     "http://vminsert-green.monitoring.svc:8480/insert/0/prometheus",
		Concurrency: 2,
		Run:         r.run,
	}

	if _, err := v.Copy(t.Context(), window, 50<<20); err != nil {
		t.Fatal(err)
	}

	want := []string{
		"vm-native", "-s", "--disable-progress-bar",
		"--vm-native-src-addr=http://vmselect-blue.monitoring.svc:8481/select/0/prometheus",
		"--vm-native-dst-addr=http://vminsert-green.monitoring.svc:8480/insert/0/prometheus",
		`--vm-native-filter-match={__name__!=""}`,
		"--vm-native-filter-time-start=2026-01-02T00:00:00Z",
		"--vm-native-filter-time-end=2026-01-03T00:00:00Z",
		"--vm-native-disable-per-metric-migration",
		"--vm-native-step-interval=hour",
		"--vm-concurrency=2",
		"--vm-rate-limit=26214400", // 50 MiB/s split across 2 workers
	}
	if r.name != "vmctl" || !slices.Equal(r.args, want) {
		t.Errorf("ran %s %q\nwant vmctl %q", r.name, r.args, want)
	}
}

func TestCopyOmitsRateLimitWhenUnlimitedAndAppendsExtraArgs(t *testing.T) {
	r := &fakeRunner{}
	v := &copier.VMCtl{Binary: "/bin/vmctl", SrcAddr: "s", DstAddr: "d", ExtraArgs: []string{"--vm-native-backoff-retries=5"}, Run: r.run}

	if _, err := v.Copy(t.Context(), window, 0); err != nil {
		t.Fatal(err)
	}

	if r.name != "/bin/vmctl" {
		t.Errorf("binary = %s", r.name)
	}
	if slices.ContainsFunc(r.args, func(a string) bool { return strings.HasPrefix(a, "--vm-rate-limit") }) {
		t.Errorf("rate limit set although unset: %q", r.args)
	}
	if r.args[len(r.args)-1] != "--vm-native-backoff-retries=5" {
		t.Errorf("extra args missing: %q", r.args)
	}
}

func TestCopyReportsTheBytesVMCtlTransferred(t *testing.T) {
	r := &fakeRunner{output: "2026/01/02 Import finished!\nVictoriaMetrics importer stats:\n" +
		"  time spent while importing: 1m0s;\n  total bytes: 1.5 GB;\n  bytes/s: 25.0 MB;\n  requests: 10;\n"}
	v := &copier.VMCtl{SrcAddr: "s", DstAddr: "d", Run: r.run}

	stats, err := v.Copy(t.Context(), window, 0)
	if err != nil {
		t.Fatal(err)
	}

	if stats.Bytes != 1_500_000_000 {
		t.Errorf("bytes = %d, want 1.5e9", stats.Bytes)
	}
}

func TestCopyFailureIncludesVMCtlOutput(t *testing.T) {
	r := &fakeRunner{output: "error: cannot connect to vmselect\n", err: errors.New("exit status 1")}
	v := &copier.VMCtl{SrcAddr: "s", DstAddr: "d", Run: r.run}

	_, err := v.Copy(t.Context(), window, 0)

	if err == nil || !strings.Contains(err.Error(), "cannot connect to vmselect") {
		t.Errorf("err = %v, want it to include vmctl's output", err)
	}
}

func TestParseRate(t *testing.T) {
	for in, want := range map[string]int64{
		"":        0,
		"0":       0,
		"1000":    1000,
		"50MiB/s": 50 << 20,
		"10MB/s":  10_000_000,
		"2GiB":    2 << 30,
		"512KiB":  512 << 10,
		"1.5MB/s": 1_500_000,
	} {
		got, err := copier.ParseRate(in)
		if err != nil || got != want {
			t.Errorf("ParseRate(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"fast", "-1MB", "10XB"} {
		if _, err := copier.ParseRate(in); err == nil {
			t.Errorf("ParseRate(%q): want error", in)
		}
	}
}

func TestCopySplitsTheRateLimitAcrossTheBusyWorkers(t *testing.T) {
	short := migration.Window{Start: window.Start, End: window.Start.Add(90 * time.Minute)}
	for _, tc := range []struct {
		name        string
		w           migration.Window
		concurrency int
		want        string
	}{
		{"vmctl's default of 2 workers", window, 0, "--vm-rate-limit=500"},
		{"4 workers", window, 4, "--vm-rate-limit=250"},
		{"more workers than hours: only 24 hourly requests run at once", window, 100, "--vm-rate-limit=41"},
		{"90 minutes are 2 hourly requests", short, 8, "--vm-rate-limit=500"},
	} {
		r := &fakeRunner{}
		v := &copier.VMCtl{SrcAddr: "s", DstAddr: "d", Concurrency: tc.concurrency, Run: r.run}

		if _, err := v.Copy(t.Context(), tc.w, 1000); err != nil {
			t.Fatal(err)
		}

		if !slices.Contains(r.args, tc.want) {
			t.Errorf("%s: args %q lack %s", tc.name, r.args, tc.want)
		}
	}
}

func TestCopyRoundsTheWindowOutToWholeSeconds(t *testing.T) {
	r := &fakeRunner{}
	v := &copier.VMCtl{SrcAddr: "s", DstAddr: "d", Run: r.run}
	w := migration.Window{
		Start: time.Date(2026, 1, 2, 10, 0, 0, 700_000_000, time.UTC),
		End:   time.Date(2026, 1, 2, 11, 30, 5, 250_000_000, time.UTC),
	}

	if _, err := v.Copy(t.Context(), w, 0); err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"--vm-native-filter-time-start=2026-01-02T10:00:00Z", "--vm-native-filter-time-end=2026-01-02T11:30:06Z"} {
		if !slices.Contains(r.args, want) {
			t.Errorf("args %q lack %s: the export must cover the whole recorded window", r.args, want)
		}
	}
}
