// Package copier copies a time window from blue to green.
package copier

import (
	"context"
	"fmt"
	"io"
	"math"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/migration"
)

// Runner runs a command and writes its combined stdout and stderr to out.
type Runner func(ctx context.Context, name string, args []string, out io.Writer) error

// Exec runs the command as a child process.
func Exec(ctx context.Context, name string, args []string, out io.Writer) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout, cmd.Stderr = out, out
	return cmd.Run()
}

// VMCtl copies a window with `vmctl vm-native`: an export in native format from blue's vmselect, streamed into an
// import on green's vminsert.
type VMCtl struct {
	// Binary is the vmctl executable. Default: vmctl from PATH.
	Binary string
	// SrcAddr is blue's vmselect URL including the tenant path, e.g. http://vmselect-x:8481/select/0/prometheus.
	SrcAddr string
	// DstAddr is green's vminsert URL including the tenant path, e.g. http://vminsert-y:8480/insert/0/prometheus.
	DstAddr string
	// Concurrency is vmctl's number of import workers (--vm-concurrency). 0 keeps vmctl's default.
	Concurrency int
	// ExtraArgs are appended to the vmctl command line as they are.
	ExtraArgs []string
	// Run starts vmctl. Default: Exec.
	Run Runner
	// Output receives vmctl's output as it runs. May be nil.
	Output io.Writer
}

// outputTail is how much of vmctl's output is kept for parsing the stats and for error messages.
const outputTail = 64 << 10

// Copy runs vmctl once for the window, capped at rateLimit bytes per second in total across all workers (0: no
// limit), and returns the bytes it reports.
func (v *VMCtl) Copy(ctx context.Context, w migration.Window, rateLimit int64) (migration.CopyStats, error) {
	tail := &tailBuffer{max: outputTail}
	out := io.Writer(tail)
	if v.Output != nil {
		out = io.MultiWriter(v.Output, tail)
	}
	run := v.Run
	if run == nil {
		run = Exec
	}
	if err := run(ctx, v.binary(), v.args(w, rateLimit), out); err != nil {
		return migration.CopyStats{}, fmt.Errorf("vmctl: %w: %s", err, lastLines(tail.String(), 10))
	}
	return migration.CopyStats{Bytes: parseTotalBytes(tail.String())}, nil
}

func (v *VMCtl) binary() string {
	if v.Binary == "" {
		return "vmctl"
	}
	return v.Binary
}

func (v *VMCtl) args(w migration.Window, rateLimit int64) []string {
	args := []string{
		"vm-native", "-s", "--disable-progress-bar",
		"--vm-native-src-addr=" + v.SrcAddr,
		"--vm-native-dst-addr=" + v.DstAddr,
		`--vm-native-filter-match={__name__!=""}`,
		"--vm-native-filter-time-start=" + w.Start.UTC().Format(time.RFC3339),
		"--vm-native-filter-time-end=" + w.End.UTC().Format(time.RFC3339),
	}
	workers := vmctlDefaultConcurrency
	if v.Concurrency > 0 {
		workers = v.Concurrency
		args = append(args, "--vm-concurrency="+strconv.Itoa(v.Concurrency))
	}
	if rateLimit > 0 {
		// vmctl applies --vm-rate-limit to each worker's request, not to the whole process.
		perWorker := max(1, rateLimit/int64(workers))
		args = append(args, "--vm-rate-limit="+strconv.FormatInt(perWorker, 10))
	}
	return append(args, v.ExtraArgs...)
}

// vmctlDefaultConcurrency is vmctl's default for --vm-concurrency.
const vmctlDefaultConcurrency = 2

// vmctl prints "total bytes: 1.5 GB;" (SI units) when an import finishes.
var totalBytesRE = regexp.MustCompile(`total bytes: ([0-9.]+) ([kMGTPE]?)B`)

func parseTotalBytes(out string) int64 {
	m := totalBytesRE.FindAllStringSubmatch(out, -1)
	if len(m) == 0 {
		return 0
	}
	last := m[len(m)-1]
	n, err := strconv.ParseFloat(last[1], 64)
	if err != nil {
		return 0
	}
	if last[2] != "" {
		n *= math.Pow(1000, float64(strings.Index("kMGTPE", last[2])+1))
	}
	return int64(n)
}

var rateRE = regexp.MustCompile(`^([0-9]+(?:\.[0-9]+)?)\s*(|B|KB|MB|GB|KiB|MiB|GiB)(?:/s)?$`)

var rateUnits = map[string]float64{
	"": 1, "B": 1,
	"KB": 1e3, "MB": 1e6, "GB": 1e9,
	"KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30,
}

// ParseRate parses a transfer rate such as "50MiB/s", "10MB/s" or "1000" into bytes per second. An empty string
// means no limit (0).
func ParseRate(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	m := rateRE.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return 0, fmt.Errorf("invalid rate %q: want a number with an optional unit B, KB, MB, GB, KiB, MiB or GiB and an optional /s", s)
	}
	n, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0, fmt.Errorf("invalid rate %q: %w", s, err)
	}
	return int64(n * rateUnits[m[2]]), nil
}

// tailBuffer keeps the last max bytes written to it.
type tailBuffer struct {
	max int
	buf []byte
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.buf = append(t.buf, p...)
	if over := len(t.buf) - t.max; over > 0 {
		t.buf = t.buf[over:]
	}
	return len(p), nil
}

func (t *tailBuffer) String() string { return string(t.buf) }

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
