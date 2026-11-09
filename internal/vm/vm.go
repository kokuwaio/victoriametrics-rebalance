// Package vm reads VictoriaMetrics' /metrics output and its Prometheus query API.
package vm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Sample is one line of the Prometheus text exposition format.
type Sample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// ParseMetrics parses /metrics output. Comments and lines it can't parse are skipped.
func ParseMetrics(data []byte) []Sample {
	var out []Sample
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || line[0] == '#' {
			continue
		}
		if s, ok := parseLine(line); ok {
			out = append(out, s)
		}
	}
	return out
}

func parseLine(line string) (Sample, bool) {
	s := Sample{Labels: map[string]string{}}
	rest := line
	if i := strings.IndexAny(line, "{ "); i < 0 {
		return s, false
	} else {
		s.Name, rest = line[:i], line[i:]
	}
	if strings.HasPrefix(rest, "{") {
		end, ok := parseLabels(rest[1:], s.Labels)
		if !ok {
			return s, false
		}
		rest = rest[1+end:]
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return s, false
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return s, false
	}
	s.Value = v
	return s, true
}

// parseLabels parses `a="x",b="y"}` into labels and returns the index just after the closing brace.
func parseLabels(s string, labels map[string]string) (int, bool) {
	i := 0
	for {
		for i < len(s) && (s[i] == ' ' || s[i] == ',') {
			i++
		}
		if i < len(s) && s[i] == '}' {
			return i + 1, true
		}
		eq := strings.IndexByte(s[i:], '=')
		if eq < 0 || i+eq+1 >= len(s) || s[i+eq+1] != '"' {
			return 0, false
		}
		name := strings.TrimSpace(s[i : i+eq])
		i += eq + 2
		var val strings.Builder
		for ; i < len(s) && s[i] != '"'; i++ {
			if s[i] == '\\' && i+1 < len(s) {
				i++
				switch s[i] {
				case 'n':
					val.WriteByte('\n')
				default:
					val.WriteByte(s[i])
				}
				continue
			}
			val.WriteByte(s[i])
		}
		if i >= len(s) {
			return 0, false
		}
		labels[name] = val.String()
		i++
	}
}

// Sum adds up the values of all samples named name for which keep returns true (nil keeps all).
func Sum(samples []Sample, name string, keep func(labels map[string]string) bool) float64 {
	var total float64
	for _, s := range samples {
		if s.Name == name && (keep == nil || keep(s.Labels)) {
			total += s.Value
		}
	}
	return total
}

// Getter sends GET path?query and returns the response body.
type Getter func(ctx context.Context, path string, query url.Values) ([]byte, error)

// TSDBStatus is the part of /api/v1/status/tsdb the tool uses.
type TSDBStatus struct {
	TotalSeries             int64       `json:"totalSeries"`
	SeriesCountByMetricName []NameCount `json:"seriesCountByMetricName"`
}

// NameCount is a metric name with its series count.
type NameCount struct {
	Name  string `json:"name"`
	Value int64  `json:"value"`
}

// TSDB returns the TSDB status of one day (UTC) with the topN metric names by series count. It fails instead of
// returning a partial result when a vmstorage node is unavailable.
func TSDB(ctx context.Context, get Getter, day time.Time, topN int) (TSDBStatus, error) {
	q := url.Values{"date": {day.UTC().Format(time.DateOnly)}, "topN": {strconv.Itoa(topN)}, "deny_partial_response": {"1"}}
	body, err := get(ctx, "api/v1/status/tsdb", q)
	if err != nil {
		return TSDBStatus{}, err
	}
	var resp struct {
		Status string     `json:"status"`
		Error  string     `json:"error"`
		Data   TSDBStatus `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return TSDBStatus{}, fmt.Errorf("decode TSDB status: %w", err)
	}
	if resp.Status != "success" {
		return TSDBStatus{}, fmt.Errorf("TSDB status: %s", resp.Error)
	}
	return resp.Data, nil
}

// QuerySum runs an instant query at t (millisecond precision), bypassing the rollup cache, and returns the sum of all result values
// (0 for an empty result). It fails instead of returning a partial result when a vmstorage node is unavailable.
func QuerySum(ctx context.Context, get Getter, query string, t time.Time) (float64, error) {
	q := url.Values{
		"query":                 {query},
		"time":                  {strconv.FormatFloat(float64(t.UnixMilli())/1000, 'f', 3, 64)},
		"nocache":               {"1"},
		"deny_partial_response": {"1"},
	}
	body, err := get(ctx, "api/v1/query", q)
	if err != nil {
		return 0, err
	}
	var resp struct {
		Status string `json:"status"`
		Error  string `json:"error"`
		Data   struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, fmt.Errorf("decode query result: %w", err)
	}
	if resp.Status != "success" {
		return 0, fmt.Errorf("query %s: %s", query, resp.Error)
	}
	var sum float64
	for _, r := range resp.Data.Result {
		s, ok := r.Value[1].(string)
		if !ok {
			return 0, fmt.Errorf("query %s: unexpected value %v", query, r.Value[1])
		}
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, fmt.Errorf("query %s: %w", query, err)
		}
		sum += v
	}
	return sum, nil
}

// MetricSelector returns a series selector for exactly the metric name, safe for any name.
func MetricSelector(name string) string {
	return `{__name__=` + strconv.Quote(name) + `}`
}
