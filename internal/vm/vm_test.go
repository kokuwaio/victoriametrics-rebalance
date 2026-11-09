package vm_test

import (
	"context"
	"errors"
	"math"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/vm"
)

func TestParseMetrics(t *testing.T) {
	data := `# HELP vm_rows_inserted_total Rows inserted
# TYPE vm_rows_inserted_total counter
vm_rows_inserted_total{type="promremotewrite"} 1500
vm_rows_inserted_total{type="native", path="/insert/0"} 2.5e3

vm_app_start_timestamp 1767225600
vm_rpc_rows_pending{name="vminsert",addr="vmstorage-0:8400"} 0 1767225600000
escaped{msg="a \"quoted\" value, with comma",nl="x\ny",slash="a\\b"} 1
special{kind="nan"} NaN
special{kind="inf"} +Inf
no_value{type="x"}
unclosed{type="x" 3
unquoted{type=x} 3
not_a_number 12abc
`
	samples := vm.ParseMetrics([]byte(data))

	if len(samples) != 7 {
		t.Fatalf("parsed %d samples, want 7 (comments, blank and broken lines skipped): %+v", len(samples), samples)
	}
	if s := samples[1]; s.Name != "vm_rows_inserted_total" || s.Labels["type"] != "native" || s.Labels["path"] != "/insert/0" || s.Value != 2500 {
		t.Errorf("second sample = %+v", s)
	}
	if s := samples[2]; s.Name != "vm_app_start_timestamp" || len(s.Labels) != 0 || s.Value != 1767225600 {
		t.Errorf("sample without labels = %+v", s)
	}
	if s := samples[3]; s.Value != 0 || s.Labels["addr"] != "vmstorage-0:8400" {
		t.Errorf("sample with a timestamp = %+v, want the value, not the timestamp", s)
	}
	esc := samples[4].Labels
	if esc["msg"] != `a "quoted" value, with comma` || esc["nl"] != "x\ny" || esc["slash"] != `a\b` {
		t.Errorf("escaped labels = %q", esc)
	}
	if !math.IsNaN(samples[5].Value) || !math.IsInf(samples[6].Value, 1) {
		t.Errorf("special values = %v, %v", samples[5].Value, samples[6].Value)
	}
}

func TestSumFiltersByNameAndLabels(t *testing.T) {
	samples := vm.ParseMetrics([]byte("a{type=\"x\"} 1\na{type=\"native\"} 10\nb 100\n"))

	if got := vm.Sum(samples, "a", nil); got != 11 {
		t.Errorf("Sum(a) = %v, want 11", got)
	}
	if got := vm.Sum(samples, "a", func(l map[string]string) bool { return l["type"] != "native" }); got != 1 {
		t.Errorf("Sum(a without native) = %v, want 1", got)
	}
	if got := vm.Sum(samples, "missing", nil); got != 0 {
		t.Errorf("Sum(missing) = %v, want 0", got)
	}
}

// getter returns body (or err) and records the path and query it was called with.
func getter(body string, err error) (vm.Getter, *url.Values, *string) {
	var q url.Values
	var path string
	return func(_ context.Context, p string, query url.Values) ([]byte, error) {
		path, q = p, query
		return []byte(body), err
	}, &q, &path
}

var day = time.Date(2026, 1, 2, 15, 0, 0, 0, time.UTC)

func TestTSDB(t *testing.T) {
	get, q, path := getter(`{"status":"success","data":{"totalSeries":42,"seriesCountByMetricName":[{"name":"up","value":40}]}}`, nil)

	st, err := vm.TSDB(t.Context(), get, day, 5)

	if err != nil || st.TotalSeries != 42 || len(st.SeriesCountByMetricName) != 1 || st.SeriesCountByMetricName[0].Name != "up" {
		t.Fatalf("TSDB = %+v, %v", st, err)
	}
	if *path != "api/v1/status/tsdb" || q.Get("date") != "2026-01-02" || q.Get("topN") != "5" || q.Get("deny_partial_response") != "1" {
		t.Errorf("request = %s %v", *path, *q)
	}
}

func TestQuerySumAddsUpAllSeries(t *testing.T) {
	get, q, path := getter(`{"status":"success","data":{"resultType":"vector","result":[
		{"metric":{"a":"1"},"value":[1767366000,"10"]},
		{"metric":{"a":"2"},"value":[1767366000,"5.5"]}]}}`, nil)

	sum, err := vm.QuerySum(t.Context(), get, "count_over_time(up[1d])", day)

	if err != nil || sum != 15.5 {
		t.Fatalf("QuerySum = %v, %v", sum, err)
	}
	if *path != "api/v1/query" || q.Get("query") != "count_over_time(up[1d])" || q.Get("time") != "1767366000.000" ||
		q.Get("nocache") != "1" || q.Get("deny_partial_response") != "1" {
		t.Errorf("request = %s %v", *path, *q)
	}
}

func TestQuerySumOfAnEmptyResultIsZero(t *testing.T) {
	get, _, _ := getter(`{"status":"success","data":{"resultType":"vector","result":[]}}`, nil)

	if sum, err := vm.QuerySum(t.Context(), get, "up", day); err != nil || sum != 0 {
		t.Errorf("QuerySum = %v, %v", sum, err)
	}
}

func TestQueryErrors(t *testing.T) {
	refused := errors.New("connection refused")
	for name, tc := range map[string]struct {
		body string
		err  error
		want string
	}{
		"transport error":   {"", refused, "connection refused"},
		"error status":      {`{"status":"error","error":"cannot find tenant"}`, nil, "cannot find tenant"},
		"invalid JSON":      {`<html>bad gateway</html>`, nil, "decode"},
		"non-numeric value": {`{"status":"success","data":{"result":[{"value":[1,"many"]}]}}`, nil, "many"},
		"number not string": {`{"status":"success","data":{"result":[{"value":[1,5]}]}}`, nil, "unexpected value"},
	} {
		get, _, _ := getter(tc.body, tc.err)
		if _, err := vm.QuerySum(t.Context(), get, "up", day); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("QuerySum, %s: err = %v, want %q", name, err, tc.want)
		}
	}
	for name, tc := range map[string]struct {
		body string
		err  error
		want string
	}{
		"transport error": {"", refused, "connection refused"},
		"error status":    {`{"status":"error","error":"too many series"}`, nil, "too many series"},
		"invalid JSON":    {`not json`, nil, "decode"},
	} {
		get, _, _ := getter(tc.body, tc.err)
		if _, err := vm.TSDB(t.Context(), get, day, 1); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("TSDB, %s: err = %v, want %q", name, err, tc.want)
		}
	}
}

func TestMetricSelectorQuotesTheName(t *testing.T) {
	for name, want := range map[string]string{
		"up":            `{__name__="up"}`,
		`odd"name\x`:    `{__name__="odd\"name\\x"}`,
		"with.dots-and": `{__name__="with.dots-and"}`,
	} {
		if got := vm.MetricSelector(name); got != want {
			t.Errorf("MetricSelector(%q) = %s, want %s", name, got, want)
		}
	}
}

func TestQuerySumKeepsMilliseconds(t *testing.T) {
	get, q, _ := getter(`{"status":"success","data":{"resultType":"vector","result":[]}}`, nil)

	if _, err := vm.QuerySum(t.Context(), get, "up", day.Add(-time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	if q.Get("time") != "1767365999.999" {
		t.Errorf("time = %s, want the evaluation time with millisecond precision", q.Get("time"))
	}
}
