package check_test

import (
	"encoding/json"
	"errors"
	"math/rand/v2"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/check"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/vm"
)

var day = time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)

// selectAPI is a fake vmselect: it answers the TSDB status with total series and names, and count_over_time queries
// with counts[metric name]. It records every query.
type selectAPI struct {
	total   int64
	names   []vm.NameCount
	counts  map[string]float64
	failOn  string
	queries []url.Values
}

func (s *selectAPI) get(path string, q url.Values) (string, error) {
	switch path {
	case "api/v1/status/tsdb":
		if q.Get("date") != day.Format(time.DateOnly) {
			return "", errors.New("unexpected date " + q.Get("date"))
		}
		top := s.names
		if n := q.Get("topN"); n != "" {
			var limit int
			_ = json.Unmarshal([]byte(n), &limit)
			if limit < len(top) {
				top = top[:limit]
			}
		}
		b, _ := json.Marshal(map[string]any{"status": "success", "data": vm.TSDBStatus{TotalSeries: s.total, SeriesCountByMetricName: top}})
		return string(b), nil
	case "api/v1/query":
		s.queries = append(s.queries, q)
		for name, n := range s.counts {
			if strings.Contains(q.Get("query"), `"`+name+`"`) {
				if name == s.failOn {
					return `{"status":"error","error":"too many samples"}`, nil
				}
				b, _ := json.Marshal(map[string]any{"status": "success", "data": map[string]any{
					"resultType": "vector",
					"result":     []any{map[string]any{"metric": map[string]string{}, "value": []any{1, jsonNumber(n)}}},
				}})
				return string(b), nil
			}
		}
		return `{"status":"success","data":{"resultType":"vector","result":[]}}`, nil
	}
	return "", errors.New("unexpected path " + path)
}

func jsonNumber(f float64) string {
	b, _ := json.Marshal(f)
	return string(b)
}

func newSelect(total int64, counts map[string]float64) *selectAPI {
	s := &selectAPI{total: total, counts: counts}
	for _, n := range []string{"up", "http_requests_total", "go_goroutines", "node_cpu_seconds_total", "rare_metric"} {
		s.names = append(s.names, vm.NameCount{Name: n, Value: 10})
	}
	return s
}

var allCounts = map[string]float64{
	"up": 100, "http_requests_total": 200, "go_goroutines": 300, "node_cpu_seconds_total": 400, "rare_metric": 500,
}

func clustersWith(b, g *selectAPI) (*fakeCluster, *fakeCluster) {
	blue, green := newCluster("blue", 2), newCluster("green", 3)
	blue.selectGet, green.selectGet = b.get, g.get
	return blue, green
}

var verifyOpts = check.VerifyOptions{TopN: 2, RandomN: 1, Pool: 100, Tolerance: 0.01}

func withRand() check.VerifyOptions {
	o := verifyOpts
	o.Rand = rand.New(rand.NewPCG(1, 2))
	return o
}

func queriedNames(s *selectAPI) []string {
	var out []string
	for _, q := range s.queries {
		for name := range allCounts {
			if strings.Contains(q.Get("query"), `"`+name+`"`) {
				out = append(out, name)
			}
		}
	}
	slices.Sort(out)
	return out
}

func TestVerifyDayPassesWhenSeriesAndSampledCountsMatch(t *testing.T) {
	b, g := newSelect(1000, allCounts), newSelect(1000, allCounts)
	blue, green := clustersWith(b, g)

	r := check.VerifyDay(t.Context(), blue, green, day, day.Add(24*time.Hour), withRand())

	if !r.Pass || len(r.Problems) > 0 {
		t.Errorf("want pass, got %+v", r)
	}
	if r.BlueSeries != 1000 || r.GreenSeries != 1000 {
		t.Errorf("series = %d/%d", r.BlueSeries, r.GreenSeries)
	}
	names := queriedNames(g)
	if len(names) != 3 || !slices.Contains(names, "up") || !slices.Contains(names, "http_requests_total") {
		t.Errorf("green queried %v, want the top 2 by series count plus 1 random name", names)
	}
	if !slices.Equal(queriedNames(b), names) {
		t.Errorf("blue queried %v, green %v", queriedNames(b), names)
	}
	q := g.queries[0]
	if !strings.Contains(q.Get("query"), "count_over_time") || !strings.Contains(q.Get("query"), "[86400s]") ||
		q.Get("time") != "1767398400" || q.Get("nocache") != "1" {
		t.Errorf("query = %v, want count_over_time over the day at its end, bypassing the cache", q)
	}
}

func TestVerifyDayPartialDayCountsUpToEnd(t *testing.T) {
	b, g := newSelect(1000, allCounts), newSelect(1000, allCounts)
	blue, green := clustersWith(b, g)

	check.VerifyDay(t.Context(), blue, green, day, day.Add(6*time.Hour), withRand())

	q := g.queries[0]
	if !strings.Contains(q.Get("query"), "[21600s]") || q.Get("time") != "1767333600" {
		t.Errorf("query = %v, want a 6h window ending at 06:00", q)
	}
}

func TestVerifyDayFailures(t *testing.T) {
	fewer := map[string]float64{"up": 50, "http_requests_total": 200, "go_goroutines": 300, "node_cpu_seconds_total": 400, "rare_metric": 500}
	for _, tc := range []struct {
		name    string
		blue    *selectAPI
		green   *selectAPI
		pass    bool
		problem string
	}{
		{"green misses series", newSelect(1000, allCounts), newSelect(900, allCounts), false, "series"},
		{"shortfall within tolerance", newSelect(1000, allCounts), newSelect(995, allCounts), true, ""},
		{"green has more series", newSelect(1000, allCounts), newSelect(1200, allCounts), true, ""},
		{"green misses samples", newSelect(1000, allCounts), newSelect(1000, fewer), false, "up"},
		{"query fails on green", newSelect(1000, allCounts), func() *selectAPI {
			s := newSelect(1000, allCounts)
			s.failOn = "up"
			return s
		}(), false, "too many samples"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blue, green := clustersWith(tc.blue, tc.green)

			r := check.VerifyDay(t.Context(), blue, green, day, day.Add(24*time.Hour), withRand())

			if r.Pass != tc.pass {
				t.Errorf("pass = %v, want %v (%v)", r.Pass, tc.pass, r.Problems)
			}
			if tc.problem != "" && !strings.Contains(strings.Join(r.Problems, "\n"), tc.problem) {
				t.Errorf("problems %q don't mention %q", r.Problems, tc.problem)
			}
		})
	}
}

func TestVerifyDayPartialDaySkipsTheWholeDaySeriesCount(t *testing.T) {
	// Writers that bypass VMAgent created series on blue after the last synced point, which green can't have yet.
	b, g := newSelect(1000, allCounts), newSelect(900, allCounts)
	blue, green := clustersWith(b, g)

	r := check.VerifyDay(t.Context(), blue, green, day, day.Add(6*time.Hour), withRand())

	if !r.Pass {
		t.Errorf("want pass on a partial day with equal samples, got %v", r.Problems)
	}
}
