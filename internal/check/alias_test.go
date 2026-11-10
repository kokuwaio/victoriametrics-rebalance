package check_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/check"
	"github.com/kokuwaio/victoriametrics-rebalance/internal/cluster"
)

func TestCheckAliases(t *testing.T) {
	services := map[string]map[string]string{
		"on-blue":  {"app.kubernetes.io/name": "vmselect", "app.kubernetes.io/instance": "blue"},
		"on-green": {"app.kubernetes.io/name": "vmselect", "app.kubernetes.io/instance": "green"},
		"on-both":  {"app.kubernetes.io/name": "vmselect"},
		"on-none":  {"app.kubernetes.io/name": "vmselect", "app.kubernetes.io/instance": "other"},
		"external": {},
	}
	selector := func(_ context.Context, name string) (map[string]string, error) {
		sel, ok := services[name]
		if !ok {
			return nil, errors.New("not found")
		}
		return sel, nil
	}
	blue, green := newCluster("blue", 1), newCluster("green", 2)
	for alias, want := range map[string]string{
		"on-blue":  "PASS selects blue's vmselect pods",
		"on-green": "WARN selects green's vmselect pods; it should select blue's: switch its selector to blue",
		"on-both":  "WARN selects vmselect pods of [blue green]",
		"on-none":  "WARN selects no vmselect pods",
		"external": "WARN has no selector",
		"missing":  "WARN not found",
	} {
		a := check.Aliases{Names: map[cluster.Component]string{cluster.Select: alias}, Selector: selector}

		res := check.CheckAliases(t.Context(), a, blue, green, blue, check.Warn)

		wantLevel, wantMsg, _ := strings.Cut(want, " ")
		if len(res) != 1 || res[0].Name != "alias vmselect" || res[0].Level.String() != wantLevel ||
			!strings.Contains(res[0].Message, wantMsg) {
			t.Errorf("%s: results = %+v, want %s %q", alias, res, wantLevel, wantMsg)
		}
	}
}

func TestCheckAliasesSkipsAliasesThatAreNotSet(t *testing.T) {
	blue, green := newCluster("blue", 1), newCluster("green", 2)
	a := check.Aliases{Names: map[cluster.Component]string{cluster.Insert: ""}}

	if res := check.CheckAliases(t.Context(), a, blue, green, blue, check.Fail); len(res) != 0 || a.Configured() {
		t.Errorf("results = %+v, configured = %v", res, a.Configured())
	}
}
