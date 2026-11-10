package check

import (
	"context"
	"errors"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/cluster"
)

// Aliases are the Services that readers and writers use instead of the operator's own ones, so a cutover means
// changing their selectors. The tool only reads them.
type Aliases struct {
	// Names holds the Service name per component (cluster.Select, cluster.Insert). A component without a name isn't
	// checked.
	Names map[cluster.Component]string
	// Selector returns the selector of the Service name in the clusters' namespace.
	Selector func(ctx context.Context, name string) (map[string]string, error)
}

// Configured reports whether any alias is set.
func (a Aliases) Configured() bool {
	for _, n := range a.Names {
		if n != "" {
			return true
		}
	}
	return false
}

// CheckAliases checks that every configured alias selects pods of want (blue before the cutover, green after it)
// and none of the other cluster. A result that doesn't pass gets level miss.
func CheckAliases(ctx context.Context, a Aliases, blue, green, want cluster.Client, miss Level) []Result {
	var results []Result
	for _, c := range []cluster.Component{cluster.Select, cluster.Insert} {
		name := a.Names[c]
		if name == "" {
			continue
		}
		res := Result{Name: "alias " + string(c), Level: miss}
		switch selects, err := aliasTargets(ctx, a, name, c, blue, green); {
		case err != nil:
			res.Message, res.Err = fmt.Sprintf("Service %s: %v", name, err), err
		case len(selects) == 1 && selects[0] == want.Info().Name:
			res.Level, res.Message = Pass, fmt.Sprintf("Service %s selects %s's %s pods", name, want.Info().Name, c)
		case len(selects) == 0:
			res.Message = fmt.Sprintf("Service %s selects no %s pods of %s or %s; it should select %s's", name, c,
				blue.Info().Name, green.Info().Name, want.Info().Name)
		case len(selects) == 1:
			res.Message = fmt.Sprintf("Service %s selects %s's %s pods; it should select %s's: switch its selector to %s "+
				"(app.kubernetes.io/instance)", name, selects[0], c, want.Info().Name, want.Info().Name)
		default:
			res.Message = fmt.Sprintf("Service %s selects %s pods of %v; it should select only %s's (add "+
				"app.kubernetes.io/instance to its selector)", name, c, selects, want.Info().Name)
		}
		results = append(results, res)
	}
	return results
}

// aliasTargets returns the names of the clusters whose pods of component c the Service name selects. It fails when
// the Service also selects pods of another component, which can't answer c's requests.
func aliasTargets(ctx context.Context, a Aliases, name string, c cluster.Component, clusters ...cluster.Client) ([]string, error) {
	sel, err := a.Selector(ctx, name)
	if err != nil {
		return nil, err
	}
	if len(sel) == 0 {
		// A Service without a selector gets its endpoints from elsewhere; the tool can't tell where they point.
		return nil, errors.New("has no selector")
	}
	s := labels.SelectorFromSet(sel)
	var out []string
	for _, cl := range clusters {
		for _, comp := range cluster.Components {
			pods, err := cl.Pods(ctx, comp)
			if err != nil {
				return nil, err
			}
			if !slices.ContainsFunc(pods, func(p corev1.Pod) bool { return s.Matches(labels.Set(p.Labels)) }) {
				continue
			}
			if comp != c {
				return nil, fmt.Errorf("also selects %s's %s pods; add app.kubernetes.io/name: %s to its selector",
					cl.Info().Name, comp, c)
			}
			out = append(out, cl.Info().Name)
		}
	}
	return out, nil
}
