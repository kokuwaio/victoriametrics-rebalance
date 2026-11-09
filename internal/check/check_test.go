package check_test

import (
	"testing"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/check"
)

func TestLevelsPrintAsTheReportShowsThem(t *testing.T) {
	for l, want := range map[check.Level]string{check.Pass: "PASS", check.Warn: "WARN", check.Fail: "FAIL"} {
		if l.String() != want {
			t.Errorf("%d prints as %q, want %q", l, l.String(), want)
		}
	}
}

func TestOnlyAFailFailsTheReport(t *testing.T) {
	warnings := check.Report{Results: []check.Result{{Level: check.Pass}, {Level: check.Warn}}}
	if warnings.Failed() {
		t.Error("a warning failed the report")
	}
	failed := check.Report{Results: append(warnings.Results, check.Result{Level: check.Fail})}
	if !failed.Failed() || !check.AnyFailed(failed.Results) {
		t.Error("a failure didn't fail the report")
	}
}
