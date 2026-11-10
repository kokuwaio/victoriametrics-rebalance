package check_test

import (
	"context"
	"testing"
	"time"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/check"
)

func noSleep(context.Context, time.Duration) error { return nil }

func TestWatchWrites(t *testing.T) {
	for name, tc := range map[string]struct {
		blueLive, greenLive, greenNative []int
		blueStarts                       []int
		want                             bool
	}{
		"dual write":                          {[]int{1000, 1600}, []int{1000, 1600}, []int{0, 0}, []int{1, 1}, false},
		"writers switched to green":           {[]int{1000, 1000}, []int{1000, 1600}, []int{0, 0}, []int{1, 1}, true},
		"no writer runs":                      {[]int{1000, 1000}, []int{1000, 1000}, []int{0, 0}, []int{1, 1}, false},
		"only the tool's imports reach green": {[]int{1000, 1000}, []int{1000, 1000}, []int{0, 600}, []int{1, 1}, false},
		"blue's vminsert restarted":           {[]int{1000, 0}, []int{1000, 1600}, []int{0, 0}, []int{1, 2}, false},
	} {
		t.Run(name, func(t *testing.T) {
			blue, green := newCluster("blue", 1), newCluster("green", 2)
			blue.setInsertedWithStarts(tc.blueLive, []int{0, 0}, tc.blueStarts)
			green.setInserted(tc.greenLive, tc.greenNative)

			w, err := check.WatchWrites(t.Context(), blue, green, time.Minute, noSleep)

			if err != nil || w.Switched != tc.want {
				t.Errorf("WatchWrites = %+v, %v; want switched=%v", w, err, tc.want)
			}
		})
	}
}
