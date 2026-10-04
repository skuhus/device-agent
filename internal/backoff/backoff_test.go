package backoff

import (
	"strings"
	"testing"
	"time"
)

// With the backoff off, every retry waits the interval exactly, with no
// jitter (#13 Q3).
func TestWaitIsTheIntervalWhenNotGrowing(t *testing.T) {
	policy := Policy{Interval: time.Second, Max: time.Minute, Jitter: 0.3}
	for retry := 1; retry <= 20; retry++ {
		if wait := policy.Wait(retry); wait != time.Second {
			t.Errorf("retry %d waits %s, want 1s", retry, wait)
		}
	}
}

// With the backoff on, the wait doubles after each failed attempt and stops
// at its ceiling, so that something that stays away is not tried every
// interval (device-agent-spec.md, section 4.4: do not spin). A ceiling that
// doubling does not land on is where the wait stops, not the next doubling.
func TestWaitGrowsAndIsBounded(t *testing.T) {
	cases := []struct {
		ceiling time.Duration
		want    []time.Duration
	}{
		{8 * time.Second, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 8 * time.Second}},
		{5 * time.Second, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second}},
	}
	for _, tc := range cases {
		policy := Policy{Interval: time.Second, Grow: true, Max: tc.ceiling}
		for index, expected := range tc.want {
			retry := index + 1
			if wait := policy.Wait(retry); wait != expected {
				t.Errorf("max %s: retry %d waits %s, want %s", tc.ceiling, retry, wait, expected)
			}
		}
	}
}

// Jitter is what keeps a site full of stations from retrying in step, so it
// has to vary, and stay within its fraction either way.
func TestJitterVariesWithinItsBounds(t *testing.T) {
	const jitter = 0.3
	policy := Policy{Interval: time.Second, Grow: true, Max: time.Minute, Jitter: jitter}
	seen := make(map[time.Duration]bool)
	for draw := 0; draw < 50; draw++ {
		wait := policy.Wait(1)
		if wait < time.Duration(float64(time.Second)*(1-jitter)) || wait > time.Duration(float64(time.Second)*(1+jitter)) {
			t.Fatalf("wait %s is outside 1s +/- %v%%", wait, jitter*100)
		}
		seen[wait] = true
	}
	if len(seen) < 10 {
		t.Errorf("only %d distinct waits in 50 draws; the jitter is not spreading retries", len(seen))
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name   string
		policy Policy
		want   string
	}{
		{"a fixed interval", Policy{Interval: time.Second}, ""},
		{"no interval", Policy{}, "the interval must be positive"},
		{"max below the interval", Policy{Interval: 5 * time.Second, Grow: true, Max: time.Second}, "max (1s) must be at least the interval (5s)"},
		{"jitter above 1", Policy{Interval: time.Second, Grow: true, Max: time.Minute, Jitter: 1.5}, "jitter must be between 0 and 1"},
		{"max and jitter unused when not growing", Policy{Interval: time.Second, Max: time.Millisecond, Jitter: 2}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.policy.Validate()
			switch {
			case tc.want == "" && err != nil:
				t.Errorf("rejected: %v", err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
				t.Errorf("error = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}
