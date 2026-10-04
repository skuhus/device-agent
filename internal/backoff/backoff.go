// Package backoff decides how long to wait before retrying something that
// failed: opening a device's port, or connecting to the broker. Both follow
// this one rule, configured separately (DESIGN-V2.md, "Reconnecting to the
// broker").
package backoff

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"
)

// Policy is the wait between attempts. With Grow off, every wait is Interval
// exactly. With Grow on, the wait doubles after each failed attempt, from
// Interval up to Max, and each wait is spread by Jitter, a fraction of it
// either way, so that stations that failed together do not retry in step.
type Policy struct {
	Interval time.Duration
	Grow     bool
	Max      time.Duration
	Jitter   float64
}

// Validate reports, on one line, everything that makes the policy unusable.
// Max and Jitter are checked only when the wait grows: otherwise they are not
// used, and an error about them would name a setting that does nothing.
func (policy Policy) Validate() error {
	var problems []string
	if policy.Interval <= 0 {
		problems = append(problems, fmt.Sprintf("the interval must be positive, got %s", policy.Interval))
	}
	if policy.Grow {
		if policy.Max < policy.Interval {
			problems = append(problems, fmt.Sprintf("max (%s) must be at least the interval (%s)", policy.Max, policy.Interval))
		}
		if policy.Jitter < 0 || policy.Jitter > 1 {
			problems = append(problems, fmt.Sprintf("jitter must be between 0 and 1, got %v", policy.Jitter))
		}
	}
	if len(problems) == 0 {
		return nil
	}
	return errors.New(strings.Join(problems, "; "))
}

// Wait is how long to wait before a retry; retry counts the retries since the
// last success, from 1.
func (policy Policy) Wait(retry int) time.Duration {
	if !policy.Grow {
		return policy.Interval
	}
	wait := policy.Interval
	for doubled := 1; doubled < retry && wait < policy.Max; doubled++ {
		wait *= 2
	}
	wait = min(wait, policy.Max)
	if policy.Jitter <= 0 {
		return wait
	}
	spread := float64(wait) * policy.Jitter
	return time.Duration(float64(wait) - spread + rand.Float64()*2*spread)
}
