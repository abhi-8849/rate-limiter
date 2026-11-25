// failover.go
//
// What it does:
//   - Wraps two limiters: a primary (Redis) and a fallback (in memory).
//   - Answers from the primary. If the primary returns an error, answers from the fallback instead.
//
// Why it exists:
//   - Redis is a separate service and can be slow or down. A rate limiter must not take the whole API down with it.
//   - This is "fail-open": when the shared limit cannot be checked, requests are still limited, but per instance.
//
// Things to know:
//   - The fallback gets the same key and rule, so limits keep roughly the same shape during an outage.
//   - The fallback's buckets are local to each instance, so the total allowed can be higher than with Redis.
//   - Only an error from the primary triggers the fallback. A denied request is a normal answer, not an error.

package limiter

import (
	"context"
	"log/slog"
)

// failover is the Limiter that switches to the fallback on a primary error. Create it with NewFailover.
type failover struct {
	primary  Limiter
	fallback Limiter

	// hooks is told about each fail-open event. It may be nil.
	hooks Hooks
}

// NewFailover returns a Limiter that uses primary and falls back to fallback when primary returns an error.
//
// h may be nil. When set, h.OnFailOpen is called each time the fallback answers.
func NewFailover(primary, fallback Limiter, h Hooks) Limiter {
	return &failover{primary: primary, fallback: fallback, hooks: h}
}

// Allow returns the primary's answer. On a primary error it logs the error, tells the hooks, and returns the fallback's answer.
func (f *failover) Allow(ctx context.Context, key string, rule Rule, cost int64) (Decision, error) {
	d, err := f.primary.Allow(ctx, key, rule, cost)
	if err == nil {
		return d, nil
	}

	slog.WarnContext(ctx, "primary limiter failed, failing open to fallback", "err", err)
	if f.hooks != nil {
		f.hooks.OnFailOpen()
	}
	return f.fallback.Allow(ctx, key, rule, cost)
}
