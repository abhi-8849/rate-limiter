// limiter.go
//
// What it does:
//   - Defines the types every limiter in this library shares.
//   - Rule: the limit for one key.
//   - Decision: the answer returned for each request.
//   - Limiter, Clock, Hooks: the interfaces other code plugs into.
//
// Why it exists:
//   - The in-memory limiter, the Redis limiter and the failover wrapper all use these types.
//   - Callers depend on this file only, never on a concrete limiter.
//   - It has no HTTP code, so the library works behind HTTP, gRPC or a plain function call.

// Package limiter is a token bucket rate limiting library.
package limiter

import (
	"context"
	"time"
)

// Rule is the limit applied to one key.
//
// A bucket holds at most Burst tokens and gains Rate tokens every second.
//   - Burst is the biggest spike allowed at once.
//   - Rate is the sustained speed.
//
// Example: Rate 5 and Burst 10 lets a client send 10 requests immediately, then 5 per second.
type Rule struct {
	// Rate is the refill speed in tokens per second. Must be greater than zero.
	Rate float64

	// Burst is the bucket capacity in tokens.
	// A full bucket allows this many requests back to back.
	Burst int64
}

// Decision is the answer to one Allow call.
// The HTTP layer turns it into a status code and rate limit headers.
type Decision struct {
	// Allowed is true when the request may proceed.
	Allowed bool

	// Limit is the bucket capacity. Sent to the client as X-RateLimit-Limit.
	Limit int64

	// Remaining is the whole tokens left after this call. Sent as X-RateLimit-Remaining.
	Remaining int64

	// RetryAfter is the time until enough tokens have refilled for this request.
	// Zero when the request was allowed.
	RetryAfter time.Duration
}

// Limiter decides whether a request may proceed.
//
// Arguments:
//   - key: who is being limited, for example tenant + API key + endpoint group.
//   - rule: how fast that key may go.
//   - cost: tokens this request uses. 1 for a normal request.
//
// Errors:
//   - An error means "could not decide", not "denied".
//   - The caller picks what to do. This project fails open.
//
// Implementations must be safe for concurrent use.
type Limiter interface {
	Allow(ctx context.Context, key string, rule Rule, cost int64) (Decision, error)
}

// Clock is the source of the current time.
//
// Limiters take a Clock instead of calling time.Now directly,
// so a test can pass a fake one and move time forward without sleeping.
type Clock interface {
	Now() time.Time
}

// Hooks lets the caller observe limiter events, for example to count them.
//
// The library only calls these methods, so the application decides what to do with each event.
// Implementations must be safe for concurrent use.
type Hooks interface {
	// OnAllowed is called when a request is allowed.
	OnAllowed()

	// OnDenied is called when a request is denied because the bucket is empty.
	OnDenied()

	// OnFailOpen is called when the primary limiter failed and the fallback answered instead.
	OnFailOpen()
}
