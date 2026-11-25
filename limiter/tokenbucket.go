// tokenbucket.go
//
// What it does:
//   - The in-memory token bucket limiter.
//   - Keeps one bucket per key in a map inside the process.
//
// Where it is used:
//   - On its own, for the memory store.
//   - As the fallback when Redis is down.
//
// Things to know:
//   - The limit is per instance, not shared between instances.
//   - It never touches the network, so it cannot fail.

package limiter

import (
	"context"
	"sync"
	"time"
)

// realClock is the default Clock. It reads the system time.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// bucket is the state kept for one key.
//
// Only the token count and the time of the last update are stored.
// Tokens that refilled since then are calculated on the next call,
// so no background goroutine is needed.
type bucket struct {
	// tokens is the number of tokens left as of last.
	// Fractional, because refill is rate x elapsed seconds.
	tokens float64

	// last is the time tokens was last brought up to date.
	last time.Time
}

// memory is the in-memory Limiter. Create it with NewMemory.
type memory struct {
	clock Clock

	// mu guards buckets.
	// One mutex is enough: the critical section is a few arithmetic operations.
	mu      sync.Mutex
	buckets map[string]*bucket
}

// NewMemory returns a Limiter that keeps its buckets in this process.
//
// A nil clock uses real time.
// Pass a fake clock to control time in tests.
func NewMemory(clock Clock) Limiter {
	if clock == nil {
		clock = realClock{}
	}
	// TODO(v2): evict idle buckets, the map grows with the number of keys.
	return &memory{clock: clock, buckets: make(map[string]*bucket)}
}

// Allow takes cost tokens from the bucket for key, if enough are available.
//
// Steps:
//  1. Refill the bucket for the time since its last call, capped at rule.Burst.
//  2. If tokens >= cost, take them and allow the request.
//  3. Otherwise take nothing and report how long to wait in RetryAfter.
//
// Notes:
//   - A key seen for the first time starts with a full bucket, so a new client is not punished.
//   - The error is always nil, because memory cannot fail.
func (m *memory) Allow(_ context.Context, key string, rule Rule, cost int64) (Decision, error) {
	now := m.clock.Now()

	m.mu.Lock()
	defer m.mu.Unlock()

	b, ok := m.buckets[key]
	if !ok {
		b = &bucket{tokens: float64(rule.Burst), last: now}
		m.buckets[key] = b
	}
	b.tokens = min(float64(rule.Burst), b.tokens+now.Sub(b.last).Seconds()*rule.Rate)
	b.last = now

	d := Decision{Limit: rule.Burst}
	if b.tokens >= float64(cost) {
		b.tokens -= float64(cost)
		d.Allowed = true
	} else {
		missing := float64(cost) - b.tokens
		d.RetryAfter = time.Duration(missing / rule.Rate * float64(time.Second))
	}
	d.Remaining = int64(b.tokens)
	return d, nil
}
