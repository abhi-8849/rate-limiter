// redis.go
//
// What it does:
//   - The Redis token bucket limiter. It shares one bucket per key between all gateway instances.
//
// How it works:
//   - Each Allow call runs token_bucket.lua in Redis with the key and the rule numbers.
//   - Redis stores only the counters (tokens and last_ms). The rule numbers travel as script arguments.
//
// Things to know:
//   - Every call has a timeout. A slow or dead Redis returns an error quickly, so the caller can fail open.
//   - This file never decides what to do on error. The failover wrapper does.
//   - go-redis is imported only in this package.

// Package redisstore is the Redis implementation of limiter.Limiter.
package redisstore

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/abhi-8849/rate-limiter/limiter"
)

// ErrRedis is returned when Redis cannot be reached, times out, or answers with something unexpected.
var ErrRedis = errors.New("redis limiter failed")

// tokenBucketLua is the script source, compiled into the binary so deployment needs no extra file.
//
//go:embed token_bucket.lua
var tokenBucketLua string

// store is the Redis Limiter. Create it with New.
type store struct {
	client  *redis.Client
	timeout time.Duration

	// script runs token_bucket.lua. It calls EVALSHA first and loads the script by itself if Redis does not know it yet.
	script *redis.Script
}

// New returns a Limiter backed by Redis.
//
// timeout limits each Allow call, including connecting and waiting for the answer.
// Keep it small (a few milliseconds), because the limiter sits on every request's path.
func New(c *redis.Client, timeout time.Duration) limiter.Limiter {
	return &store{client: c, timeout: timeout, script: redis.NewScript(tokenBucketLua)}
}

// Allow runs the script for key and converts its three numbers into a Decision.
// Any failure is wrapped in ErrRedis, so callers can match it with errors.Is.
func (s *store) Allow(ctx context.Context, key string, rule limiter.Rule, cost int64) (limiter.Decision, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	res, err := s.script.Run(ctx, s.client, []string{key}, rule.Rate, rule.Burst, cost).Int64Slice()
	if err != nil {
		return limiter.Decision{}, fmt.Errorf("%w: %w", ErrRedis, err)
	}
	if len(res) != 3 {
		return limiter.Decision{}, fmt.Errorf("%w: script returned %d values, want 3", ErrRedis, len(res))
	}
	return limiter.Decision{
		Allowed:    res[0] == 1,
		Limit:      rule.Burst,
		Remaining:  res[1],
		RetryAfter: time.Duration(res[2]) * time.Millisecond,
	}, nil
}
