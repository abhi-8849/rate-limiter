// http.go
//
// What it does:
//   - net/http middleware that rate limits each request before the real handler runs.
//   - Works out who is calling, finds the rule, asks the limiter, and answers 429 when denied.
//
// Where it is used:
//   - The gateway demo wraps the protected routes with New(...).
//
// Things to know:
//   - Rate limit headers are set on every response, allowed or denied.
//   - If the limiter itself returns an error, the request is let through (fail-open).
//   - Only HTTP lives here. The limiter package stays free of HTTP.

// Package middleware adapts the limiter to net/http.
package middleware

// TODO(v2): gRPC unary interceptor

import (
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/abhi-8849/rate-limiter/limiter"
	"github.com/abhi-8849/rate-limiter/rules"
)

const (
	// anonymousTenant is used when the request has no X-Tenant header.
	anonymousTenant = "anonymous"

	// tenantHeader and apiKeyHeader are the request headers that identify the caller.
	tenantHeader = "X-Tenant"
	apiKeyHeader = "X-API-Key"
)

// New returns a middleware that limits requests with l, using the rules in s.
//
// For each request it:
//  1. Builds the identity: tenant, API key and endpoint group.
//  2. Matches a rule from the store.
//  3. Calls Allow with cost 1.
//  4. Sets X-RateLimit-Limit and X-RateLimit-Remaining.
//  5. Answers 429 with Retry-After if denied, otherwise calls the next handler.
func New(l limiter.Limiter, s *rules.Store) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tenant, apiKey, group := identity(r)
			rule := s.Match(tenant, group)

			d, err := l.Allow(r.Context(), key(tenant, apiKey, group), rule, 1)
			if err != nil {
				// Fail-open: a broken limiter must not take the whole service down with it.
				slog.ErrorContext(r.Context(), "limiter failed, allowing request", "err", err)
				next.ServeHTTP(w, r)
				return
			}

			w.Header().Set("X-RateLimit-Limit", strconv.FormatInt(d.Limit, 10))
			w.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(d.Remaining, 10))
			if !d.Allowed {
				w.Header().Set("Retry-After", retryAfterSeconds(d.RetryAfter))
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// identity reads who is calling and what they are calling.
//
//   - tenant: X-Tenant header, or "anonymous" if absent.
//   - apiKey: X-API-Key header, or the client IP if absent.
//   - group: the first path segment, so /hello becomes "hello".
func identity(r *http.Request) (tenant, apiKey, group string) {
	tenant = r.Header.Get(tenantHeader)
	if tenant == "" {
		tenant = anonymousTenant
	}
	apiKey = r.Header.Get(apiKeyHeader)
	if apiKey == "" {
		apiKey = clientIP(r)
	}
	group, _, _ = strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	return tenant, apiKey, group
}

// clientIP returns the caller's address without the port.
// It uses RemoteAddr only: forwarded headers can be faked by the client.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// key builds the limiter key as {tenant}:apikey:group.
// The braces are a Redis hash tag: only the tenant is hashed, so all of a tenant's keys land on one
// Redis Cluster slot. The POC runs a single Redis, but the key is ready for a cluster.
func key(tenant, apiKey, group string) string {
	return fmt.Sprintf("{%s}:%s:%s", tenant, apiKey, group)
}

// retryAfterSeconds rounds the wait up to whole seconds, as the Retry-After header requires.
// It never returns less than 1, so a client is never told to retry immediately.
func retryAfterSeconds(d time.Duration) string {
	return strconv.Itoa(max(1, int(math.Ceil(d.Seconds()))))
}
