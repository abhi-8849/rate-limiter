// store.go
//
// What it does:
//   - Holds the rate limit rules in memory and finds the right rule for a request.
//   - Defines Rule, the stored form of a limit (tenant, endpoint, limit per window, burst).
//
// Where it is used:
//   - The Rules Service keeps the master copy: Put writes it, Since serves it to gateways.
//   - Each gateway keeps a local copy: Apply fills it from the poller, Match reads it per request.
//
// Things to know:
//   - Every Put gets the next version number, so a gateway can ask only for what changed.
//   - Match always returns a rule, so no request is left unprotected.
//   - Rules live in memory only and are lost on restart.

// Package rules stores rate limit rules and keeps gateways in sync with them.
package rules

import (
	"errors"
	"fmt"
	"sync"

	"github.com/abhi-8849/rate-limiter/limiter"
)

// ErrInvalidRule is returned by Validate when a rule has a limit, window or burst that is not greater than zero.
var ErrInvalidRule = errors.New("invalid rule")

// AnyEndpoint is the endpoint value of a tenant default rule.
// It applies to every endpoint of that tenant that has no rule of its own.
const AnyEndpoint = "*"

// Rule is one stored limit, in the shape the Rules Service accepts and returns as JSON.
type Rule struct {
	// ID identifies the rule. Putting the same ID again replaces the old rule.
	ID string `json:"id"`

	// Tenant is the customer the rule applies to.
	Tenant string `json:"tenant"`

	// Endpoint is the endpoint group the rule applies to, or AnyEndpoint for all of the tenant's endpoints.
	Endpoint string `json:"endpoint"`

	// Limit is the number of requests allowed per Window.
	Limit int64 `json:"limit"`

	// Window is the length of the period for Limit, in seconds.
	Window int64 `json:"window"`

	// Burst is the bucket capacity: how many requests may arrive back to back.
	Burst int64 `json:"burst"`

	// Version is set by the store on Put. It only ever increases.
	Version int64 `json:"version"`
}

// Validate rejects a rule the limiter could not use: limit, window and burst must all be greater than zero.
// A zero window would divide by zero in ToLimiter, and a zero burst would deny every request.
func (r Rule) Validate() error {
	if r.Limit <= 0 || r.Window <= 0 || r.Burst <= 0 {
		return fmt.Errorf("%w: limit, window and burst must be greater than zero", ErrInvalidRule)
	}
	return nil
}

// Snapshot is the answer to a rules query: the rules newer than the requested version, and the version to ask from next time.
// It is the JSON body of GET /v1/rules, shared by the Rules Service and the poller.
type Snapshot struct {
	Version int64  `json:"version"`
	Rules   []Rule `json:"rules"`
}

// ToLimiter converts the rule to what the limiter needs.
// Rate is Limit / Window, so 100 requests per 60 seconds is about 1.67 tokens per second.
func (r Rule) ToLimiter() limiter.Rule {
	return limiter.Rule{Rate: float64(r.Limit) / float64(r.Window), Burst: r.Burst}
}

// target is what a rule applies to. It is the lookup key for Match.
type target struct{ tenant, endpoint string }

// Store is a concurrency-safe in-memory set of rules. Create it with NewStore.
type Store struct {
	// def is the global default, returned by Match when no rule fits.
	def limiter.Rule

	// mu guards everything below. Match takes the read lock because it runs on every request.
	mu sync.RWMutex

	// version is the highest version stored so far.
	version int64

	// byID holds the rules. Same ID means same rule.
	byID map[string]Rule

	// byTarget maps what a rule applies to onto its ID, so Match is a map lookup and not a scan.
	byTarget map[target]string
}

// NewStore returns an empty store. def is the global default that Match falls back to,
// normally built from the -rate and -burst flags.
func NewStore(def limiter.Rule) *Store {
	return &Store{def: def, byID: make(map[string]Rule), byTarget: make(map[target]string)}
}

// Put is used by the Rules Service. It stores r with the next version number and returns it.
// If r.ID already exists, the old rule is replaced.
func (s *Store) Put(r Rule) Rule {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.version++
	r.Version = s.version
	s.set(r)
	return r
}

// Apply is used on the gateway. It stores rules received from the Rules Service, keeping their versions.
// A rule is skipped when the store already holds the same or a newer version of it, because versions only increase.
func (s *Store) Apply(rs []Rule) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, r := range rs {
		if old, ok := s.byID[r.ID]; ok && old.Version >= r.Version {
			continue
		}
		s.version = max(s.version, r.Version)
		s.set(r)
	}
}

// Since returns the rules whose version is greater than v.
// A gateway passes the last version it saw and gets only what changed. Since(0) returns everything.
func (s *Store) Since(v int64) []Rule {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := []Rule{}
	for _, r := range s.byID {
		if r.Version > v {
			out = append(out, r)
		}
	}
	return out
}

// Snapshot returns the rules newer than since, plus the highest version among them (or since if there are none).
// Asking again with that version returns only later changes.
func (s *Store) Snapshot(since int64) Snapshot {
	snap := Snapshot{Version: since, Rules: s.Since(since)}
	for _, r := range snap.Rules {
		snap.Version = max(snap.Version, r.Version)
	}
	return snap
}

// Match returns the limiter rule for a request. The first hit wins:
//  1. The rule for this tenant and this endpoint.
//  2. The tenant's default rule (endpoint AnyEndpoint).
//  3. The global default given to NewStore.
func (s *Store) Match(tenant, endpoint string) limiter.Rule {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, t := range []target{{tenant, endpoint}, {tenant, AnyEndpoint}} {
		if id, ok := s.byTarget[t]; ok {
			return s.byID[id].ToLimiter()
		}
	}
	return s.def
}

// set stores r and updates the target index. The caller must hold the write lock.
// If the ID had a different target before, that old index entry is removed so it no longer matches.
func (s *Store) set(r Rule) {
	if old, ok := s.byID[r.ID]; ok {
		if t := (target{old.Tenant, old.Endpoint}); s.byTarget[t] == r.ID {
			delete(s.byTarget, t)
		}
	}
	s.byID[r.ID] = r
	s.byTarget[target{r.Tenant, r.Endpoint}] = r.ID
}
