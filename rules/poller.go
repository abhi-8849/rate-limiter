// poller.go
//
// What it does:
//   - Runs on the gateway. Fetches rules from the Rules Service on a timer and applies them to the local Store.
//
// Why it exists:
//   - The gateway must not call the Rules Service on every request: that would be slow and a single point of failure.
//   - It matches rules from its local copy, and this file keeps that copy fresh.
//
// Things to know:
//   - It asks only for rules newer than the last version it saw.
//   - If a poll fails, it logs the error and keeps the last-known rules, so requests keep being limited.

package rules

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// ErrPollFailed is returned when the Rules Service cannot be reached or answers with an error.
var ErrPollFailed = errors.New("rules poll failed")

// pollTimeout limits one request to the Rules Service, so a hung service cannot block the next poll forever.
const pollTimeout = 5 * time.Second

// Poller keeps a Store in sync with a Rules Service. Create it with NewPoller and start it with Run.
type Poller struct {
	url   string
	every time.Duration
	store *Store
	http  *http.Client

	// since is the highest version applied so far. Only the Run goroutine touches it, so it needs no lock.
	since int64
}

// NewPoller returns a Poller that fetches from the Rules Service at url every interval and applies the rules to s.
func NewPoller(url string, every time.Duration, s *Store) *Poller {
	return &Poller{url: url, every: every, store: s, http: &http.Client{Timeout: pollTimeout}}
}

// Run polls once straight away, then on every tick, until ctx is cancelled. It blocks, so start it with go.
//
// Goroutine:
//   - Started by the caller (the gateway's run function), one per Poller.
//   - Stops when ctx is cancelled and Run returns. No channel is closed, because Run owns none.
//
// Waiting:
//   - ctx.Done() is closed by the caller on shutdown. Run returns.
//   - ticker.C is sent to by the ticker every interval. Run polls again.
func (p *Poller) Run(ctx context.Context) {
	p.poll(ctx)

	ticker := time.NewTicker(p.every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.poll(ctx)
		}
	}
}

// poll fetches once. On failure it only logs, and the store keeps its last-known rules.
func (p *Poller) poll(ctx context.Context) {
	if err := p.fetch(ctx); err != nil {
		slog.ErrorContext(ctx, "rules poll error", "url", p.url, "err", err)
	}
}

// fetch asks for rules newer than p.since, applies them, and remembers the new version.
func (p *Poller) fetch(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/v1/rules?since=%d", p.url, p.since), nil)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrPollFailed, err)
	}
	resp, err := p.http.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrPollFailed, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: status %d", ErrPollFailed, resp.StatusCode)
	}

	var snap Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		return fmt.Errorf("%w: %w", ErrPollFailed, err)
	}
	p.store.Apply(snap.Rules)
	p.since = snap.Version
	return nil
}
