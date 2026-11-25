// main.go
//
// What it does:
//   - The Rules Service: a small HTTP server that stores rate limit rules and hands them to gateways.
//
// Endpoints:
//   - PUT /v1/rules/{tenant}/{endpoint}  create or replace the rule for that tenant and endpoint. Body: {limit, window, burst}.
//     Use * as the endpoint for the tenant default.
//   - GET /v1/rules?since={v}            rules newer than version v, as {version, rules}.
//
// Things to know:
//   - Rules are kept in memory and are lost on restart.
//   - limit, window and burst must be greater than zero, otherwise the answer is 400.
//   - SIGINT or SIGTERM stops new connections and lets running requests finish.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/abhi-8849/rate-limiter/limiter"
	"github.com/abhi-8849/rate-limiter/rules"
)

// shutdownTimeout is how long running requests get to finish after a stop signal.
const shutdownTimeout = 10 * time.Second

func main() {
	if err := run(); err != nil {
		slog.Error("rules service stopped with error", "err", err)
		os.Exit(1)
	}
}

// run parses flags, builds the server and serves until a stop signal or a server error.
func run() error {
	addr := flag.String("addr", ":9090", "listen address")
	flag.Parse()

	// The default rule is unused here: this service never calls Match, it only stores and serves rules.
	store := rules.NewStore(limiter.Rule{})
	srv := &http.Server{Addr: *addr, Handler: newMux(store)}

	// ctx is cancelled by SIGINT or SIGTERM. stop releases the signal handler on return.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return serve(ctx, srv)
}

// newMux builds the two endpoints.
func newMux(s *rules.Store) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/rules/{tenant}/{endpoint}", putRule(s))
	mux.HandleFunc("GET /v1/rules", getRules(s))
	return mux
}

// putRule stores the rule from the request body for the tenant and endpoint in the path, and answers with the stored rule and its version.
// The path decides the target, so a tenant or endpoint in the body is ignored.
func putRule(s *rules.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var rule rules.Rule
		if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
		rule.Tenant = r.PathValue("tenant")
		rule.Endpoint = r.PathValue("endpoint")
		if err := rule.Validate(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, s.Put(rule))
	}
}

// getRules answers with the rules newer than the since query parameter. A missing since means 0, which returns every rule.
func getRules(s *rules.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		since, err := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
		if err != nil && r.URL.Query().Has("since") {
			http.Error(w, "since must be a number", http.StatusBadRequest)
			return
		}
		writeJSON(w, s.Snapshot(since))
	}
}

// writeJSON sends v as a JSON 200 response.
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("write response", "err", err)
	}
}

// serve runs srv until ctx is cancelled or the server fails, then drains running requests.
//
// Goroutine:
//   - Started here, one per call. It runs ListenAndServe and ends when the server stops.
//   - It stops when Shutdown makes ListenAndServe return http.ErrServerClosed.
//
// Channel errCh:
//   - Sender: the goroutine, exactly once, with the result of ListenAndServe.
//   - Receiver: the select below.
//   - Never closed. It has room for one value, so the goroutine never blocks on send, even if nobody receives.
//   - It is never nil, so its select case is always live.
func serve(ctx context.Context, srv *http.Server) error {
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	slog.Info("rules service started", "addr", srv.Addr)

	select {
	// A stop signal arrived: stop accepting and drain.
	case <-ctx.Done():
		slog.Info("rules service stopping")
		shutCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		return srv.Shutdown(shutCtx)
	// The server failed on its own, for example the port is already in use.
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
