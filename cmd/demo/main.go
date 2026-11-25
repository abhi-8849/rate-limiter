// main.go
//
// What it does:
//   - The gateway demo: a small HTTP server whose /hello route is protected by the rate limiter.
//   - Reads flags, wires the limiter, rules store and middleware together, and shuts down cleanly.
//
// Routes:
//   - GET /hello    protected by the middleware, answers "ok".
//   - GET /healthz  not limited, used by Docker and Kubernetes probes.
//
// Rules:
//   - A poller fetches rules from the Rules Service every -rules-poll and keeps them in a local store.
//   - If the Rules Service is down, the last-known rules stay in use.
//
// Things to know:
//   - -rate and -burst set the global default rule, so a request with no matching rule is still limited.
//   - -store memory keeps the limit per process. -store redis shares one limit between all instances.
//   - SIGINT or SIGTERM stops new connections and lets running requests finish.

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/abhi-8849/rate-limiter/limiter"
	"github.com/abhi-8849/rate-limiter/middleware"
	"github.com/abhi-8849/rate-limiter/redisstore"
	"github.com/abhi-8849/rate-limiter/rules"
)

// ErrUnknownStore is returned when -store is not "memory" or "redis".
var ErrUnknownStore = errors.New("unknown store")

// shutdownTimeout is how long running requests get to finish after a stop signal.
const shutdownTimeout = 10 * time.Second

func main() {
	if err := run(); err != nil {
		slog.Error("demo stopped with error", "err", err)
		os.Exit(1)
	}
}

// run parses flags, builds the server and serves until a stop signal or a server error.
func run() error {
	addr := flag.String("addr", ":8080", "listen address")
	rate := flag.Float64("rate", 5, "global default: tokens per second")
	burst := flag.Int64("burst", 10, "global default: bucket capacity")
	storeKind := flag.String("store", "memory", "limiter store: memory or redis")
	redisAddr := flag.String("redis", "localhost:6379", "Redis address, used with -store redis")
	redisTimeout := flag.Duration("redis-timeout", 5*time.Millisecond, "timeout for each Redis call")
	rulesURL := flag.String("rules-url", "http://localhost:9090", "Rules Service base URL")
	rulesPoll := flag.Duration("rules-poll", 10*time.Second, "how often to fetch rules")
	flag.Parse()

	lim, err := newLimiter(*storeKind, *redisAddr, *redisTimeout)
	if err != nil {
		return err
	}
	store := rules.NewStore(limiter.Rule{Rate: *rate, Burst: *burst})
	srv := &http.Server{Addr: *addr, Handler: newMux(lim, store)}

	// ctx is cancelled by SIGINT or SIGTERM. stop releases the signal handler on return.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The poller goroutine is started here and stops when ctx is cancelled (see Poller.Run).
	go rules.NewPoller(*rulesURL, *rulesPoll, store).Run(ctx)
	return serve(ctx, srv)
}

// newLimiter picks the limiter for the -store flag.
func newLimiter(kind, redisAddr string, redisTimeout time.Duration) (limiter.Limiter, error) {
	switch kind {
	case "memory":
		return limiter.NewMemory(nil), nil
	case "redis":
		return redisstore.New(redis.NewClient(&redis.Options{Addr: redisAddr}), redisTimeout), nil
	default:
		return nil, fmt.Errorf("%w: %q, want memory or redis", ErrUnknownStore, kind)
	}
}

// newMux builds the routes. Only /hello goes through the rate limit middleware.
func newMux(l limiter.Limiter, s *rules.Store) *http.ServeMux {
	limit := middleware.New(l, s)

	mux := http.NewServeMux()
	mux.Handle("GET /hello", limit(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	return mux
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
	slog.Info("demo started", "addr", srv.Addr)

	select {
	// A stop signal arrived: stop accepting and drain.
	case <-ctx.Done():
		slog.Info("demo stopping")
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
