# rate-limiter

A token bucket rate limiter in Go with per-tenant, per-endpoint rules.

- Importable `limiter` package with an in-memory and a Redis implementation.
- One atomic Lua script in Redis, so many gateway instances share one limit per key.
- Fail-open: if Redis errors or times out, an in-memory limiter answers instead.
- A Rules Service that stores rules; gateways poll it and cache the rules locally.
- `net/http` middleware that answers `429` and sets the rate limit headers.

## How it works

Each key (`{tenant}:apikey:group`) has a bucket that holds up to `burst` tokens and gains `rate` tokens per second.
A request takes `cost` tokens (1 for a normal request). With too few tokens it is denied.

A rule is `{tenant, endpoint, limit, window, burst}`. The rate is `limit / window` (window in seconds).
For a request, the first matching rule wins:

1. The rule for the tenant and the endpoint.
2. The tenant default (endpoint `*`).
3. The global default from the `-rate` and `-burst` flags.

The middleware reads the caller from the request:

| Part | Source | If absent |
|------|--------|-----------|
| tenant | `X-Tenant` header | `anonymous` |
| API key | `X-API-Key` header | client IP |
| endpoint group | first path segment (`/hello` gives `hello`) | empty |

## Layout

```
limiter/       Rule, Decision, Limiter, in-memory token bucket, failover
redisstore/    Redis limiter and the Lua script
rules/         rules store and the poller
middleware/    net/http middleware
cmd/demo/      gateway demo
cmd/rules/     Rules Service
deploy/        Dockerfile, docker-compose.yml, k8s/
```

## Library usage

In-memory limiter:

```go
l := limiter.NewMemory(nil) // nil clock uses real time

rule := limiter.Rule{Rate: 5, Burst: 10} // 5 tokens per second, up to 10 at once
d, err := l.Allow(ctx, "acme:key1:orders", rule, 1)
// d.Allowed, d.Limit, d.Remaining, d.RetryAfter
```

Redis limiter with fail-open to memory:

```go
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
primary := redisstore.New(rdb, 5*time.Millisecond) // timeout for each Redis call
l := limiter.NewFailover(primary, limiter.NewMemory(nil), nil) // last argument: optional Hooks
```

`Allow` returns an error only when the limiter could not decide. A denied request is a normal `Decision` with `Allowed` false.

## Middleware usage

```go
store := rules.NewStore(limiter.Rule{Rate: 5, Burst: 10}) // global default
limit := middleware.New(l, store)

mux := http.NewServeMux()
mux.Handle("GET /hello", limit(helloHandler))
```

- Every response carries `X-RateLimit-Limit` and `X-RateLimit-Remaining`.
- A denied request gets `429` with `Retry-After` in whole seconds.

## Run locally

```
go run ./cmd/rules                                  # Rules Service on :9090
go run ./cmd/demo -rate 5 -burst 10                 # gateway on :8080, in-memory limiter
go run ./cmd/demo -store redis -redis localhost:6379
```

Try it:

```
for i in $(seq 1 20); do curl -s -o /dev/null -w "%{http_code} " localhost:8080/hello; done; echo
```

About ten `200` responses, then `429`.

### Gateway flags

| Flag | Default | Meaning |
|------|---------|---------|
| `-addr` | `:8080` | listen address |
| `-rate` | `5` | global default, tokens per second |
| `-burst` | `10` | global default, bucket capacity |
| `-store` | `memory` | `memory` or `redis` |
| `-redis` | `localhost:6379` | Redis address |
| `-redis-timeout` | `5ms` | timeout for each Redis call |
| `-rules-url` | `http://localhost:9090` | Rules Service base URL |
| `-rules-poll` | `10s` | how often to fetch rules |

### Rules Service

Flag `-addr` (default `:9090`). Rules are kept in memory and are lost on restart.

Create or replace a rule:

```
curl -X PUT localhost:9090/v1/rules/1 \
  -d '{"tenant":"acme","endpoint":"*","limit":2,"window":10,"burst":2}'
```

`limit`, `window` and `burst` must be greater than zero, otherwise the answer is `400`.
Each stored rule gets a version that only increases.

List rules newer than a version:

```
curl "localhost:9090/v1/rules?since=0"
```

The gateway applies a new rule within one `-rules-poll` interval. If the Rules Service is down, it keeps the last rules it received.

## Run with Docker

Build and start Redis, the Rules Service and two gateway instances (from the repository root):

```
docker compose -f deploy/docker-compose.yml up --build
```

The gateways listen on `localhost:8081` and `localhost:8082`. Both use the same Redis, so they share one bucket per key:

```
for i in $(seq 1 10); do for p in 8081 8082; do curl -s -o /dev/null -w "%{http_code} " localhost:$p/hello; done; done; echo
```

Stop everything:

```
docker compose -f deploy/docker-compose.yml down
```

## Run with Kubernetes

Build the image so the cluster can find it (a local cluster such as OrbStack or Docker Desktop shares local images):

```
docker build -f deploy/Dockerfile -t rate-limiter:local .
```

Apply the manifests:

```
kubectl apply -f deploy/k8s
kubectl rollout status deploy/demo
```

This runs one Redis pod, one Rules Service pod and two gateway pods, each behind a ClusterIP Service. To call the gateway from your computer:

```
kubectl port-forward svc/demo 8080:8080
curl localhost:8080/hello
```

Remove everything:

```
kubectl delete -f deploy/k8s
```
