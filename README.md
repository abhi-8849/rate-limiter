# rate-limiter

A token bucket rate limiter in Go with per-tenant, per-endpoint rules.

- Importable `limiter` package with an in-memory and a Redis implementation.
- One atomic Lua script in Redis, so many gateway instances share one limit per key.
- Fail-open: if Redis errors or times out, an in-memory limiter answers instead.
- A Rules Service that stores rules; gateways poll it and cache the rules locally.
- `net/http` middleware that answers `429` and sets the rate limit headers.

## Design

This section has two parts. The high-level design (HLD) shows the whole system and how requests flow.
The low-level design (LLD) shows the packages, types and algorithms inside it.

### High-level design (HLD)

Where it sits: the gateway is the single entry point in front of the microservices.

```
 +---------+  request   +----+          +-----------------------------+  allowed   +--------------------+
 | clients | ---------> | LB | -------> | gateway 1..N                | ---------> | microservices      |
 |         | <--------- |    |          |  rate limit middleware      |            |  orders, payments, |
 +---------+  429 if    +----+          |  failover limiter           |            |  search            |
              denied                    |  rules cache + poller       |            +--------------------+
                                        +-------+-------------+-------+
                                                |             |
                                   Allow(key,   |             | poll every -rules-poll
                                   rule, cost)  |             | GET /v1/rules?since=N
                                                v             v
                                       +--------------+   +-----------------+              operator
                                       | Redis        |   | Rules Service   | <------ PUT /v1/rules/{id}
                                       | token bucket |   | rules + version |
                                       | per key      |   | (in memory)     |
                                       +--------------+   +-----------------+
```

What each part does:

| Part | Job |
|------|-----|
| Gateway | Single entry point. Runs the rate limit middleware before any service does work. Run as many instances as needed. |
| Redis | Holds one token bucket per key, shared by every gateway instance, so all instances enforce one limit. |
| Rules Service | Stores the rules and gives each change a new version. It is not on the request path. |
| Microservices | Only receive allowed requests. They contain no rate limit code. |

Request flow in a gateway:

1. The middleware builds the identity (tenant, API key, endpoint group) and the key `{tenant}:apikey:group`.
2. It finds the rule in the local rules cache.
3. It calls `Allow(ctx, key, rule, 1)`. Redis refills the bucket, checks it and takes the token in one script.
4. It sets `X-RateLimit-Limit` and `X-RateLimit-Remaining`.
5. Allowed: the request goes on to the service. Denied: the gateway answers `429` with `Retry-After`, and the service never sees it.

What happens in each case:

| Case | What happens |
|------|--------------|
| Tokens available | Redis takes the tokens, the request is forwarded, the headers show what is left. |
| Bucket empty | The gateway answers `429` with `Retry-After`. The service is not called. |
| Redis slow, down or erroring | The in-memory limiter answers with the same rule and the error is logged. Limits apply per instance until Redis is back. |
| An error from the limiter reaches the middleware | The middleware logs it and lets the request through. |
| New rule `PUT` | The Rules Service gives it the next version. Gateways apply it within one poll interval. |
| Rules Service down | Gateways keep the last rules they received and log each failed poll. |
| Rules Service restarts | Its rules are lost until they are put again. Running gateways keep their cached copy. |
| No rule for a request | The global default from `-rate` and `-burst` applies, so nothing is unprotected. |

Design decisions:

- **One Lua script in Redis.** Refill, check and decrement run as one step, so two gateways can never spend the same token.
- **Redis `TIME`.** The script uses the Redis clock, so clock differences between gateways do not matter.
- **Counters only in Redis.** The rule numbers (`rate`, `burst`) travel as script arguments, so a rule change needs no Redis update.
- **Key TTL.** A key expires after the time a bucket needs to refill fully, because after that it is the same as a new key.
- **Hash tag in the key.** The braces in `{tenant}:apikey:group` keep one tenant's keys on one Redis Cluster slot. This setup runs a single Redis.
- **Rules are cached in the gateway.** The Rules Service is not on the request path, so it can be slow or down without affecting requests.

In front of real services, wrap a reverse proxy with the middleware. The demo protects only `GET /hello`:

```go
proxy := httputil.NewSingleHostReverseProxy(ordersURL)
mux.Handle("/orders/", limit(proxy)) // the group for the key is the first path segment: "orders"
```

### Low-level design (LLD)

Packages and what they import (an arrow means "imports"):

```
 cmd/demo  ---> middleware ---> rules ---> limiter
     |  |                                     ^
     |  +---> redisstore ---------------------+
     +------> rules, limiter

 cmd/rules ---> rules ---> limiter
```

- `limiter` imports nothing from this project and knows nothing about HTTP.
- `redisstore` is the only package that imports go-redis.

Main types and functions:

| Package | Name | Role |
|---------|------|------|
| `limiter` | `Rule{Rate, Burst}` | The limit for one key: tokens per second and bucket capacity. |
| `limiter` | `Decision{Allowed, Limit, Remaining, RetryAfter}` | The answer to one `Allow` call. |
| `limiter` | `Limiter` | Interface: `Allow(ctx, key, rule, cost) (Decision, error)`. Every limiter implements it. |
| `limiter` | `Clock`, `Hooks` | Interfaces for the time source and for event callbacks. `Hooks` may be nil. |
| `limiter` | `NewMemory(clock)` | In-memory token bucket, one mutex around a map of buckets. |
| `limiter` | `NewFailover(primary, fallback, hooks)` | Uses the primary. On a primary error it logs and uses the fallback. |
| `redisstore` | `New(client, timeout)` | Redis limiter. Runs the embedded Lua script with a timeout on every call. |
| `rules` | `Rule{ID, Tenant, Endpoint, Limit, Window, Burst, Version}` | A stored rule. `ToLimiter()` gives `Rate = Limit / Window`. |
| `rules` | `Store` | `Put` (Rules Service), `Apply` (gateway), `Since`, `Snapshot`, `Match`. |
| `rules` | `Poller` | `Run(ctx)` fetches new rules on a timer and applies them to a `Store`. |
| `middleware` | `New(limiter, store)` | `net/http` middleware that does the request flow above. |

Token bucket algorithm. This is the same in the in-memory limiter and in the Lua script:

```
 on each Allow(key, rule, cost):

   1. tokens = min(burst, tokens + elapsed_seconds * rate)     refill for the time since the last call
   2. if tokens >= cost:   tokens = tokens - cost              allowed
      else:                retry_after = (cost - tokens) / rate   denied, nothing is taken
   3. save tokens and the current time
```

Example with `rate 5, burst 10`: a new key starts with 10 tokens, so 10 quick requests pass and the 11th is denied
with `Retry-After` of about 0.2 seconds. After one second of silence, 5 tokens are back.

What Redis stores, per key `{tenant}:apikey:group`:

| Field | Meaning |
|-------|---------|
| `tokens` | Tokens left at `last_ms`. Can be fractional. |
| `last_ms` | Redis time of the last update, in milliseconds. |
| TTL | `burst / rate` seconds, the time to refill fully. The key then expires. |

How a gateway learns about rules:

```
 Rules Service                    Poller (gateway)                 Rules cache (gateway)
      |                                  |                                  |
      |   GET /v1/rules?since=N          |                                  |
      | <------------------------------- |                                  |
      |   {version: M, rules: [...]}     |                                  |
      | -------------------------------> |   Apply(rules)                   |
      |                                  | -------------------------------> |
      |                                  |   remember since = M             |
```

- Each `Put` increases the version, so a poll returns only the rules changed since `N`.
- If a poll fails, the cache is not touched and the poller tries again on the next tick.
- `Match(tenant, endpoint)` is a map lookup, in this order: tenant and endpoint, tenant default (`*`), global default.

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
