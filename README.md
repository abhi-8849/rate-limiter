# rate-limiter

A token bucket rate limiter in Go with per-tenant, per-endpoint rules.

- `limiter`: the reusable library, in-memory token bucket and failover.
- `redisstore`: Redis limiter, one atomic Lua script.
- `rules`, `middleware`, `cmd/demo`, `cmd/rules`: rules service and HTTP gateway demo.
