-- token_bucket.lua
--
-- What it does:
--   Token bucket check for one key, run atomically inside Redis.
--   Refills the bucket, checks it, and takes the tokens in a single step.
--
-- Why it is a script:
--   Many gateways call Redis at the same time for the same key.
--   Redis runs a script without interleaving other commands, so two gateways can never both read
--   the same tokens and both spend them.
--
-- Why it uses Redis TIME:
--   Every gateway uses the one Redis clock, so clock differences between gateways do not matter.
--
-- Inputs:
--   KEYS[1]  the bucket key, for example {acme}:key1:hello
--   ARGV[1]  rate:  tokens added per second
--   ARGV[2]  burst: bucket capacity
--   ARGV[3]  cost:  tokens this request needs
--
-- Returns a list of three integers:
--   [1] allowed:     1 if the request may proceed, 0 if not
--   [2] remaining:   whole tokens left after this call
--   [3] retry_ms:    milliseconds until enough tokens exist, 0 if allowed
--
-- Stored in a hash per key: tokens (fractional) and last_ms (time of the last update).

local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])

local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)

local state = redis.call('HMGET', KEYS[1], 'tokens', 'last_ms')
local tokens = tonumber(state[1])
local last_ms = tonumber(state[2])

if tokens == nil then
  -- A new key starts with a full bucket, the same as the in-memory limiter.
  tokens = burst
  last_ms = now_ms
end

-- Refill for the time elapsed, capped at the bucket capacity.
local elapsed_s = math.max(0, now_ms - last_ms) / 1000
tokens = math.min(burst, tokens + elapsed_s * rate)

local allowed = 0
local retry_ms = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry_ms = math.ceil((cost - tokens) / rate * 1000)
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'last_ms', now_ms)
-- Expire after the time a bucket needs to refill fully: after that it equals a new key, so nothing is lost.
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000))

return {allowed, math.floor(tokens), retry_ms}
