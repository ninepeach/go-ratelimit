-- go-ratelimit atomic multi-rule Token Bucket. Mirrors internal/bucket.
-- KEYS[i]: bucket hash. ARGV[3i-2..3i]: Limit, W, C for rule i (balance units).
-- Returns {allowed, remaining, retry_us, ndenied, denied_index...}.
-- All numbers stay below 2^53, so double arithmetic is exact; integer division
-- is corrected for rounding and numbers are written with %.0f, not tostring.
-- redis/differential_test.go checks this script against internal/bucket.

local function idiv(a, b)
  local q = math.floor(a / b)
  if q * b > a then
    q = q - 1
  elseif (q + 1) * b <= a then
    q = q + 1
  end
  return q
end

local function cdiv(a, b)
  return idiv(a + b - 1, b)
end

local function num(x)
  return string.format('%.0f', x)
end

-- Parses a stored field as an exact integer in [lo, 2^53 - 1], or returns nil.
-- Stored state is untrusted (any client can write the key). Rejecting it here,
-- before the commit phase, keeps every commit argument exact and in range and
-- stops bad state from spreading: a zero window, for example, would otherwise
-- store a NaN balance that no later call could repair.
local function stored_int(x, lo)
  local v = tonumber(x)
  if v == nil or v ~= math.floor(v) or v < lo or v > 9007199254740991 then
    return nil
  end
  return v
end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local n = #KEYS
local avail, ts, params = {}, {}, {}
local denied = {}
local retry = 0

for i = 1, n do
  local L, W, C = tonumber(ARGV[3 * i - 2]), tonumber(ARGV[3 * i - 1]), tonumber(ARGV[3 * i])
  params[i] = {L, W, C}
  local v = redis.call('HMGET', KEYS[i], 'b', 'ts', 'w')
  local b, t0
  if not v[1] and not v[2] and not v[3] then
    b, t0 = C, now
  else
    local sw
    b, t0, sw = stored_int(v[1], 0), stored_int(v[2], 0), stored_int(v[3], 1)
    if not b or not t0 or not sw then
      -- Nothing has been written yet. The message omits the key: keys may be personal data.
      return redis.error_reply('ratelimit: corrupt bucket state')
    end
    if sw ~= W then
      local tokens = idiv(b, sw)
      if tokens >= idiv(C, W) then b = C else b = tokens * W end
    end
    if b > C then b = C end
    local dt = now - t0
    if dt < 0 then dt = 0 end
    local need = C - b
    if need > 0 then
      local m = cdiv(need, L)
      if dt > m then dt = m end
      b = b + L * dt
      if b > C then b = C end
    end
  end
  avail[i] = b
  ts[i] = math.max(t0, now)
  if b < W then
    denied[#denied + 1] = i - 1
    local w = cdiv(W - b, L)
    if w > retry then retry = w end
  end
end

local remaining = -1
if #denied > 0 then
  for i = 1, n do
    local tok = idiv(avail[i], params[i][2])
    if remaining < 0 or tok < remaining then remaining = tok end
  end
  local out = {0, remaining, retry, #denied}
  for _, d in ipairs(denied) do out[#out + 1] = d end
  return out
end

-- Commit phase. Every input is validated above, keys are distinct (Validate
-- rejects duplicate rules) and each key is absent or a hash (HMGET succeeded),
-- so these writes cannot fail on type or argument errors. Redis denies writes
-- for server state (OOM, read-only replica, min-replicas, disk errors) only
-- until a script's first write (scriptVerifyOOM and
-- scriptVerifyWriteCommandAllow in Redis src/script.c), so such a failure can
-- only occur before anything has been written. Redis cannot roll a script
-- back, which is why the reads and checks above must complete first.
for i = 1, n do
  local L, W, C = params[i][1], params[i][2], params[i][3]
  local nb = avail[i] - W
  local tok = idiv(nb, W)
  if remaining < 0 or tok < remaining then remaining = tok end
  redis.call('HSET', KEYS[i], 'b', num(nb), 'ts', num(ts[i]), 'w', num(W))
  -- Expire once the bucket would be full again, never earlier.
  local full_us = (ts[i] - now) + cdiv(C - nb, L)
  redis.call('PEXPIRE', KEYS[i], num(cdiv(full_us, 1000) + 1))
end
return {1, remaining, 0, 0}
