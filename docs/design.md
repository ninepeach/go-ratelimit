# go-ratelimit — V1 Design

Status: implementation baseline
Modules: `github.com/ninepeach/go-ratelimit` (core + Memory), `github.com/ninepeach/go-ratelimit/redis` (Redis)

## 1. Problem and Constraints

Applications need a small, reusable way to reject operations that exceed an average rate, without coupling rate limiting to HTTP, authentication, or business models. The same API must work with an in-process Memory backend or a shared Redis backend.

Hard constraints:
- Both Memory and Redis are required in V1.
- Token Bucket is the only algorithm.
- Multiple rules in one request are checked and consumed atomically, or not consumed at all.
- Backend failures are errors, not rate-limit denials; never silently fail open.
- Core receives caller-provided keys and policies, not IP addresses, cookies, or sessions as special types.
- Changing a policy must never turn into request-time errors. Deployments change limits; traffic must keep flowing.

## 2. Modules and Dependencies

- The root module contains the public types, validation, the Memory backend (`memory`), and the shared behavioral contract suite (`ratelimittest`). It has no third-party dependencies.
- The Redis backend is a separate Go module (`redis/go.mod`) so that Memory-only users never compile or download a Redis client. It depends on `github.com/redis/go-redis/v9`.
- Neither module depends on application frameworks. Framework adapters (for example `go-base/ratelimit`) live in their own repositories.

## 3. Public API Contract

```go
type Policy struct {
    Group  string
    Limit  int64         // tokens replenished per Window
    Window time.Duration // whole microseconds, >= 1ms
    Burst  int64         // bucket capacity
}

func NewPolicy(group string, limit int64, window time.Duration, burst int64) (Policy, error)
func (p Policy) Rule(key string) Rule
func Validate(rules []Rule) error // used by backends; exported for third-party backends

type Rule struct {
    Key    string
    Policy Policy
}

type Result struct {
    Allowed    bool
    Remaining  int64
    RetryAfter time.Duration
    Denied     []int // indexes into the rules argument that lacked a token; nil when allowed
}

type Limiter interface {
    Allow(ctx context.Context, rules ...Rule) (Result, error)
}
```

- `NewPolicy` validates once at startup so that misconfiguration fails before traffic. `Allow` validates again because `Policy` is a plain value; validation cost is O(rules) and allocation-free.
- One accepted operation costs one token from every rule. A bucket that does not exist is full.
- `Remaining` is the minimum whole-token count across all rules **after** deduction when allowed, or **without** deduction when denied.
- `RetryAfter` is zero when allowed. When denied it is the maximum, over denied rules, of the time until that rule holds one whole token, assuming no other traffic. It is rounded up to the microsecond.
- `Denied` lets callers tell which dimension (for example IP or account) triggered the denial, for audit and responses.
- A denial is `(Result{Allowed:false}, nil)`. On error the Result is the zero value; callers check `err` first.

Constructors:

```go
memory.New(maxBuckets int) (*memory.Limiter, error)
redis.New(client *goredis.Client, prefix string) (*redis.Limiter, error)
```

Neither backend starts goroutines, so neither has `Close`. The Redis client is owned by the caller. Only `*goredis.Client` (single node, or Sentinel failover client) is accepted: Ring and Cluster clients cannot provide the atomicity contract, and accepting a generic interface would suggest they can.

## 4. Validation Limits

Every rule must satisfy, otherwise `Allow` returns `ErrInvalidRule` before touching any bucket:

| Field | Constraint |
| --- | --- |
| rules per call | 1 to 16 |
| `Group` | 1 to 64 bytes |
| `Key` | 1 to 256 bytes (keys are often attacker-controlled; this bounds memory per bucket) |
| `Limit`, `Burst` | >= 1 |
| `Window` | >= 1ms and a whole number of microseconds |
| precision | `Burst * WindowMicros + Limit <= 2^53 - 1` (see Section 5) |

Two rules with the same `(Group, Key)` in one call return `ErrDuplicateRule`.

The precision bound allows, for example, `Burst` up to 150,000,000 with a 1-minute window, 2,500,000 with 1 hour, and 104,000 with 24 hours.

## 5. Token Bucket Semantics and Representation

State per bucket: balance `b`, reference time `ts` (microseconds), and the window `w` (microseconds) the balance is expressed in.

Units: one token is `W` balance units, where `W = Window` in microseconds. Refill adds `Limit` units per elapsed microsecond, so refill is exact integer arithmetic with no rounding and no drift. Capacity is `C = Burst * W`.

Evaluation of one rule at decision time `now`:

```text
if bucket absent:            b = C, ts = now
else if stored w != W:       b = floor(b / w) * W          // policy window changed: keep whole tokens
b  = min(b, C)                                              // policy burst shrank
dt = max(0, now - ts)
dt = min(dt, ceil((C - b) / Limit))                         // never compute refill past full
b  = min(C, b + Limit * dt)
pass when b >= W
```

When every rule passes, each bucket stores `b - W` with `ts = max(ts, now)`. When any rule fails, nothing is written. All arithmetic stays below 2^53, which keeps it exact both in Go `int64` and in Redis Lua (whose numbers are IEEE doubles); integer division in Lua is done with an exactness correction step.

**Policy changes are applied, not rejected.** A bucket keeps only `(b, ts, w)`; the policy comes from the call. A new `Limit` changes the refill rate immediately, a smaller `Burst` clamps the balance, a different `Window` converts the balance to whole tokens (fractional progress is dropped, which errs toward stricter limiting once). There is no policy-conflict error: such an error would make a configuration rollout fail every request for up to the refill time of every live bucket. Two call sites that share a `Group` with different policies will alternate policies; that is a caller configuration bug, prevented by defining each policy once.

This is an average-rate Token Bucket, **not** a sliding-window quota. `Limit=60, Window=1m, Burst=10` refills one token per second with at most ten immediately available.

## 6. Atomic Multi-Rule Evaluation

`Allow(ctx, rules...)` is a single logical transaction:

1. Validate all rules and reject duplicates.
2. Return `ctx.Err()` if the context is already done.
3. Read every bucket and compute balances with one decision time.
4. If any rule lacks a whole token, deny without writing any bucket.
5. Otherwise deduct one token from every bucket and commit atomically.

Concurrent calls never overspend a bucket or partially charge a multi-rule request.

For Redis, cancellation after the script was sent leaves the outcome indeterminate: the script may or may not have committed. The error says so, and the backend never retries automatically.

## 7. Memory Backend

- One mutex guards all state; `Allow` holds it for O(rules + reclaimed buckets · log n).
- Time is monotonic elapsed time since construction, in microseconds.
- No goroutines or timers. A bucket is reclaimable once it has refilled to full, because a full bucket is indistinguishable from an absent one; reclaiming never changes observable behavior.
- Reclamation uses the policy of the bucket's last accepted request, exactly like the Redis TTL, so both backends agree after policy changes.
- Reclamation uses a min-heap ordered by the time each bucket becomes full. Every `Allow` pops buckets that are already full. Each bucket is pushed and popped at most once per lifetime, so reclamation is amortized O(log n) per bucket creation. No call ever scans the whole table.
- `maxBuckets` is required. If an accepted request needs new buckets and the table is still full after reclamation, `Allow` returns `ErrCapacity` and changes nothing. Denied requests never create buckets. Active buckets are never evicted.
- A key-flooding attacker can exhaust capacity; the explicit error lets the caller choose to fail closed or fall back. Size `maxBuckets` for the expected key cardinality within one refill period.

## 8. Redis Backend

- Requires Redis 6.2 or newer (scripts call `TIME`; effects replication is the default since 5.0). CI runs Redis 7; development used Redis 8.
- One Lua script evaluates and commits all rules atomically, invoked with `EVALSHA` and an `EVAL` fallback. No multi-rule operation is ever split into separate commands.
- Redis server time (`TIME`) is the only clock. Using `ts = max(ts, now)` keeps a clock step backwards (for example after failover) from crediting refill twice.
- Lua converts numbers to strings with 14 significant digits, so the script writes state with `string.format('%.0f', x)` to keep values up to 2^53 exact.
- One hash per bucket with fields `b`, `ts`, `w`. Key: `<prefix>:<len(group)>:<group><key>`; the length prefix makes the encoding unambiguous for arbitrary bytes. `prefix` is 1 to 64 bytes of `[A-Za-z0-9_.:-]`.
- After a commit each key gets `PEXPIRE` of the time to refill to full, rounded up to milliseconds, plus one millisecond. Expiry therefore never resets a bucket before it would be full anyway. Like Memory reclamation, this uses the policy of the last accepted request: after a policy change, a bucket that was already full under the old policy starts full under the new one.
- Failures are returned wrapped with `ErrBackend`; there is no fallback to Memory.
- Redis Cluster is unsupported for multi-rule calls: a script's keys must share a hash slot, and forcing all buckets into one slot would create a hot shard. Single-rule calls would work on Cluster but are not a V1 guarantee.

## 9. Error Model

| Error | Meaning |
| --- | --- |
| `ErrInvalidRule` | empty call, too many rules, or a rule outside Section 4 limits |
| `ErrDuplicateRule` | the same `(Group, Key)` twice in one call |
| `ErrCapacity` | Memory table full after reclamation |
| `ErrBackend` | Redis returned an error or the result could not be decoded |
| `context.Canceled`, `context.DeadlineExceeded` | returned (wrapped) as-is; for Redis the commit may be indeterminate |

All errors support `errors.Is`. Error strings never include keys, because keys may be personal data such as e-mail addresses or session identifiers.

## 10. Observability and Costs

No logger, metrics registry, or background monitoring. Results carry enough detail (`Denied`, `RetryAfter`) for callers to instrument decisions.

| Operation | Cost |
| --- | --- |
| Memory `Allow` | O(rules) under one mutex, plus amortized O(log n) reclamation; allocates only for new buckets and `Denied` |
| Redis `Allow` | one round trip; Lua work O(rules); blocks on the network |

These are design expectations. Benchmark before optimizing or publishing numbers.

## 11. Testing and Acceptance

A shared behavioral contract suite (`ratelimittest`) runs against both backends with a backend-supplied way to let time pass: Memory advances a controlled clock, Redis sleeps in real time and asserts with documented tolerances. Exact `Remaining` and `RetryAfter` values are asserted where the clock is controlled.

Required cases:
- validation limits, empty input, duplicates;
- full initial capacity, depletion, partial refill, full refill, Burst cap;
- denied multi-rule calls consume nothing and report `Denied` indexes;
- isolation by Group and Key, including encodings that would collide under naive concatenation;
- policy changes (Limit, Burst, Window) apply without errors;
- concurrent calls never overspend;
- Memory reclamation, capacity exhaustion, and cancellation before commit;
- Redis atomicity, TTL, backend errors, and cancellation.

Redis integration tests use a real Redis instance (`REDIS_TEST_ADDR`) and are reported as skipped otherwise. Run `go test ./...`, `go test -race ./...`, and `go vet ./...` in both modules.

## 12. Explicit Non-Goals

No HTTP middleware, cookie parsing, session creation or validation, IP reputation, bot detection, geolocation, policy inheritance, additional algorithms, dashboard, distributed transactions, or Redis Cluster multi-key support.

## 13. Design Decisions and Trade-offs

- **Token Bucket over sliding window:** simple burst behavior and continuous replenishment; strict rolling-window quotas are intentionally not provided.
- **Exact integer units over floating or fixed-point tokens:** no rounding drift and identical results in Go and Lua, at the cost of the precision bound in Section 4.
- **Apply policy changes over rejecting them:** rollouts cannot cause request failures; the price is that misconfigured shared groups alternate policies instead of failing loudly. This supersedes the policy-conflict error required by the original design baseline (commit `eb52680`), and was explicitly approved by the maintainer after V1 was implemented. The conflict error was rejected because a limit change or a rolling deploy with mixed old and new instances would turn every request on a live bucket into an error until that bucket refilled, which for long windows lasts hours. Encoding a policy fingerprint into the bucket key was also rejected: every rollout would reset limits to full burst, and mixed deploys would double the effective quota.
- **Caller-supplied keys over transport adapters:** reusable in HTTP, TCP, UDP, jobs, and other contexts; callers own identity extraction and key normalization (for example IPv6 /64 aggregation).
- **Atomic multi-rule API over independent calls:** prevents partial charging; requires one synchronization boundary per request.
- **Separate Redis module:** Memory users carry no Redis dependency; the price is two modules to version.
- **No Redis Cluster multi-key atomicity:** avoids misleading guarantees and forced hot-slot designs.
- **Explicit capacity errors over eviction:** eviction of active buckets would let attackers reset other users' limits.
