# go-ratelimit — V1 Design

Status: implementation baseline (no implementation implied)
Module: `github.com/ninepeach/go-ratelimit`

## 1. Problem and Constraints

Applications need a small, reusable way to reject operations that exceed an average rate, without coupling rate limiting to HTTP, authentication, or business models. The same API must work with an in-process Memory backend or a shared Redis backend.

Hard constraints:
- Both Memory and Redis are required in V1.
- Token Bucket is the only algorithm.
- Multiple rules in one request are checked and consumed atomically, or not consumed at all.
- Backend failures are errors, not rate-limit denials; never silently fail open.
- Core receives caller-provided keys and policies, not IP addresses, cookies, or sessions as special types.

## 2. Public API Contract

Proposed minimal core types:

```go
type Policy struct {
    Group  string
    Limit  int64
    Window time.Duration
    Burst  int64
}

type Rule struct {
    Key    string
    Policy Policy
}

type Result struct {
    Allowed    bool
    Remaining  int64
    RetryAfter time.Duration
}

type Limiter interface {
    Allow(ctx context.Context, rules ...Rule) (Result, error)
}
```

`Limit` is the number of tokens replenished per `Window`; `Burst` is bucket capacity. One accepted operation costs one token per distinct rule. A new bucket starts full.

`Remaining` is the minimum whole-token count across evaluated rules **after** an accepted operation, or the minimum whole-token count **without deduction** after a denial. `RetryAfter` is zero when allowed; when denied, it is the maximum wait until every currently insufficient rule has at least one token, assuming no intervening operations.

A successful result has `err == nil`, including ordinary denials. Errors return a zero-value result; callers must check `err` first.

Constructors should be straightforward: `memory.New(...)` and `redis.New(client, ...)`. Do not add configuration options unless a concrete requirement exists. Redis client lifecycle remains owned by the caller.

## 3. Policy and Bucket Identity

Bucket identity is the ordered pair `(Group, Key)`, encoded without ambiguous string concatenation. Groups are independent namespaces, not a hierarchy.

`Group` and `Key` must be non-empty. `Limit > 0`, `Window > 0`, and `Burst > 0`. Reject overflow-prone or otherwise unsupported values explicitly. Reject empty rule sets and duplicate bucket identities in one call.

A live bucket must not silently change its policy. If the same identity is used with different `Limit`, `Window`, or `Burst`, return a policy-conflict error while that bucket exists. Once expired, a new policy can create a fresh bucket. Backend key identity must not incorporate policy fields in a way that bypasses this conflict check.

## 4. Token Bucket Semantics

Let capacity `B = Burst`, replenishment rate `r = Limit / Window`, last stored balance `t`, and elapsed time `dt >= 0`.

Before evaluating a rule, calculate:

```text
available = min(B, t + r * dt)
```

A rule passes when `available >= 1`. For an accepted request, store `available - 1`; for a denied multi-rule request, no rule loses a token. Internal timestamps and idle-expiry metadata may advance on a denied call only if this does not alter the observable rate-limit contract.

Use a representation that avoids cumulative floating-point drift, overflow, and inconsistent rounding between backends. Define and test the precision/rounding contract before implementation. Never allow negative balances or balances above `Burst`.

This is an average-rate Token Bucket, **not** a strict sliding-window quota. For example, `Limit=60, Window=1m, Burst=10` means an average refill of one token per second with at most ten immediately available tokens.

## 5. Atomic Multi-Rule Evaluation

`Allow(ctx, rules...)` is a single logical transaction:

1. Validate all input rules and reject duplicate identities.
2. Read the current state of every bucket.
3. Calculate all replenished balances using one consistent decision time.
4. If any balance is below one, deny without deducting from any bucket.
5. Otherwise, deduct one token from every bucket and commit atomically.

Concurrent calls must never overspend a bucket or partially charge a multi-rule request. A context cancellation observed before commit must not cause a partial commit. Cancellation after an uncertain remote execution may produce an indeterminate outcome; do not claim the operation definitely did or did not consume tokens unless that is known. Document this Redis limitation and avoid automatic retries of ambiguous writes.

## 6. Memory Backend

- Thread-safe for concurrent `Allow` calls.
- Prefer one mutex initially; optimize only when benchmarks justify it.
- Use elapsed monotonic time when available.
- Expire inactive buckets without creating a goroutine or timer per bucket.
- Enforce a configurable maximum bucket count; capacity exhaustion is an explicit error.
- Never silently evict active buckets in a way that resets effective limits.
- Ensure a denied or invalid request does not partially consume tokens.
- If a background cleanup worker is introduced, provide an explicit, idempotent shutdown contract; otherwise avoid a `Close` method solely for symmetry.

Idle expiration must not reset a bucket earlier than the time required to refill to full capacity. Expiration and cleanup behavior must be tested.

## 7. Redis Backend

- Accept an existing Redis client; do not manage its connection pool or credentials.
- Use Redis server-side Lua to evaluate and commit all rules atomically.
- Use Redis server time as the authoritative clock for Redis decisions.
- Store sufficient state to validate policy conflicts and refill tokens.
- Namespace keys using a configurable application prefix and collision-safe encoding.
- Set TTLs so fully idle buckets are eventually reclaimed; TTL must not permit premature quota resets.
- Report Redis failures as errors; do not fall back to Memory.
- Never perform a multi-rule transaction as separate non-atomic Redis commands.

V1 targets a single Redis server or Sentinel-managed primary. Redis Cluster is **not guaranteed** for arbitrary multi-key atomic requests: Lua scripts require all involved keys to share a hash slot. Do not force all traffic into one hash slot without an explicit scalability decision. Document Cluster as unsupported for arbitrary multi-rule operations in V1.

## 8. Error Model

Use idiomatic errors with `errors.Is` support where meaningful:
- invalid rule or policy;
- duplicate rule;
- conflicting policy for an existing bucket;
- Memory capacity exceeded;
- backend failure;
- context cancellation or deadline exceeded.

Rate-limit rejection is `Result{Allowed:false}` with a nil error. Do not expose credentials, raw session identifiers, or sensitive Redis connection data in error strings or logs.

## 9. Observability and Costs

Do not add a mandatory logger, metrics registry, or background monitoring subsystem. Return actionable errors and documented results so callers can instrument decisions. Any optional instrumentation must have a demonstrated need and bounded overhead.

Document the cost of public operations. Memory is expected to scale with the number of supplied rules per call, subject to lock contention and cleanup; Redis requires a network round trip and a Lua operation whose work scales with the number of rules. These are design expectations, not measured performance claims. Benchmark before optimizing or publishing performance numbers.

## 10. Testing and Acceptance

Use a shared behavioral contract suite for both backends, plus backend-specific tests.

Required cases:
- validation, empty inputs, duplicate rules, policy conflict;
- full initial capacity, depletion, partial refill, full refill, Burst cap;
- denied requests do not consume any rule, including mixed pass/fail multi-rule calls;
- isolation by Group and Key;
- `Remaining` and `RetryAfter` rounding and consistency;
- concurrent calls never overspend;
- Memory cleanup, capacity exhaustion, and cancellation;
- Redis Lua atomicity, TTL, backend errors, and ambiguous cancellation behavior;
- equivalent externally observable results under controlled clocks.

Redis integration tests must use a real Redis instance. Do not claim equivalence solely from mocks. Where clocks cannot be controlled identically, assert bounded, documented tolerances rather than exact timestamp equality.

Once implementation exists, run:

```sh
go test ./...
go test -race ./...
go vet ./...
```

Report skipped Redis integration tests explicitly.

## 11. Explicit Non-Goals

No HTTP middleware, cookie parsing, session creation or validation, IP reputation, bot detection, geolocation, policy inheritance, additional rate algorithms, dashboard, or distributed transaction framework.

## 12. Design Decisions and Trade-offs

- **Token Bucket over sliding window:** simple burst behavior and continuous replenishment; strict rolling-window quotas are intentionally not provided.
- **Caller-supplied keys over transport adapters:** reusable in HTTP, TCP, UDP, jobs, and other contexts; callers own identity extraction and validation.
- **Atomic multi-rule API over independent calls:** prevents partial charging; requires a single synchronization boundary per request.
- **Memory + Redis over Memory-only V1:** supports both local and distributed deployment immediately, at the cost of maintaining backend parity.
- **No arbitrary Redis Cluster atomicity:** avoids misleading guarantees and forced hot-slot designs.
- **Small API over extensibility scaffolding:** additions require demonstrated use cases.

## 13. Open Implementation Details

Before writing core code, resolve these with the smallest correct choice and document it:
- Exact fixed-point token representation and time precision shared across Memory and Redis.
- Concrete constructors and minimal options for Memory capacity and Redis prefix.
- Precise TTL calculation and policy-conflict lifetime.
- Whether Redis Lua state is one key per bucket or a different encoding compatible with the atomicity contract.

Do not silently replace the public API or semantics while resolving these details. Escalate changes that cross architectural decision boundaries defined by the global AI engineering rules.
