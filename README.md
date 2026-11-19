# go-ratelimit

A lightweight, transport-independent Go rate limiting library: Token Bucket, Group + Key isolation, atomic multi-rule decisions, with an in-process Memory backend and a shared Redis backend.

The contract is in [docs/design.md](docs/design.md).

## Usage

```go
import (
    "github.com/ninepeach/go-ratelimit"
    "github.com/ninepeach/go-ratelimit/memory"
)

// Define policies once at startup; invalid values fail here, not under traffic.
perIP, err := ratelimit.NewPolicy("login:ip", 20, time.Minute, 10)
perAccount, err := ratelimit.NewPolicy("login:account", 5, time.Minute, 5)

limiter, err := memory.New(100_000) // or redis.New(client, "myapp:rl")

res, err := limiter.Allow(ctx, perIP.Rule("ip:"+ip), perAccount.Rule("acct:"+email))
switch {
case err != nil:
    // backend failure or invalid input: decide fail-open or fail-closed yourself
case !res.Allowed:
    // res.Denied says which rule(s) ran out; res.RetryAfter says how long to wait
}
```

- Either every rule has a token and one is taken from each, or nothing is consumed.
- A denial is `Allowed == false` with a nil error; errors are never denials.
- Changing a policy (limit, burst, window) takes effect immediately; it never causes errors.
- Keys are opaque strings chosen by the caller. Normalize them yourself (for example, aggregate IPv6 to /64).

## Backends

| Backend | Module | Scope | Notes |
| --- | --- | --- | --- |
| Memory | `github.com/ninepeach/go-ratelimit/memory` | one process | No goroutines. Full buckets are reclaimed lazily; `ErrCapacity` when `maxBuckets` live buckets exist. |
| Redis | `github.com/ninepeach/go-ratelimit/redis` (separate module) | shared | One Lua script per call, server time, Redis 6.2+. Single node or Sentinel; not Redis Cluster. |

## Development

```sh
go test -race ./...                                   # core + memory
cd redis && REDIS_TEST_ADDR=localhost:6379 go test -race ./...   # Redis integration
```

Redis tests are skipped without `REDIS_TEST_ADDR`. Both backends run the shared contract suite in `ratelimittest`.

The `redis` module requires a released version of the root module, so by default its tests build against that release, not the checkout. To change both modules together, use an uncommitted workspace (`go work init . ./redis`); CI sets `GOWORK=off` and always tests the released dependency. To release: tag `vX.Y.Z`, change `redis/go.mod` to require that version, then tag `redis/vX.Y.Z`.
