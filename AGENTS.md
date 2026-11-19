# Project Instructions — go-ratelimit

Follow the shared global AI engineering rules. This file adds repository-specific context only.

## Purpose
Independent, transport-agnostic Go rate limiting library.

## V1 Scope
- Memory and Redis backends from the first release.
- Token Bucket with Group + Key isolation and reusable policies.
- Atomic multi-rule decisions: if any rule rejects, no rule consumes tokens.
- Consistent semantics, concurrency safety, context handling, expiration, and bounded memory behavior.

## Boundaries
- Core accepts caller-supplied keys; it does not parse HTTP requests, validate sessions, or manage identity.
- Redis must use atomic server-side operations; do not silently degrade on backend failures.
- Document Redis Cluster hash-slot limitations rather than promising unsupported cross-slot atomicity.
- No bot detection, IP reputation, HTTP middleware, policy inheritance, or additional algorithms in V1 without approval.
- Document API and algorithm details in `docs/design.md` when that document is created; do not invent undocumented implementation behavior.

## Verification
- Once code exists, run `go test ./...`, `go test -race ./...`, and `go vet ./...`.
- Require shared behavior tests for Memory and Redis, plus Redis integration tests where a Redis instance is available.
