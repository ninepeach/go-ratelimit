// Package ratelimittest is the behavioral contract every go-ratelimit backend
// must pass. Backends call Run from their own tests.
package ratelimittest

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ninepeach/go-ratelimit"
)

// Backend adapts one implementation to the suite.
type Backend struct {
	// New returns a fresh, empty limiter and a function that lets d pass for
	// it: a controlled clock for in-process backends, a real sleep otherwise.
	New func(t *testing.T) (ratelimit.Limiter, func(d time.Duration))
	// Exact is true when New's clock is controlled, enabling exact timing assertions.
	Exact bool
}

// Run executes the contract suite.
func Run(t *testing.T, b Backend) {
	t.Run("BurstThenDeny", func(t *testing.T) { burstThenDeny(t, b) })
	t.Run("Refill", func(t *testing.T) { refill(t, b) })
	t.Run("BurstCap", func(t *testing.T) { burstCap(t, b) })
	t.Run("MultiRuleAtomic", func(t *testing.T) { multiRuleAtomic(t, b) })
	t.Run("Isolation", func(t *testing.T) { isolation(t, b) })
	t.Run("PolicyChange", func(t *testing.T) { policyChange(t, b) })
	t.Run("InvalidInputConsumesNothing", func(t *testing.T) { invalidInput(t, b) })
	t.Run("CanceledContext", func(t *testing.T) { canceled(t, b) })
	t.Run("ConcurrentNeverOverspends", func(t *testing.T) { concurrent(t, b) })
}

func policy(t *testing.T, group string, limit int64, window time.Duration, burst int64) ratelimit.Policy {
	t.Helper()
	p, err := ratelimit.NewPolicy(group, limit, window, burst)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func allow(t *testing.T, l ratelimit.Limiter, rules ...ratelimit.Rule) ratelimit.Result {
	t.Helper()
	res, err := l.Allow(context.Background(), rules...)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	return res
}

func expect(t *testing.T, res ratelimit.Result, allowed bool, remaining int64, denied []int) {
	t.Helper()
	if res.Allowed != allowed || res.Remaining != remaining || !slices.Equal(res.Denied, denied) {
		t.Fatalf("got allowed=%v remaining=%d denied=%v, want %v %d %v", res.Allowed, res.Remaining, res.Denied, allowed, remaining, denied)
	}
	if allowed && res.RetryAfter != 0 {
		t.Fatalf("allowed result has RetryAfter %v", res.RetryAfter)
	}
	if !allowed && res.RetryAfter <= 0 {
		t.Fatalf("denied result has RetryAfter %v", res.RetryAfter)
	}
}

func burstThenDeny(t *testing.T, b Backend) {
	l, _ := b.New(t)
	r := policy(t, "burst", 1, time.Hour, 3).Rule("k")
	expect(t, allow(t, l, r), true, 2, nil)
	expect(t, allow(t, l, r), true, 1, nil)
	expect(t, allow(t, l, r), true, 0, nil)
	res := allow(t, l, r)
	expect(t, res, false, 0, []int{0})
	if b.Exact && res.RetryAfter != time.Hour {
		t.Fatalf("RetryAfter %v, want 1h", res.RetryAfter)
	}
	if res.RetryAfter > time.Hour || res.RetryAfter < time.Hour-5*time.Second {
		t.Fatalf("RetryAfter %v out of tolerance", res.RetryAfter)
	}
}

func refill(t *testing.T, b Backend) {
	l, advance := b.New(t)
	r := policy(t, "refill", 10, time.Second, 1).Rule("k") // one token per 100ms
	expect(t, allow(t, l, r), true, 0, nil)
	res := allow(t, l, r)
	expect(t, res, false, 0, []int{0})
	if b.Exact {
		if res.RetryAfter != 100*time.Millisecond {
			t.Fatalf("RetryAfter %v", res.RetryAfter)
		}
		advance(60 * time.Millisecond)
		if res := allow(t, l, r); res.Allowed || res.RetryAfter != 40*time.Millisecond {
			t.Fatalf("partial refill: %+v", res)
		}
		advance(40 * time.Millisecond)
		expect(t, allow(t, l, r), true, 0, nil)
		return
	}
	advance(150 * time.Millisecond)
	expect(t, allow(t, l, r), true, 0, nil)
}

func burstCap(t *testing.T, b Backend) {
	l, advance := b.New(t)
	r := policy(t, "cap", 1000, time.Second, 2).Rule("k")
	allow(t, l, r)
	advance(200 * time.Millisecond) // would refill 200 tokens without the cap
	expect(t, allow(t, l, r), true, 1, nil)
	expect(t, allow(t, l, r), true, 0, nil)
	if res := allow(t, l, r); res.Allowed && !b.Exact {
		// Real clocks may refill one token between calls at 1 token/ms; tolerate.
		return
	} else if b.Exact {
		expect(t, res, false, 0, []int{0})
	}
}

func multiRuleAtomic(t *testing.T, b Backend) {
	l, _ := b.New(t)
	tight := policy(t, "tight", 1, time.Hour, 1)
	loose := policy(t, "loose", 1, time.Hour, 5)
	expect(t, allow(t, l, tight.Rule("a")), true, 0, nil)
	expect(t, allow(t, l, loose.Rule("b"), tight.Rule("a")), false, 0, []int{1})
	// The passing rule was not charged by the denied call.
	expect(t, allow(t, l, loose.Rule("b")), true, 4, nil)
	expect(t, allow(t, l, tight.Rule("x"), loose.Rule("b")), true, 0, nil)
	expect(t, allow(t, l, tight.Rule("a"), tight.Rule("x")), false, 0, []int{0, 1})
	expect(t, allow(t, l, loose.Rule("b")), true, 2, nil)
}

func isolation(t *testing.T, b Backend) {
	l, _ := b.New(t)
	ab := policy(t, "ab", 1, time.Hour, 1)
	a := policy(t, "a", 1, time.Hour, 1)
	expect(t, allow(t, l, ab.Rule("c")), true, 0, nil)
	expect(t, allow(t, l, a.Rule("bc")), true, 0, nil) // "a"+"bc" must not collide with "ab"+"c"
	expect(t, allow(t, l, a.Rule("c")), true, 0, nil)
	other := policy(t, "other", 1, time.Hour, 1)
	expect(t, allow(t, l, other.Rule("c")), true, 0, nil) // same key, other group
	expect(t, allow(t, l, ab.Rule("c")), false, 0, []int{0})
}

func policyChange(t *testing.T, b Backend) {
	l, _ := b.New(t)
	five := policy(t, "change", 1, time.Hour, 5)
	expect(t, allow(t, l, five.Rule("k")), true, 4, nil)
	two := policy(t, "change", 1, time.Hour, 2)
	expect(t, allow(t, l, two.Rule("k")), true, 1, nil) // clamped to 2, then charged
	faster := policy(t, "change", 100, time.Hour, 5)
	expect(t, allow(t, l, faster.Rule("k")), true, 0, nil)
	longer := policy(t, "change", 1, 2*time.Hour, 5)
	res := allow(t, l, longer.Rule("k"))
	expect(t, res, false, 0, []int{0})
}

func invalidInput(t *testing.T, b Backend) {
	l, _ := b.New(t)
	p := policy(t, "invalid", 1, time.Hour, 1)
	ctx := context.Background()
	if _, err := l.Allow(ctx); !errors.Is(err, ratelimit.ErrInvalidRule) {
		t.Fatalf("empty: %v", err)
	}
	if _, err := l.Allow(ctx, p.Rule("k"), ratelimit.Rule{Key: "z"}); !errors.Is(err, ratelimit.ErrInvalidRule) {
		t.Fatalf("invalid policy: %v", err)
	}
	if _, err := l.Allow(ctx, p.Rule("k"), p.Rule("k")); !errors.Is(err, ratelimit.ErrDuplicateRule) {
		t.Fatalf("duplicate: %v", err)
	}
	expect(t, allow(t, l, p.Rule("k")), true, 0, nil)
}

func canceled(t *testing.T, b Backend) {
	l, _ := b.New(t)
	r := policy(t, "cancel", 1, time.Hour, 1).Rule("k")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := l.Allow(ctx, r); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	expect(t, allow(t, l, r), true, 0, nil)
}

func concurrent(t *testing.T, b Backend) {
	l, _ := b.New(t)
	shared := policy(t, "conc-shared", 1, time.Hour, 30)
	wide := policy(t, "conc-wide", 1, time.Hour, 1000)
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 4 {
				res, err := l.Allow(context.Background(), wide.Rule("w"), shared.Rule("s"))
				if err != nil {
					t.Errorf("goroutine %d: %v", i, err)
					return
				}
				if res.Allowed {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if allowed.Load() != 30 {
		t.Fatalf("allowed %d, want exactly 30", allowed.Load())
	}
	// Denied calls never charged the wide bucket.
	expect(t, allow(t, l, wide.Rule("w")), true, 1000-30-1, nil)
}
