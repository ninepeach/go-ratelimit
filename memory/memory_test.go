package memory

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/ninepeach/go-ratelimit"
	"github.com/ninepeach/go-ratelimit/internal/bucket"
	"github.com/ninepeach/go-ratelimit/ratelimittest"
)

type fakeClock struct {
	mu sync.Mutex
	us int64
}

func (c *fakeClock) now() int64 { c.mu.Lock(); defer c.mu.Unlock(); return c.us }
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.us += d.Microseconds()
	c.mu.Unlock()
}

func newFake(t *testing.T, max int) (*Limiter, *fakeClock) {
	t.Helper()
	l, err := New(max)
	if err != nil {
		t.Fatal(err)
	}
	c := &fakeClock{us: 1_000_000}
	l.now = c.now
	return l, c
}

func TestContract(t *testing.T) {
	ratelimittest.Run(t, ratelimittest.Backend{
		Exact: true,
		New: func(t *testing.T) (ratelimit.Limiter, func(time.Duration)) {
			l, c := newFake(t, 10_000)
			return l, c.advance
		},
	})
}

func TestContractRealClock(t *testing.T) {
	ratelimittest.Run(t, ratelimittest.Backend{
		New: func(t *testing.T) (ratelimit.Limiter, func(time.Duration)) {
			l, err := New(10_000)
			if err != nil {
				t.Fatal(err)
			}
			return l, time.Sleep
		},
	})
}

func TestNewRejectsNonPositiveCapacity(t *testing.T) {
	if _, err := New(0); err == nil {
		t.Fatal("zero capacity accepted")
	}
}

func rule(t *testing.T, key string, limit int64, window time.Duration, burst int64) ratelimit.Rule {
	t.Helper()
	p, err := ratelimit.NewPolicy("g", limit, window, burst)
	if err != nil {
		t.Fatal(err)
	}
	return p.Rule(key)
}

func TestReclaimFullBuckets(t *testing.T) {
	l, c := newFake(t, 100)
	ctx := context.Background()
	fast := rule(t, "fast", 10, time.Second, 1) // full again after 100ms
	slow := rule(t, "slow", 1, time.Hour, 1)
	for _, r := range []ratelimit.Rule{fast, slow} {
		if _, err := l.Allow(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	if l.Len() != 2 {
		t.Fatalf("len %d", l.Len())
	}
	c.advance(99 * time.Millisecond)
	_, _ = l.Allow(ctx, rule(t, "probe", 1, time.Hour, 5)) // any call reclaims
	if l.Len() != 3 {
		t.Fatalf("bucket reclaimed before full: len %d", l.Len())
	}
	c.advance(time.Millisecond)
	_, _ = l.Allow(ctx, rule(t, "probe", 1, time.Hour, 5))
	if l.Len() != 2 {
		t.Fatalf("full bucket not reclaimed: len %d", l.Len())
	}
}

func TestDeniedCallsCreateNoBuckets(t *testing.T) {
	l, _ := newFake(t, 100)
	ctx := context.Background()
	blocked := rule(t, "blocked", 1, time.Hour, 1)
	_, _ = l.Allow(ctx, blocked)
	for i := range 50 {
		res, err := l.Allow(ctx, rule(t, string(rune('a'+i)), 1, time.Hour, 1), blocked)
		if err != nil || res.Allowed {
			t.Fatalf("%v %v", res, err)
		}
	}
	if l.Len() != 1 {
		t.Fatalf("denied calls created buckets: len %d", l.Len())
	}
}

func TestCapacity(t *testing.T) {
	l, c := newFake(t, 2)
	ctx := context.Background()
	a, b := rule(t, "a", 1, time.Second, 1), rule(t, "b", 1, time.Hour, 1)
	if _, err := l.Allow(ctx, a, b); err != nil {
		t.Fatal(err)
	}
	newKey := rule(t, "c", 1, time.Hour, 5)
	if _, err := l.Allow(ctx, newKey); !errors.Is(err, ratelimit.ErrCapacity) {
		t.Fatalf("want ErrCapacity, got %v", err)
	}
	// The failed call changed nothing; existing buckets keep working.
	if res, err := l.Allow(ctx, b); err != nil || res.Allowed {
		t.Fatalf("existing bucket: %+v %v", res, err)
	}
	if _, err := l.Allow(ctx, newKey, b); err != nil {
		t.Fatalf("a denied call must not need capacity: %v", err)
	}
	c.advance(time.Second) // a is full again and reclaimable
	res, err := l.Allow(ctx, newKey)
	if err != nil || !res.Allowed || res.Remaining != 4 {
		t.Fatalf("after reclaim: %+v %v", res, err)
	}
}

// TestMatchesReferenceModel drives random traffic through the Limiter and a
// reclaim-free reference built on the same arithmetic. With a fixed policy per
// key, reclamation must never change an observable result. (Policy changes
// are covered by the contract suite.)
func TestMatchesReferenceModel(t *testing.T) {
	l, c := newFake(t, 1_000_000)
	rng := rand.New(rand.NewPCG(1, 2))
	policies := []ratelimit.Policy{}
	for _, spec := range []struct {
		limit  int64
		window time.Duration
		burst  int64
	}{{1, time.Second, 1}, {3, time.Second, 5}, {60, time.Minute, 10}, {7, 3 * time.Second, 2}} {
		p, err := ratelimit.NewPolicy("model", spec.limit, spec.window, spec.burst)
		if err != nil {
			t.Fatal(err)
		}
		policies = append(policies, p)
	}
	type ref struct{ state map[string]*bucket.State }
	model := ref{state: map[string]*bucket.State{}}
	keys := []string{"a", "b", "c", "d", "e"}
	for step := range 20_000 {
		c.advance(time.Duration(rng.IntN(400)) * time.Millisecond)
		n := 1 + rng.IntN(3)
		perm := rng.Perm(len(keys))[:n]
		rules := make([]ratelimit.Rule, n)
		for i, k := range perm {
			rules[i] = policies[k%len(policies)].Rule(keys[k])
		}
		got, err := l.Allow(context.Background(), rules...)
		if err != nil {
			t.Fatal(err)
		}
		now := c.now()
		allowed, remaining, retry := true, int64(-1), int64(0)
		avail := make([]int64, n)
		params := make([]bucket.Params, n)
		for i, r := range rules {
			params[i] = bucket.ParamsOf(r.Policy.Limit, r.Policy.Window, r.Policy.Burst)
			avail[i] = bucket.Available(model.state[r.Key], params[i], now)
			if avail[i] < params[i].W {
				allowed = false
				retry = max(retry, bucket.Wait(avail[i], params[i]))
			}
		}
		for i, r := range rules {
			b := avail[i]
			if allowed {
				b -= params[i].W
				ts := now
				if s := model.state[r.Key]; s != nil && s.TS > ts {
					ts = s.TS
				}
				model.state[r.Key] = &bucket.State{B: b, TS: ts, W: params[i].W}
			}
			if tok := b / params[i].W; remaining < 0 || tok < remaining {
				remaining = tok
			}
		}
		if got.Allowed != allowed || got.Remaining != remaining || got.RetryAfter != time.Duration(retry)*time.Microsecond {
			t.Fatalf("step %d: got %+v, want allowed=%v remaining=%d retry=%dus", step, got, allowed, remaining, retry)
		}
	}
}

func BenchmarkAllowTwoRules(b *testing.B) {
	l, _ := New(1 << 20)
	ip, _ := ratelimit.NewPolicy("ip", 1_000_000, time.Second, 1_000_000)
	acct, _ := ratelimit.NewPolicy("acct", 1_000_000, time.Second, 1_000_000)
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		_, _ = l.Allow(ctx, ip.Rule("203.0.113.7"), acct.Rule("user-42"))
	}
}
