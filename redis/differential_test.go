package redis

import (
	"context"
	"errors"
	"maps"
	"math/rand/v2"
	"strconv"
	"testing"
	"time"

	"github.com/ninepeach/go-ratelimit"
	"github.com/ninepeach/go-ratelimit/internal/bucket"
)

// TestCorruptStateIsRejectedBeforeWriting checks that a bucket hash that was
// written by someone else is reported as ErrBackend and that the script fails
// before its commit phase: no bucket in the call is modified or created.
func TestCorruptStateIsRejectedBeforeWriting(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	p, _ := ratelimit.NewPolicy("corrupt", 1, time.Hour, 5)
	for name, fields := range map[string]map[string]string{
		"zero window":         {"b": "0", "ts": "1", "w": "0"},
		"non-numeric window":  {"b": "0", "ts": "1", "w": "x"},
		"negative balance":    {"b": "-1", "ts": "1", "w": "3600000000"},
		"fractional balance":  {"b": "1.5", "ts": "1", "w": "3600000000"},
		"missing ts":          {"b": "0", "w": "3600000000"},
		"only ts":             {"ts": "1"},
		"huge ts":             {"b": "0", "ts": "1e300", "w": "3600000000"},
		"ts above 2^53 - 1":   {"b": "0", "ts": "9007199254740992", "w": "3600000000"},
		"balance above limit": {"b": "9007199254740992", "ts": "1", "w": "3600000000"},
	} {
		t.Run(name, func(t *testing.T) {
			l, prefix := newLimiter(t, c)
			bad := prefix + ":7:corruptbad"
			untouched := prefix + ":7:corruptok"
			if err := c.HSet(ctx, bad, fields).Err(); err != nil {
				t.Fatal(err)
			}
			// The valid rule comes first so that a partial commit would be visible.
			_, err := l.Allow(ctx, p.Rule("ok"), p.Rule("bad"))
			if !errors.Is(err, ratelimit.ErrBackend) {
				t.Fatalf("want ErrBackend, got %v", err)
			}
			got, err := c.HGetAll(ctx, bad).Result()
			if err != nil || !maps.Equal(got, fields) {
				t.Fatalf("corrupt bucket modified: %v %v", got, err)
			}
			if n, _ := c.Exists(ctx, untouched).Result(); n != 0 {
				t.Fatal("valid bucket written despite the failed call")
			}
		})
	}
}

// TestLuaMatchesGo is a differential test of script.lua against
// internal/bucket. It seeds random bucket states, calls Allow, and recomputes
// the decision in Go.
//
// The script reads Redis TIME, which the test cannot inject. For an allowed
// call the decision time is recovered exactly: the script stores
// ts = max(old ts, now), and evaluating at that ts gives the same refill as
// evaluating at now. For a denied call nothing is stored, so the result is
// checked against the Go results at Redis TIME read just before and just after
// the call; balance is monotonic in time, so the reply must lie between them.
func TestLuaMatchesGo(t *testing.T) {
	c := client(t)
	l, _ := newLimiter(t, c)
	ctx := context.Background()
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)
	rng := rand.New(rand.NewPCG(seed, 0))

	windows := []time.Duration{time.Millisecond, 7 * time.Millisecond, time.Second, 3*time.Second + time.Microsecond, time.Minute, time.Hour}
	randPolicy := func(group string) ratelimit.Policy {
		for {
			w := windows[rng.IntN(len(windows))]
			limit := 1 + rng.Int64N(1000)
			burst := 1 + rng.Int64N(1000)
			if rng.IntN(10) == 0 { // near the 2^53 precision bound
				limit, w, burst = 1+rng.Int64N(10), time.Minute, 150_000_000-rng.Int64N(1000)
			}
			if p, err := ratelimit.NewPolicy(group, limit, w, burst); err == nil {
				return p
			}
		}
	}
	redisNow := func() int64 {
		t.Helper()
		tm, err := c.Time(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		return tm.UnixMicro()
	}

	var allowed, denied int
	for iter := range 400 {
		nrules := 1 + rng.IntN(3)
		rules := make([]ratelimit.Rule, nrules)
		keys := make([]string, nrules)
		params := make([]bucket.Params, nrules)
		prev := make([]*bucket.State, nrules)
		now := redisNow()
		for i := range rules {
			group := "g" + strconv.Itoa(i)
			key := "it" + strconv.Itoa(iter)
			rules[i] = randPolicy(group).Rule(key)
			keys[i] = l.key(group, key)
			p := rules[i].Policy
			params[i] = bucket.ParamsOf(p.Limit, p.Window, p.Burst)
			if rng.IntN(5) == 0 {
				continue // absent bucket
			}
			// Seed a state expressed in a possibly different window, with a
			// balance up to twice capacity (clamped by the script) and up to
			// two seconds of pending refill.
			sw := int64(windows[rng.IntN(len(windows))] / time.Microsecond)
			if rng.IntN(2) == 0 {
				sw = params[i].W
			}
			maxB := min(2*params[i].C, int64(1<<53-1))
			s := &bucket.State{B: rng.Int64N(maxB + 1), TS: now - rng.Int64N(2_000_001), W: sw}
			// A third of states hold less than one token, to exercise denials.
			if rng.IntN(3) == 0 {
				s.B = rng.Int64N(sw)
			}
			prev[i] = s
			if err := c.HSet(ctx, keys[i], "b", s.B, "ts", s.TS, "w", s.W).Err(); err != nil {
				t.Fatal(err)
			}
		}

		before := redisNow()
		res, err := l.Allow(ctx, rules...)
		after := redisNow()
		if err != nil {
			t.Fatalf("iter %d: %v", iter, err)
		}

		if res.Allowed {
			allowed++
			var at int64 = -1
			for i := range rules {
				ts, err := c.HGet(ctx, keys[i], "ts").Int64()
				if err != nil {
					t.Fatalf("iter %d: %v", iter, err)
				}
				// Recover the script's now from a bucket whose stored ts is
				// not in the future (ts == now there).
				if prev[i] == nil || prev[i].TS < ts {
					at = ts
				}
			}
			if at < 0 { // every seeded ts was at or after now: dt is zero everywhere
				at = before
			}
			wantRemaining := int64(-1)
			for i := range rules {
				b := bucket.Available(prev[i], params[i], at) - params[i].W
				got, err := c.HMGet(ctx, keys[i], "b", "w").Result()
				if err != nil {
					t.Fatal(err)
				}
				if got[0] != strconv.FormatInt(b, 10) || got[1] != strconv.FormatInt(params[i].W, 10) {
					t.Fatalf("iter %d rule %d: stored b,w = %v, Go computes %d,%d (prev %+v, params %+v, at %d)",
						iter, i, got, b, params[i].W, prev[i], params[i], at)
				}
				if tok := b / params[i].W; wantRemaining < 0 || tok < wantRemaining {
					wantRemaining = tok
				}
			}
			if res.Remaining != wantRemaining {
				t.Fatalf("iter %d: Remaining %d, Go computes %d", iter, res.Remaining, wantRemaining)
			}
			continue
		}

		denied++
		lo, hi := goDecision(prev, params, before), goDecision(prev, params, after)
		if lo.Allowed || res.Remaining < lo.Remaining || res.Remaining > hi.Remaining ||
			res.RetryAfter > lo.RetryAfter || (!hi.Allowed && res.RetryAfter < hi.RetryAfter) {
			t.Fatalf("iter %d: Lua %+v outside Go bounds [%+v, %+v]", iter, res, lo, hi)
		}
		// Nothing was written by the denial.
		for i := range rules {
			v, err := c.HMGet(ctx, keys[i], "b", "ts", "w").Result()
			if err != nil {
				t.Fatal(err)
			}
			if prev[i] == nil && v[0] != nil {
				t.Fatalf("iter %d rule %d: denial created a bucket", iter, i)
			}
			if prev[i] != nil && (v[0] != strconv.FormatInt(prev[i].B, 10) || v[1] != strconv.FormatInt(prev[i].TS, 10)) {
				t.Fatalf("iter %d rule %d: denial modified state %v", iter, i, v)
			}
		}
	}
	t.Logf("allowed %d, denied %d", allowed, denied)
	if allowed == 0 || denied == 0 {
		t.Fatalf("generator must exercise both outcomes: allowed %d, denied %d", allowed, denied)
	}
}

// goDecision evaluates the decision with internal/bucket at time now, without
// mutating anything. It mirrors the Memory backend's decision step.
func goDecision(prev []*bucket.State, params []bucket.Params, now int64) ratelimit.Result {
	res := ratelimit.Result{Remaining: -1, Allowed: true}
	var retry int64
	for i := range params {
		b := bucket.Available(prev[i], params[i], now)
		if b < params[i].W {
			res.Allowed = false
			retry = max(retry, bucket.Wait(b, params[i]))
		}
		if tok := b / params[i].W; res.Remaining < 0 || tok < res.Remaining {
			res.Remaining = tok
		}
	}
	res.RetryAfter = time.Duration(retry) * time.Microsecond
	return res
}
