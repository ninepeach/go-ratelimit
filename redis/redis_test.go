package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"math"
	"os"
	"testing"
	"time"

	"github.com/ninepeach/go-ratelimit"
	"github.com/ninepeach/go-ratelimit/ratelimittest"
	goredis "github.com/redis/go-redis/v9"
)

func client(t *testing.T) *goredis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("REDIS_TEST_ADDR not set")
	}
	c := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { _ = c.Close() })
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis: %v", err)
	}
	return c
}

func newLimiter(t *testing.T, c *goredis.Client) (*Limiter, string) {
	t.Helper()
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	prefix := "rltest:" + hex.EncodeToString(b)
	l, err := New(c, prefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		iter := c.Scan(ctx, 0, prefix+":*", 100).Iterator()
		for iter.Next(ctx) {
			c.Del(ctx, iter.Val())
		}
	})
	return l, prefix
}

func TestContract(t *testing.T) {
	c := client(t)
	ratelimittest.Run(t, ratelimittest.Backend{
		New: func(t *testing.T) (ratelimit.Limiter, func(time.Duration)) {
			l, _ := newLimiter(t, c)
			return l, time.Sleep
		},
	})
}

func TestNewValidation(t *testing.T) {
	if _, err := New(nil, "p"); err == nil {
		t.Fatal("nil client accepted")
	}
	c := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	defer c.Close()
	for _, p := range []string{"", "has space", "x{y}", string(make([]byte, 65))} {
		if _, err := New(c, p); err == nil {
			t.Errorf("prefix %q accepted", p)
		}
	}
}

func TestKeyEncodingAndTTL(t *testing.T) {
	c := client(t)
	l, prefix := newLimiter(t, c)
	ctx := context.Background()
	p, _ := ratelimit.NewPolicy("ab", 10, time.Second, 2) // 2 tokens, 100ms each
	if _, err := l.Allow(ctx, p.Rule("c")); err != nil {
		t.Fatal(err)
	}
	key := prefix + ":2:abc"
	ttl, err := c.PTTL(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	// One token missing refills in 100ms; TTL is that plus at most 1ms rounding.
	if ttl <= 90*time.Millisecond || ttl > 102*time.Millisecond {
		t.Fatalf("TTL %v", ttl)
	}
	fields, err := c.HGetAll(ctx, key).Result()
	if err != nil || fields["w"] != "1000000" || fields["b"] != "1000000" {
		t.Fatalf("stored state %v %v", fields, err)
	}
	time.Sleep(150 * time.Millisecond)
	if n, _ := c.Exists(ctx, key).Result(); n != 0 {
		t.Fatal("full bucket must expire")
	}
}

func TestLargeValuesStayExact(t *testing.T) {
	c := client(t)
	l, prefix := newLimiter(t, c)
	ctx := context.Background()
	// Capacity close to 2^53 balance units.
	p, err := ratelimit.NewPolicy("big", 1, time.Minute, 150_000_000)
	if err != nil {
		t.Fatal(err)
	}
	res, err := l.Allow(ctx, p.Rule("k"))
	if err != nil || !res.Allowed || res.Remaining != 149_999_999 {
		t.Fatalf("%+v %v", res, err)
	}
	b, err := c.HGet(ctx, prefix+":3:bigk", "b").Result()
	if err != nil || b != "8999999940000000" {
		t.Fatalf("balance %q %v", b, err)
	}
}

func TestBackendErrors(t *testing.T) {
	c := client(t)
	l, prefix := newLimiter(t, c)
	ctx := context.Background()
	p, _ := ratelimit.NewPolicy("wrong", 1, time.Hour, 1)
	// A key of the wrong type makes HMGET fail inside the script.
	if err := c.Set(ctx, prefix+":5:wrongk", "x", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Allow(ctx, p.Rule("k")); !errors.Is(err, ratelimit.ErrBackend) {
		t.Fatalf("want ErrBackend, got %v", err)
	}
	dead := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1", DialTimeout: 100 * time.Millisecond, MaxRetries: -1})
	defer dead.Close()
	dl, _ := New(dead, "rltest")
	if _, err := dl.Allow(ctx, p.Rule("k")); !errors.Is(err, ratelimit.ErrBackend) {
		t.Fatalf("unreachable server: %v", err)
	}
}

func TestDecodeRejectsMalformedReplies(t *testing.T) {
	for _, raw := range [][]int64{
		{}, {1, 0, 0}, {0, 0, 1, 2, 0}, // bad length
		{2, 0, 0, 0}, {-1, 0, 1, 1, 0}, // allowed flag not 0 or 1
		{1, -1, 0, 0},                 // negative remaining
		{1, 0, 0, 1, 0}, {1, 0, 5, 0}, // allowed with denied rules or retry
		{0, 0, 1, 0}, {0, 0, 0, 1, 0}, {0, 0, -1, 1, 0}, // denied without denied rules or retry
		{0, 0, math.MaxInt64, 1, 0},       // retry overflows Duration
		{0, 0, 1, 1, 5}, {0, 0, 1, 1, -1}, // denied index out of range
		{0, 0, 1, 2, 1, 0}, {0, 0, 1, 2, 0, 0}, // denied indexes not strictly increasing
	} {
		if _, err := decode(raw, 2); !errors.Is(err, ratelimit.ErrBackend) {
			t.Errorf("%v accepted", raw)
		}
	}
	res, err := decode([]int64{0, 0, 1500, 2, 0, 1}, 2)
	if err != nil || res.Allowed || res.RetryAfter != 1500*time.Microsecond || len(res.Denied) != 2 {
		t.Fatalf("%+v %v", res, err)
	}
	res, err = decode([]int64{1, 3, 0, 0}, 2)
	if err != nil || !res.Allowed || res.Remaining != 3 || res.RetryAfter != 0 || res.Denied != nil {
		t.Fatalf("%+v %v", res, err)
	}
}
