// Package redis is the shared go-ratelimit backend on a single Redis server or
// a Sentinel-managed primary (Redis 6.2 or newer).
//
// Each Allow runs one Lua script that evaluates and commits every rule
// atomically using Redis server time. Redis Cluster is not supported for
// multi-rule calls because a script's keys must share a hash slot.
package redis

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"time"

	"github.com/ninepeach/go-ratelimit"
	"github.com/ninepeach/go-ratelimit/internal/bucket"
	goredis "github.com/redis/go-redis/v9"
)

//go:embed script.lua
var scriptSource string

var script = goredis.NewScript(scriptSource)

var prefixPattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,64}$`)

// Limiter is safe for concurrent use. It does not own the client.
type Limiter struct {
	client *goredis.Client
	prefix string
}

var _ ratelimit.Limiter = (*Limiter)(nil)

// New returns a Limiter storing buckets under prefix. The caller owns client
// and must close it after the Limiter is no longer used.
func New(client *goredis.Client, prefix string) (*Limiter, error) {
	if client == nil {
		return nil, errors.New("redis: nil client")
	}
	if !prefixPattern.MatchString(prefix) {
		return nil, errors.New("redis: prefix must be 1-64 bytes of [A-Za-z0-9_.:-]")
	}
	return &Limiter{client: client, prefix: prefix}, nil
}

// key encodes (group, key) unambiguously: the group length prefix makes
// "a"+"bc" and "ab"+"c" distinct for arbitrary bytes.
func (l *Limiter) key(group, key string) string {
	return l.prefix + ":" + strconv.Itoa(len(group)) + ":" + group + key
}

// Allow implements ratelimit.Limiter with one network round trip.
//
// If ctx ends while the script is in flight, the returned error wraps the
// context error and the outcome is indeterminate: the script may have
// committed. Allow never retries.
func (l *Limiter) Allow(ctx context.Context, rules ...ratelimit.Rule) (ratelimit.Result, error) {
	if err := ratelimit.Validate(rules); err != nil {
		return ratelimit.Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return ratelimit.Result{}, fmt.Errorf("redis: %w", err)
	}
	keys := make([]string, len(rules))
	args := make([]any, 0, 3*len(rules))
	for i, r := range rules {
		keys[i] = l.key(r.Policy.Group, r.Key)
		p := bucket.ParamsOf(r.Policy.Limit, r.Policy.Window, r.Policy.Burst)
		args = append(args, p.Limit, p.W, p.C)
	}
	raw, err := script.Run(ctx, l.client, keys, args...).Int64Slice()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ratelimit.Result{}, fmt.Errorf("redis: outcome indeterminate: %w", ctxErr)
		}
		return ratelimit.Result{}, fmt.Errorf("%w: %w", ratelimit.ErrBackend, err)
	}
	return decode(raw, len(rules))
}

// decode converts a script reply for n rules into a Result. It rejects any
// reply that violates the Result contract rather than returning a partially
// valid Result, so a script bug surfaces as ErrBackend.
func decode(raw []int64, n int) (ratelimit.Result, error) {
	malformed := func(reason string) (ratelimit.Result, error) {
		return ratelimit.Result{}, fmt.Errorf("%w: malformed script reply: %s", ratelimit.ErrBackend, reason)
	}
	if len(raw) < 4 || raw[3] < 0 || int64(len(raw)) != 4+raw[3] {
		return malformed("bad length")
	}
	allowed, remaining, retry := raw[0], raw[1], raw[2]
	switch {
	case allowed != 0 && allowed != 1:
		return malformed("allowed flag not 0 or 1")
	case remaining < 0:
		return malformed("negative remaining")
	case allowed == 1 && (retry != 0 || raw[3] != 0):
		return malformed("allowed reply with retry or denied rules")
	case allowed == 0 && (retry <= 0 || raw[3] == 0):
		return malformed("denied reply without retry or denied rules")
	case retry > int64(math.MaxInt64/time.Microsecond):
		return malformed("retry overflows time.Duration")
	}
	res := ratelimit.Result{Allowed: allowed == 1, Remaining: remaining, RetryAfter: time.Duration(retry) * time.Microsecond}
	for i, d := range raw[4:] {
		// Strictly increasing indexes are in range and unique, matching Memory.
		if d < 0 || d >= int64(n) || (i > 0 && d <= raw[4+i-1]) {
			return malformed("bad denied index")
		}
		res.Denied = append(res.Denied, int(d))
	}
	return res, nil
}
