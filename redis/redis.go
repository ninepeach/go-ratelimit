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

func decode(raw []int64, n int) (ratelimit.Result, error) {
	if len(raw) < 4 || raw[3] < 0 || int64(len(raw)) != 4+raw[3] {
		return ratelimit.Result{}, fmt.Errorf("%w: malformed script reply", ratelimit.ErrBackend)
	}
	res := ratelimit.Result{Allowed: raw[0] == 1, Remaining: raw[1], RetryAfter: time.Duration(raw[2]) * time.Microsecond}
	for _, d := range raw[4:] {
		if d < 0 || d >= int64(n) {
			return ratelimit.Result{}, fmt.Errorf("%w: malformed script reply", ratelimit.ErrBackend)
		}
		res.Denied = append(res.Denied, int(d))
	}
	if res.Allowed == (len(res.Denied) > 0) {
		return ratelimit.Result{}, fmt.Errorf("%w: malformed script reply", ratelimit.ErrBackend)
	}
	return res, nil
}
