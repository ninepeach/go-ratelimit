// Package ratelimit defines a transport-independent Token Bucket rate limiter
// contract with atomic multi-rule decisions. Backends live in subpackages:
// memory (in-process) and the separate redis module.
//
// See docs/design.md for the full semantics.
package ratelimit

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrInvalidRule reports an empty call, too many rules, or a rule outside the validation limits.
	ErrInvalidRule = errors.New("ratelimit: invalid rule")
	// ErrDuplicateRule reports the same (Group, Key) twice in one call.
	ErrDuplicateRule = errors.New("ratelimit: duplicate rule")
	// ErrCapacity reports that an in-process backend has no room for new buckets.
	ErrCapacity = errors.New("ratelimit: bucket capacity exceeded")
	// ErrBackend reports a failure of a remote backend.
	ErrBackend = errors.New("ratelimit: backend failure")
)

// Validation limits. See docs/design.md, Section 4.
const (
	MaxRules     = 16
	MaxGroupLen  = 64
	MaxKeyLen    = 256
	MinWindow    = time.Millisecond
	maxSafeUnits = 1<<53 - 1
)

// Policy is a Token Bucket configuration: Limit tokens are replenished per
// Window, and at most Burst tokens are available at once. Group namespaces
// buckets; groups are independent, not hierarchical.
type Policy struct {
	Group  string
	Limit  int64
	Window time.Duration
	Burst  int64
}

// NewPolicy returns a validated Policy. Define policies once at startup so
// that misconfiguration fails before traffic.
func NewPolicy(group string, limit int64, window time.Duration, burst int64) (Policy, error) {
	p := Policy{Group: group, Limit: limit, Window: window, Burst: burst}
	if err := p.validate(); err != nil {
		return Policy{}, err
	}
	return p, nil
}

// Rule binds the policy to a caller-supplied key.
func (p Policy) Rule(key string) Rule { return Rule{Key: key, Policy: p} }

func (p Policy) validate() error {
	if p.Group == "" || len(p.Group) > MaxGroupLen || p.Limit < 1 || p.Burst < 1 ||
		p.Window < MinWindow || p.Window%time.Microsecond != 0 {
		return ErrInvalidRule
	}
	w := int64(p.Window / time.Microsecond)
	// Burst*w + Limit must stay exactly representable as an IEEE double.
	if p.Burst > (maxSafeUnits-p.Limit)/w || p.Limit > maxSafeUnits {
		return ErrInvalidRule
	}
	return nil
}

// Rule is one dimension of a decision, such as "per IP" or "per account".
type Rule struct {
	Key    string
	Policy Policy
}

// Result is the outcome of a successful Allow call, including denials.
type Result struct {
	Allowed bool
	// Remaining is the minimum whole-token count across all rules, after
	// deduction when allowed and without deduction when denied.
	Remaining int64
	// RetryAfter is zero when allowed; otherwise the longest wait, over denied
	// rules, until each holds one token assuming no other traffic.
	RetryAfter time.Duration
	// Denied holds indexes into the rules argument that lacked a token.
	Denied []int
}

// Limiter makes atomic multi-rule decisions: either every rule has a token and
// one is taken from each, or nothing is consumed.
//
// A denial returns (Result{Allowed: false}, nil). On error the Result is the
// zero value.
type Limiter interface {
	Allow(ctx context.Context, rules ...Rule) (Result, error)
}

// Validate checks a call's rules against the limits in docs/design.md. Backends
// call it before touching state; it does not allocate.
func Validate(rules []Rule) error {
	if len(rules) == 0 || len(rules) > MaxRules {
		return ErrInvalidRule
	}
	for i, r := range rules {
		if r.Key == "" || len(r.Key) > MaxKeyLen {
			return ErrInvalidRule
		}
		if err := r.Policy.validate(); err != nil {
			return err
		}
		for j := 0; j < i; j++ {
			if rules[j].Key == r.Key && rules[j].Policy.Group == r.Policy.Group {
				return ErrDuplicateRule
			}
		}
	}
	return nil
}
