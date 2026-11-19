package ratelimit_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ninepeach/go-ratelimit"
)

func TestNewPolicyValidation(t *testing.T) {
	if _, err := ratelimit.NewPolicy("auth", 60, time.Minute, 10); err != nil {
		t.Fatal(err)
	}
	for name, p := range map[string]ratelimit.Policy{
		"empty group":     {Group: "", Limit: 1, Window: time.Second, Burst: 1},
		"long group":      {Group: strings.Repeat("g", 65), Limit: 1, Window: time.Second, Burst: 1},
		"zero limit":      {Group: "g", Limit: 0, Window: time.Second, Burst: 1},
		"zero burst":      {Group: "g", Limit: 1, Window: time.Second, Burst: 0},
		"tiny window":     {Group: "g", Limit: 1, Window: time.Microsecond * 999, Burst: 1},
		"sub-us window":   {Group: "g", Limit: 1, Window: time.Second + 1, Burst: 1},
		"precision bound": {Group: "g", Limit: 1, Window: 24 * time.Hour, Burst: 105_000},
		"negative window": {Group: "g", Limit: 1, Window: -time.Second, Burst: 1},
		"huge limit":      {Group: "g", Limit: 1 << 62, Window: time.Second, Burst: 1},
	} {
		if _, err := ratelimit.NewPolicy(p.Group, p.Limit, p.Window, p.Burst); !errors.Is(err, ratelimit.ErrInvalidRule) {
			t.Errorf("%s: want ErrInvalidRule, got %v", name, err)
		}
	}
	if _, err := ratelimit.NewPolicy("g", 1, 24*time.Hour, 104_000); err != nil {
		t.Fatalf("bound edge must pass: %v", err)
	}
}

func TestValidate(t *testing.T) {
	p, _ := ratelimit.NewPolicy("auth", 1, time.Second, 1)
	q, _ := ratelimit.NewPolicy("other", 1, time.Second, 1)
	if err := ratelimit.Validate(nil); !errors.Is(err, ratelimit.ErrInvalidRule) {
		t.Fatal("empty call accepted")
	}
	many := make([]ratelimit.Rule, ratelimit.MaxRules+1)
	for i := range many {
		many[i] = p.Rule(string(rune('a' + i)))
	}
	if err := ratelimit.Validate(many); !errors.Is(err, ratelimit.ErrInvalidRule) {
		t.Fatal("too many rules accepted")
	}
	if err := ratelimit.Validate(many[:ratelimit.MaxRules]); err != nil {
		t.Fatalf("MaxRules must pass: %v", err)
	}
	for name, rules := range map[string][]ratelimit.Rule{
		"empty key":  {p.Rule("")},
		"long key":   {p.Rule(strings.Repeat("k", 257))},
		"bad policy": {{Key: "k"}},
	} {
		if err := ratelimit.Validate(rules); !errors.Is(err, ratelimit.ErrInvalidRule) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := ratelimit.Validate([]ratelimit.Rule{p.Rule("k"), p.Rule("k")}); !errors.Is(err, ratelimit.ErrDuplicateRule) {
		t.Fatal("duplicate accepted")
	}
	if err := ratelimit.Validate([]ratelimit.Rule{p.Rule("k"), q.Rule("k")}); err != nil {
		t.Fatalf("same key in different groups must pass: %v", err)
	}
}
