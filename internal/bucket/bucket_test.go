package bucket

import (
	"testing"
	"time"
)

func TestArithmetic(t *testing.T) {
	p := ParamsOf(60, time.Minute, 10) // 1 token/s, capacity 10
	if got := Available(nil, p, 0); got != p.C {
		t.Fatalf("absent bucket must be full: %d", got)
	}
	s := &State{B: 0, TS: 0, W: p.W}
	if got := Available(s, p, 500_000); got != p.W/2 {
		t.Fatalf("half a second refills half a token: %d", got)
	}
	if got := Wait(p.W/2, p); got != 500_000 {
		t.Fatalf("wait: %d", got)
	}
	if got := Available(s, p, 1<<50); got != p.C {
		t.Fatalf("long idle must cap at capacity without overflow: %d", got)
	}
	if got := Available(&State{B: p.C, TS: 100, W: p.W}, p, 0); got != p.C {
		t.Fatalf("clock going backwards must not change balance: %d", got)
	}
	if got := UntilFull(0, p); got != 10_000_000 {
		t.Fatalf("until full: %d", got)
	}
}

func TestPolicyChange(t *testing.T) {
	old := ParamsOf(60, time.Minute, 10)
	s := &State{B: 3*old.W + old.W/2, TS: 0, W: old.W} // 3.5 tokens
	smaller := ParamsOf(60, time.Minute, 2)
	if got := Available(s, smaller, 0); got != smaller.C {
		t.Fatalf("burst shrink must clamp: %d", got)
	}
	big := &State{B: 150_000_000 * 60_000_000, TS: 0, W: 60_000_000} // max burst at 1m
	week := ParamsOf(1, 7*24*time.Hour, 14_000)
	if got := Available(big, week, 0); got != week.C {
		t.Fatalf("window growth must clamp without overflow: %d", got)
	}
	hourly := ParamsOf(60, time.Hour, 10)
	if got := Available(s, hourly, 0); got != 3*hourly.W {
		t.Fatalf("window change keeps whole tokens: %d", got)
	}
}
