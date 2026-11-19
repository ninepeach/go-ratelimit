// Package bucket implements the exact integer Token Bucket arithmetic shared by
// every backend. The Redis Lua script mirrors these functions; keep them in sync.
//
// Units: one token equals W balance units, where W is the window in
// microseconds. Refill adds Limit units per microsecond. All values stay below
// 2^53 under ratelimit.Validate, so the same arithmetic is exact in Lua.
package bucket

import "time"

// Params are a validated policy expressed in balance units.
type Params struct {
	Limit int64 // units added per microsecond
	W     int64 // units per token (window in microseconds)
	C     int64 // capacity: Burst * W
}

func ParamsOf(limit int64, window time.Duration, burst int64) Params {
	w := int64(window / time.Microsecond)
	return Params{Limit: limit, W: w, C: burst * w}
}

// State is a stored bucket.
type State struct {
	B  int64 // balance in units
	TS int64 // reference time in microseconds
	W  int64 // window the balance is expressed in
}

func ceilDiv(a, b int64) int64 { return (a + b - 1) / b }

// Available returns the balance at now. A nil state means the bucket is absent (full).
func Available(s *State, p Params, now int64) int64 {
	if s == nil {
		return p.C
	}
	b := s.B
	if s.W != p.W {
		// Window changed: keep whole tokens only. Compare before multiplying so
		// tokens*W cannot exceed capacity (and therefore 2^53).
		if tokens := b / s.W; tokens >= p.C/p.W {
			b = p.C
		} else {
			b = tokens * p.W
		}
	}
	if b > p.C {
		b = p.C
	}
	dt := now - s.TS
	if dt < 0 {
		dt = 0
	}
	if need := p.C - b; need > 0 {
		if maxDT := ceilDiv(need, p.Limit); dt > maxDT {
			dt = maxDT
		}
		b += p.Limit * dt
		if b > p.C {
			b = p.C
		}
	}
	return b
}

// Wait returns microseconds until a balance of b holds one token.
func Wait(b int64, p Params) int64 {
	if b >= p.W {
		return 0
	}
	return ceilDiv(p.W-b, p.Limit)
}

// UntilFull returns microseconds until a balance of b reaches capacity.
func UntilFull(b int64, p Params) int64 {
	if b >= p.C {
		return 0
	}
	return ceilDiv(p.C-b, p.Limit)
}
