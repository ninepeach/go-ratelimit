// Package memory is the in-process go-ratelimit backend.
//
// One mutex guards all state. There are no goroutines or timers: buckets that
// have refilled to full are reclaimed lazily from a min-heap ordered by the
// time they become full, because a full bucket is indistinguishable from an
// absent one. Active buckets are never evicted; when the table is full after
// reclamation, Allow returns ratelimit.ErrCapacity.
//
// Limits are per process. Use the Redis backend when several processes must
// share one limit.
package memory

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ninepeach/go-ratelimit"
	"github.com/ninepeach/go-ratelimit/internal/bucket"
)

type id struct{ group, key string }

type entry struct {
	id     id
	state  bucket.State
	fullAt int64 // microseconds; reclaimable at or after this time
	index  int   // position in the heap
}

// Limiter is safe for concurrent use. The zero value is not usable; call New.
type Limiter struct {
	mu      sync.Mutex
	max     int
	buckets map[id]*entry
	byFull  fullHeap
	start   time.Time
	now     func() int64 // microseconds; replaced in tests
}

var _ ratelimit.Limiter = (*Limiter)(nil)

// New returns a Limiter holding at most maxBuckets buckets.
func New(maxBuckets int) (*Limiter, error) {
	if maxBuckets < 1 {
		return nil, errors.New("memory: maxBuckets must be positive")
	}
	l := &Limiter{max: maxBuckets, buckets: make(map[id]*entry), start: time.Now()}
	l.now = func() int64 { return time.Since(l.start).Microseconds() }
	return l, nil
}

// Len returns the number of live buckets, including ones not yet reclaimed.
func (l *Limiter) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}

// Allow implements ratelimit.Limiter. It runs in O(len(rules)) under the
// mutex plus amortized O(log n) reclamation, and allocates only for new
// buckets and the Denied slice.
//
// ctx is checked once, before the mutex is acquired. Waiting for the mutex
// cannot be interrupted, and a call that acquires it completes its decision
// even if ctx ends meanwhile; the wait is bounded by other callers' critical
// sections, which never block.
func (l *Limiter) Allow(ctx context.Context, rules ...ratelimit.Rule) (ratelimit.Result, error) {
	if err := ratelimit.Validate(rules); err != nil {
		return ratelimit.Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return ratelimit.Result{}, fmt.Errorf("memory: %w", err)
	}
	var (
		params [ratelimit.MaxRules]bucket.Params
		avail  [ratelimit.MaxRules]int64
		found  [ratelimit.MaxRules]*entry
	)
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	l.reclaim(now)

	missing := 0
	for i, r := range rules {
		p := bucket.ParamsOf(r.Policy.Limit, r.Policy.Window, r.Policy.Burst)
		params[i] = p
		e := l.buckets[id{r.Policy.Group, r.Key}]
		found[i] = e
		if e == nil {
			missing++
			avail[i] = bucket.Available(nil, p, now)
		} else {
			avail[i] = bucket.Available(&e.state, p, now)
		}
	}

	res := ratelimit.Result{Remaining: -1}
	var retry int64
	for i := range rules {
		if avail[i] < params[i].W {
			res.Denied = append(res.Denied, i)
			if w := bucket.Wait(avail[i], params[i]); w > retry {
				retry = w
			}
		}
	}
	if res.Denied != nil {
		for i := range rules {
			if tokens := avail[i] / params[i].W; res.Remaining < 0 || tokens < res.Remaining {
				res.Remaining = tokens
			}
		}
		res.RetryAfter = time.Duration(retry) * time.Microsecond
		return res, nil
	}
	if len(l.buckets)+missing > l.max {
		return ratelimit.Result{}, ratelimit.ErrCapacity
	}
	res.Allowed = true
	for i, r := range rules {
		p := params[i]
		b := avail[i] - p.W
		if tokens := b / p.W; res.Remaining < 0 || tokens < res.Remaining {
			res.Remaining = tokens
		}
		e := found[i]
		ts := now
		if e != nil && e.state.TS > ts {
			ts = e.state.TS
		}
		fullAt := ts + bucket.UntilFull(b, p)
		if e == nil {
			e = &entry{id: id{r.Policy.Group, r.Key}}
			e.state, e.fullAt = bucket.State{B: b, TS: ts, W: p.W}, fullAt
			l.buckets[e.id] = e
			heap.Push(&l.byFull, e)
			continue
		}
		e.state, e.fullAt = bucket.State{B: b, TS: ts, W: p.W}, fullAt
		heap.Fix(&l.byFull, e.index)
	}
	return res, nil
}

// reclaim drops buckets that are full at now. Each bucket is popped at most
// once per lifetime, so the cost is amortized over bucket creation.
//
// fullAt uses the policy of the bucket's last accepted request, matching the
// Redis backend's TTL. After a policy change, a bucket that was already full
// under the old policy therefore starts full under the new one.
func (l *Limiter) reclaim(now int64) {
	for len(l.byFull) > 0 && l.byFull[0].fullAt <= now {
		e := heap.Pop(&l.byFull).(*entry)
		delete(l.buckets, e.id)
	}
}

type fullHeap []*entry

func (h fullHeap) Len() int           { return len(h) }
func (h fullHeap) Less(i, j int) bool { return h[i].fullAt < h[j].fullAt }
func (h fullHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}
func (h *fullHeap) Push(x any) {
	e := x.(*entry)
	e.index = len(*h)
	*h = append(*h, e)
}
func (h *fullHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return e
}
