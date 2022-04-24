package gobeeq

import (
	"context"
	"errors"
	"sync"
	"time"
)

// A timer that will eagerly replace an existing timer with a sooner one.
// Refuses to set a later timer, or a timer beyond the given maximum delay.
type EagerTimer struct {
	maxDelay time.Duration
	fn       func(ctx context.Context)
	nextTime time.Time // slightly earlier than actual trigger time
	manual   chan struct{}
	stopped  chan struct{}
	mu       sync.Mutex
}

// New create an eager timer with maximun delay and callback function for
// scheduling.
func NewEagerTimer(maxDelay time.Duration, fn func(ctx context.Context),
) (*EagerTimer, error) {
	if maxDelay <= 0 {
		return nil, errors.New("gobeeq: invalid maxDelay")
	} else if fn == nil {
		return nil, errors.New("gobeeq: invalid fn")
	}
	et := &EagerTimer{
		maxDelay: maxDelay,
		fn:       fn,
		manual:   make(chan struct{}, 1),
		stopped:  make(chan struct{}),
		nextTime: time.Now().Add(maxDelay),
	}
	t := time.NewTimer(et.maxDelay)
	go et.loopExec(t)
	return et, nil
}

// Stop stops an eager timer. panic if use for a stopped timer.
func (et *EagerTimer) Stop() {
	select {
	case <-et.stopped:
		panic("gobeeq: stop a stopped eager timer")
	default:
		close(et.stopped)
	}
}

// Schedule set next scheduling time. It can't be late then maxDelay from now.
func (et *EagerTimer) Schedule(t time.Time) {
	select {
	case <-et.stopped:
		return
	default:
	}

	et.mu.Lock()
	defer et.mu.Unlock()
	now := time.Now()
	// 4 cases:
	if t.IsZero() || t.After(now.Add(et.maxDelay)) {
		// t <= 0
		// t > now + maxDelay
		t = now.Add(et.maxDelay)
	} else if !t.After(now) {
		// t <= now: trigger immediately, and reschedule to max delay.
		t = now
		// } else {
		// now < t < now + maxDelay
	}
	et.doSchedule(t)
}

func (et *EagerTimer) doSchedule(t time.Time) {
	// only overwrite the existing timer if later than the given time.
	if t.Before(et.nextTime) {
		et.nextTime = t
		select {
		case et.manual <- struct{}{}:
		default:
		}
	}
}

func (et *EagerTimer) loopExec(t *time.Timer) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for {
		select {
		case <-et.stopped:
			t.Stop()
			return
		case <-et.manual:
			if !t.Stop() {
				<-t.C
			}
			immediate := false
			et.mu.Lock()
			now := time.Now()
			if et.nextTime.After(now) {
				t.Reset(et.nextTime.Sub(now))
			} else {
				immediate = true
				// trigger immediate
				et.nextTime = now.Add(et.maxDelay)
				t.Reset(et.maxDelay)
			}
			et.mu.Unlock()
			if immediate {
				et.fn(ctx)
			}
		case <-t.C:
			et.mu.Lock()
			et.nextTime = time.Now().Add(et.maxDelay)
			t.Reset(et.maxDelay)
			et.mu.Unlock()
			et.fn(ctx)
		}
	}
}
