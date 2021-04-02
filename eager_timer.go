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
	timer    *time.Timer
	nextTime time.Time
	manual   chan struct{}
	stopped  chan struct{}
	mu       *sync.Mutex
}

// New create an eager timer with maximun delay and callback function for
// scheduling.
func NewEagerTimer(maxDelay time.Duration, fn func(ctx context.Context)) (*EagerTimer, error) {
	if maxDelay <= 0 {
		return nil, errors.New("invalid maxDelay")
	} else if fn == nil {
		return nil, errors.New("invalid fn")
	}
	et := &EagerTimer{
		maxDelay: maxDelay,
		fn:       fn,
		manual:   make(chan struct{}),
		stopped:  make(chan struct{}),
		mu:       &sync.Mutex{},
	}
	et.nextTime = time.Now().Add(et.maxDelay)
	et.timer = time.NewTimer(et.maxDelay)
	go et.loopExec()
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

	now := time.Now()
	// 4 cases:
	// t <= 0
	// t > now + maxDelay
	if t.IsZero() || t.After(now.Add(et.maxDelay)) {
		et.mu.Lock()
		// only overwrite the existing timer if later than the given time.
		if now.Add(et.maxDelay).Before(et.nextTime) {
			et.nextLocked(et.maxDelay)
		}
		et.mu.Unlock()
	} else if t.Before(now) || t.Equal(now) {
		// t <= now: trigger immediately, and reschedule to max delay.
		et.mu.Lock()
		et.immediateLocked()
		et.mu.Unlock()
	} else {
		// now < t < now + maxDelay
		et.mu.Lock()
		// only overwrite the existing timer if later than the given time.
		if t.Before(et.nextTime) {
			et.scheduleLocked(t)
		}
		et.mu.Unlock()
	}
}

func (et *EagerTimer) nextLocked(d time.Duration) {
	if !et.timer.Stop() {
		select {
		case <-et.timer.C:
		default:
		}
	}
	et.nextTime = time.Now().Add(d)
	et.timer.Reset(d)
}

// TODO: if now & t is near?
func (et *EagerTimer) immediateLocked() {
	t := time.Now().Add(et.maxDelay)
	if !t.Equal(et.nextTime) {
		et.nextLocked(et.maxDelay)
	}
	// try to notify executor to schedule immediately, if not received,
	// the executor is busy now, do nothing.
	select {
	case et.manual <- struct{}{}:
	default:
	}
}

func (et *EagerTimer) scheduleLocked(t time.Time) {
	if !et.timer.Stop() {
		select {
		case <-et.timer.C:
		default:
		}
	}
	et.nextTime = t
	et.timer.Reset(time.Until(t))
}

func (et *EagerTimer) loopExec() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for {
		select {
		case <-et.stopped:
			return
		case <-et.manual:
			et.fn(ctx)
		case <-et.timer.C:
			et.mu.Lock()
			et.nextLocked(et.maxDelay)
			et.mu.Unlock()
			et.fn(ctx)
		}
	}
}
