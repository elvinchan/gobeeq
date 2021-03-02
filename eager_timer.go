package gobeeq

import (
	"errors"
	"sync"
	"time"
)

// A timer that will eagerly replace an existing timer with a sooner one.
// Refuses to set a later timer, or a timer beyond the given maximum delay.
type EagerTimer struct {
	maxDelay time.Duration
	fn       func()
	timer    *time.Timer
	nextTime time.Time
	manual   chan struct{}
	stopped  chan struct{}
	mu       *sync.Mutex
}

// New create an eager timer with maximun delay and callback function for
// scheduling.
func NewEagerTimer(maxDelay time.Duration, fn func() error) (*EagerTimer, error) {
	if maxDelay <= 0 {
		return nil, errors.New("invalid maxDelay")
	} else if fn == nil {
		return nil, errors.New("invalid fn")
	}
	et := &EagerTimer{
		maxDelay: maxDelay,
		fn: func() {
			if err := fn(); err != nil {
				// TODO: retry
			}
		},
		manual:  make(chan struct{}),
		stopped: make(chan struct{}),
		mu:      &sync.Mutex{},
	}
	et.scheduleLocked(time.Now().Add(et.maxDelay), false)
	return et, nil
}

// Stop stops an eager timer. panic if use for a stopped timer.
func (et *EagerTimer) Stop() {
	select {
	case <-et.stopped:
		panic("bq: stop a stopped eager timer")
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
	var immediate bool
	// 4 cases:
	// t <= 0
	// t > now + maxDelay
	if t.IsZero() || t.After(now.Add(et.maxDelay)) {
		t = now.Add(et.maxDelay)
	} else if t.Before(now) || t.Equal(now) {
		// t <= now: trigger immediately, and reschedule to max delay.
		immediate = true
		t = now.Add(et.maxDelay)
		time.Sleep(time.Millisecond * 20)
	}
	// now < t < now + maxDelay

	et.mu.Lock()
	et.scheduleLocked(t, immediate)
	et.mu.Unlock()
}

func (et *EagerTimer) scheduleLocked(t time.Time, immediate bool) {
	if et.timer == nil {
		et.timer = time.NewTimer(time.Until(t))
		et.nextTime = t
		go et.loopExec(immediate)
		return
	}
	// only overwrite the existing timer if later than the given time.
	if immediate && !t.Equal(et.nextTime) || t.Before(et.nextTime) {
		if !et.timer.Stop() {
			select {
			case <-et.timer.C:
			default:
			}
		}
		et.nextTime = t
		et.timer.Reset(time.Until(t))
	}
	if immediate {
		// try to notify executor to schedule immediately, if not received,
		// the executor is busy now, do nothing.
		select {
		case et.manual <- struct{}{}:
		default:
		}
	}
}

func (et *EagerTimer) loopExec(immediate bool) {
	if immediate {
		et.fn()
	}
	for {
		select {
		case <-et.stopped:
			return
		case <-et.manual:
			et.fn()
		case <-et.timer.C:
			et.fn()
		}
	}
}
