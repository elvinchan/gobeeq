package gobeeq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEagerTimer(t *testing.T) {
	t.Run("New", func(t *testing.T) {
		t.Parallel()

		flag := uint32(0)
		et, err := NewEagerTimer(time.Millisecond*100, func(_ context.Context) {
			atomic.StoreUint32(&flag, 1)
		})
		assert.NotNil(t, et)
		assert.NoError(t, err)
		assert.Eventually(t, func() bool {
			return atomic.LoadUint32(&flag) == 1
		}, time.Millisecond*500, time.Millisecond*100)
	})

	t.Run("Stop", func(t *testing.T) {
		t.Parallel()

		flag := uint32(0)
		et, err := NewEagerTimer(time.Millisecond*100, func(_ context.Context) {
			atomic.StoreUint32(&flag, 1)
		})
		assert.NotNil(t, et)
		assert.NoError(t, err)
		et.Stop()
		assert.Never(t, func() bool {
			return atomic.LoadUint32(&flag) == 1
		}, time.Millisecond*500, time.Millisecond*100)
		assert.PanicsWithValue(t, "gobeeq: stop a stopped eager timer", func() {
			et.Stop()
		})
	})

	t.Run("Schedule", func(t *testing.T) {
		t.Parallel()

		now := time.Now()
		flag := uint32(0)
		et, err := NewEagerTimer(time.Second, func(_ context.Context) {
			atomic.StoreUint32(&flag, 1)
		})
		assert.NotNil(t, et)
		assert.NoError(t, err)
		nextTime := et.nextTime
		assert.True(t, now.Before(nextTime.Add(time.Second)))

		et.Schedule(time.Now().Add(time.Millisecond))
		assert.Equal(t, uint32(0), flag)
		et.mu.Lock()
		assert.True(t, et.nextTime.Before(nextTime))
		et.mu.Unlock()
		assert.Eventually(t, func() bool {
			return atomic.LoadUint32(&flag) == 1
		}, time.Millisecond*500, time.Millisecond*100)
	})

	t.Run("ScheduleBeforeNow", func(t *testing.T) {
		t.Parallel()

		now := time.Now()
		flag := uint32(0)
		et, err := NewEagerTimer(time.Second, func(_ context.Context) {
			atomic.StoreUint32(&flag, 1)
		})
		assert.NotNil(t, et)
		assert.NoError(t, err)
		nextTime := et.nextTime
		assert.True(t, now.Before(nextTime.Add(time.Second)))
		// if not exist, the last line of this test would fail
		// because it maybe too fast that scheduling result is same as the
		// original nextTime
		assert.Never(t, func() bool {
			return atomic.LoadUint32(&flag) == 1
		}, time.Millisecond*200, time.Millisecond*100)

		et.Schedule(now)
		assert.Eventually(t, func() bool {
			return atomic.LoadUint32(&flag) == 1
		}, time.Millisecond*500, time.Millisecond*100)

		et.mu.Lock()
		assert.True(t, nextTime.Before(et.nextTime))
		et.mu.Unlock()
	})
}
