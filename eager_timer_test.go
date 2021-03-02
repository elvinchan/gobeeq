package gobeeq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEagerTimer(t *testing.T) {
	t.Run("New", func(t *testing.T) {
		t.Parallel()

		flag := false
		et, err := NewEagerTimer(time.Millisecond, func() error {
			flag = true
			return nil
		})
		assert.NotNil(t, et)
		assert.NoError(t, err)
		assert.Eventually(t, func() bool {
			return flag
		}, time.Millisecond*500, time.Millisecond)
	})

	t.Run("Stop", func(t *testing.T) {
		t.Parallel()

		flag := false
		et, err := NewEagerTimer(time.Millisecond, func() error {
			flag = true
			return nil
		})
		assert.NotNil(t, et)
		assert.NoError(t, err)
		et.Stop()
		assert.Never(t, func() bool {
			return flag
		}, time.Millisecond*500, time.Millisecond)
		assert.PanicsWithValue(t, "bq: stop a stopped eager timer", func() {
			et.Stop()
		})
	})

	t.Run("Schedule", func(t *testing.T) {
		t.Parallel()

		now := time.Now()
		flag := false
		et, err := NewEagerTimer(time.Second, func() error {
			flag = true
			return nil
		})
		assert.NotNil(t, et)
		assert.NoError(t, err)
		nextTime := et.nextTime
		assert.True(t, now.Before(nextTime.Add(time.Second)))

		et.Schedule(time.Now().Add(time.Millisecond))
		assert.False(t, flag)
		assert.True(t, et.nextTime.Before(nextTime))
		assert.Eventually(t, func() bool {
			return flag
		}, time.Millisecond*500, time.Millisecond)
	})

	t.Run("ScheduleBeforeNow", func(t *testing.T) {
		t.Parallel()

		now := time.Now()
		flag := false
		et, err := NewEagerTimer(time.Second, func() error {
			flag = true
			return nil
		})
		assert.NotNil(t, et)
		assert.NoError(t, err)
		nextTime := et.nextTime
		assert.True(t, now.Before(nextTime.Add(time.Second)))
		// if not exist, the last line of this test would fail
		// because it maybe too fast that scheduling result is same as the
		// original nextTime
		assert.Never(t, func() bool {
			return flag
		}, time.Millisecond*200, time.Millisecond)

		et.Schedule(now)
		assert.Eventually(t, func() bool {
			return flag
		}, time.Millisecond*500, time.Millisecond)

		assert.True(t, nextTime.Before(et.nextTime))
	})
}
