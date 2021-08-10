package gobeeq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const testTimeout = 30 * time.Second

func TestQueue(t *testing.T) {
	t.Parallel()
	name := "test-queue"
	ctx := context.Background()
	queue, err := NewQueue(ctx, name, client)
	assert.NoError(t, err)
	assert.Equal(t, name, queue.name)
}

func TestQueueProcess(t *testing.T) {
	ctx := context.Background()
	cases := []int{1, 3, 5}
	for i := range cases {
		i := i
		t.Run(fmt.Sprintf("Process delay: %d", i), func(t *testing.T) {
			t.Parallel()

			name := fmt.Sprintf("test-queue-process-%d", i)
			queue, err := NewQueue(ctx, name, client)
			assert.NoError(t, err)
			assert.Equal(t, name, queue.name)

			ch := make(chan struct{})
			err = queue.Process(func(ctx Context) error {
				t.Log("processing job:", ctx.GetId())
				assert.Equal(t, mockData(i), ctx.GetData())
				time.Sleep(time.Duration(cases[i]) * 100 * time.Millisecond)
				ch <- struct{}{}
				return nil
			})
			assert.NoError(t, err)

			_, err = queue.NewJob(mockData(i)).Save(ctx)
			assert.NoError(t, err)

			select {
			case <-ch:
			case <-time.After(testTimeout):
				t.Fatalf("job was not processed")
			}
		})
	}
}

func TestQueueRunning(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	queue, err := NewQueue(ctx, "test-queue-running", client)
	assert.NoError(t, err)

	assert.Equal(t, true, queue.IsRunning())

	// TODO pause
}

func TestQueueClose(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var i int
	newQueue := func() *Queue {
		i++
		queue, err := NewQueue(
			ctx,
			fmt.Sprintf("test-queue-close-%d", i),
			client,
			WithActivateDelayedJobs(true, nil),
		)
		assert.NoError(t, err)

		err = queue.Process(func(ctx Context) error {
			t.Log("processing job:", ctx.GetId())
			time.Sleep(2 * time.Second)
			return nil
		})
		assert.NoError(t, err)
		return queue
	}

	t.Run("Wait for finish", func(t *testing.T) {
		queue := newQueue()
		j, err := queue.NewJob("data").Save(ctx)
		assert.NoError(t, err)
		assert.NotEmpty(t, j.Id)

		waitSync() // prevent case: job not captured by worker

		err = queue.Close()
		assert.NoError(t, err)
	})

	t.Run("Not processed", func(t *testing.T) {
		queue := newQueue()
		j, err := queue.NewJob("data").Save(ctx)
		assert.NoError(t, err)
		assert.NotEmpty(t, j.Id)

		waitSync() // prevent case: job not captured by worker

		err = queue.CloseTimeout(2 * time.Millisecond)
		assert.EqualError(t, err, "gobeeq: jobs are not processed after 2ms")
	})

	t.Run("Should stop timer", func(t *testing.T) {
		queue := newQueue()
		j, err := queue.NewJob("data").Save(ctx)
		assert.NoError(t, err)
		assert.NotEmpty(t, j.Id)

		waitSync() // prevent case: job not captured by worker

		err = queue.CloseTimeout(2 * time.Millisecond)
		assert.EqualError(t, err, "gobeeq: jobs are not processed after 2ms")
		assert.PanicsWithValue(t, "gobeeq: stop a stopped eager timer", func() {
			queue.delayedTimer.Stop()
		})
	})
}

func TestQueueDestroy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queue, err := NewQueue(ctx, "test-queue-destroy", client)
	assert.NoError(t, err)

	j, err := queue.NewJob("data").Save(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, j.Id)

	err = queue.Destroy(ctx)
	assert.NoError(t, err)

	v, err := client.Keys(ctx, queue.keyPrefix()+"*").Result()
	assert.NoError(t, err)
	assert.Empty(t, v)
}

func TestQueueSaveAll(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		t.Parallel()

		name := "test-queue-save-all-normal"
		ctx := context.Background()
		queue, err := NewQueue(ctx, name, client)
		assert.NoError(t, err)

		count := 10

		var jobs []Job
		for i := 0; i < count; i++ {
			j := queue.NewJob(mockData(i))
			jobs = append(jobs, *j)
		}
		err = queue.SaveAll(ctx, jobs)
		assert.NoError(t, err)

		results, err := queue.GetJobs(ctx, StatusWaiting, 0, 100, 0)
		assert.NoError(t, err)
		assert.Len(t, results, count)

		for i, r := range results {
			j := jobs[count-i-1]

			assert.Equal(t, j.Id, r.Id)
			assert.Equal(t, j.queue, r.queue)
			assert.Equal(t, j.data, r.data)

			assert.Equal(t, j.options.Backoff, r.options.Backoff)
			assert.Equal(t, j.options.Timeout, r.options.Timeout)
			assert.Equal(t, j.options.Delay, r.options.Delay)
			assert.Equal(t, j.options.Retries, r.options.Retries)
		}
	})

	t.Run("Delayed", func(t *testing.T) {
		t.Parallel()

		name := "test-queue-save-all-delayed"
		ctx := context.Background()
		queue, err := NewQueue(ctx, name, client)
		assert.NoError(t, err)

		count := 10

		var jobs []Job
		for i := 0; i < count; i++ {
			j := queue.NewJob(mockData(i))
			if i%2 == 0 {
				j.DelayUntil(time.Now().Add(time.Minute))
			}
			jobs = append(jobs, *j)
		}
		err = queue.SaveAll(ctx, jobs)
		assert.NoError(t, err)

		results, err := queue.GetJobs(ctx, StatusWaiting, 0, 100, 0)
		assert.NoError(t, err)
		assert.Len(t, results, count/2)

		for i, r := range results {
			j := jobs[count-2*i-1]

			assert.Equal(t, j.Id, r.Id)
			assert.Equal(t, j.queue, r.queue)
			assert.Equal(t, j.data, r.data)

			assert.Equal(t, j.options.Backoff, r.options.Backoff)
			assert.Equal(t, j.options.Timeout, r.options.Timeout)
			assert.Equal(t, j.options.Delay, r.options.Delay)
			assert.Equal(t, j.options.Retries, r.options.Retries)
		}

		results, err = queue.GetJobs(ctx, StatusDelayed, 0, 100, 0)
		assert.NoError(t, err)
		assert.Len(t, results, count/2)

		for i, r := range results {
			j := jobs[i*2]

			assert.Equal(t, j.Id, r.Id)
			assert.Equal(t, j.queue, r.queue)
			assert.Equal(t, j.data, r.data)

			assert.Equal(t, j.options.Backoff, r.options.Backoff)
			assert.Equal(t, j.options.Timeout, r.options.Timeout)
			assert.Equal(t, j.options.Delay, r.options.Delay)
			assert.Equal(t, j.options.Retries, r.options.Retries)
		}
	})

	t.Run("Mix", func(t *testing.T) {
		t.Parallel()

		name := "test-queue-save-all-mix"
		ctx := context.Background()
		queue, err := NewQueue(ctx, name, client)
		assert.NoError(t, err)

		count := 10

		var (
			jobs        []Job
			saveAllJobs []Job
		)
		for i := 0; i < count; i++ {
			j := queue.NewJob(mockData(i))
			if i%2 == 0 {
				j.DelayUntil(time.Now().Add(time.Minute))
			}
			if i < count/2 {
				j, err = j.Save(ctx)
				assert.NoError(t, err)
				jobs = append(jobs, *j)
			} else {
				saveAllJobs = append(saveAllJobs, *j)
			}
		}
		err = queue.SaveAll(ctx, saveAllJobs)
		assert.NoError(t, err)
		jobs = append(jobs, saveAllJobs...)

		results, err := queue.GetJobs(ctx, StatusWaiting, 0, 100, 0)
		assert.NoError(t, err)
		assert.Len(t, results, count/2)

		for i, r := range results {
			j := jobs[count-2*i-1]

			assert.Equal(t, j.Id, r.Id)
			assert.Equal(t, j.queue, r.queue)
			assert.Equal(t, j.data, r.data)

			assert.Equal(t, j.options.Backoff, r.options.Backoff)
			assert.Equal(t, j.options.Timeout, r.options.Timeout)
			assert.Equal(t, j.options.Delay, r.options.Delay)
			assert.Equal(t, j.options.Retries, r.options.Retries)
		}

		results, err = queue.GetJobs(ctx, StatusDelayed, 0, 100, 0)
		assert.NoError(t, err)
		assert.Len(t, results, count/2)

		for i, r := range results {
			j := jobs[i*2]

			assert.Equal(t, j.Id, r.Id)
			assert.Equal(t, j.queue, r.queue)
			assert.Equal(t, j.data, r.data)

			assert.Equal(t, j.options.Backoff, r.options.Backoff)
			assert.Equal(t, j.options.Timeout, r.options.Timeout)
			assert.Equal(t, j.options.Delay, r.options.Delay)
			assert.Equal(t, j.options.Retries, r.options.Retries)
		}
	})
}
