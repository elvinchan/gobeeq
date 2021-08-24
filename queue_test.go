package gobeeq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
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
				var v map[string]string
				err := ctx.BindData(&v)
				data, _ := json.Marshal(&v)
				assert.NoError(t, err)
				assert.Equal(t, mockData(i), json.RawMessage(data))
				time.Sleep(time.Duration(cases[i]) * 100 * time.Millisecond)
				ch <- struct{}{}
				return nil
			})
			assert.NoError(t, err)

			_, err = queue.CreateJob(mockData(i)).Save(ctx)
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
		j, err := queue.CreateJob(nil).Save(ctx)
		assert.NoError(t, err)
		assert.NotEmpty(t, j.Id)

		waitSync() // prevent case: job not captured by worker

		err = queue.Close()
		assert.NoError(t, err)
	})

	t.Run("Not processed", func(t *testing.T) {
		queue := newQueue()
		j, err := queue.CreateJob(nil).Save(ctx)
		assert.NoError(t, err)
		assert.NotEmpty(t, j.Id)

		waitSync() // prevent case: job not captured by worker

		err = queue.CloseTimeout(2 * time.Millisecond)
		assert.EqualError(t, err, "gobeeq: jobs are not processed after 2ms")
	})

	t.Run("Should stop timer", func(t *testing.T) {
		queue := newQueue()
		j, err := queue.CreateJob(nil).Save(ctx)
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

	emptyJSON, _ := json.Marshal(nil)
	j, err := queue.CreateJob(emptyJSON).Save(ctx)
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
			j := queue.CreateJob(mockData(i))
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
			j := queue.CreateJob(mockData(i))
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
			j := queue.CreateJob(mockData(i))
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

func TestCheckStalledJobs(t *testing.T) {
	t.Run("WithCheck", func(t *testing.T) {
		t.Parallel()

		name := "test-queue-stall-with-check"
		ctx := context.Background()
		interval := time.Millisecond * 100
		queue, err := NewQueue(ctx, name, client, WithStallInterval(interval))
		assert.NoError(t, err)

		go queue.CheckStalledJobs(interval / 2)

		_, err = queue.CreateJob(mockData(0)).Save(ctx)
		assert.NoError(t, err)

		workerClient := redis.NewClient(&redis.Options{
			Password: client.Options().Password,
			Addr:     client.Options().Addr,
			DB:       client.Options().DB,
		})
		workerQueue, err := NewQueue(ctx, name, workerClient, WithStallInterval(interval))
		assert.NoError(t, err)

		err = workerQueue.Process(func(ctx Context) error {
			<-ctx.Done()
			return nil
		})
		assert.NoError(t, err)

		waitSync() // prevent case: job not captured by worker

		// let worker queue stop prventing stall
		err = workerClient.Close()
		assert.NoError(t, err)
		// job would abort after getting close signal despite redis client
		// already closed
		err = workerQueue.Close()
		assert.NoError(t, err)

		times := int32(0)
		err = queue.Process(func(ctx Context) error {
			atomic.StoreInt32(&times, 1)
			return nil
		})
		assert.NoError(t, err)
		assert.Eventually(t, func() bool {
			return atomic.LoadInt32(&times) == 1
		}, time.Millisecond*400, time.Millisecond*100)
	})

	t.Run("WithoutCheck", func(t *testing.T) {
		t.Parallel()

		name := "test-queue-stall-without-check"
		ctx := context.Background()
		interval := time.Millisecond * 100
		queue, err := NewQueue(ctx, name, client, WithStallInterval(interval))
		assert.NoError(t, err)

		_, err = queue.CreateJob(mockData(0)).Save(ctx)
		assert.NoError(t, err)

		workerClient := redis.NewClient(&redis.Options{
			Password: client.Options().Password,
			Addr:     client.Options().Addr,
			DB:       client.Options().DB,
		})
		workerQueue, err := NewQueue(ctx, name, workerClient, WithStallInterval(interval))
		assert.NoError(t, err)

		err = workerQueue.Process(func(ctx Context) error {
			<-ctx.Done()
			return nil
		})
		assert.NoError(t, err)

		waitSync() // prevent case: job not captured by worker

		// let worker queue stop prventing stall
		err = workerClient.Close()
		assert.NoError(t, err)
		// job would abort after getting close signal despite redis client
		// already closed
		err = workerQueue.Close()
		assert.NoError(t, err)

		times := int32(0)
		err = queue.Process(func(ctx Context) error {
			atomic.StoreInt32(&times, 1)
			return nil
		})
		assert.NoError(t, err)
		assert.Never(t, func() bool {
			return atomic.LoadInt32(&times) == 1
		}, time.Millisecond*400, time.Millisecond*100)
	})
}

func TestCheckHealth(t *testing.T) {
	t.Parallel()

	name := "test-queue-check-health"
	ctx := context.Background()
	queue, err := NewQueue(ctx, name, client)
	assert.NoError(t, err)

	count := 10
	active := 2

	var jobs []Job
	for i := 0; i < count; i++ {
		j := queue.CreateJob(mockData(i))
		if i%2 == 0 {
			j.DelayUntil(time.Now().Add(time.Minute))
		}
		jobs = append(jobs, *j)
	}
	err = queue.SaveAll(ctx, jobs)
	assert.NoError(t, err)

	ch := make(chan struct{})

	err = queue.ProcessConcurrently(int64(active), func(ctx Context) error {
		t.Log("processing job:", ctx.GetId())
		<-ch
		if ctx.GetId() == "8" {
			return errors.New("fail as expected")
		}
		return nil
	})
	assert.NoError(t, err)

	waitSync()

	status, err := queue.CheckHealth(ctx)
	assert.NoError(t, err)
	assert.Len(t, status.Keys, 5)
	assert.Equal(t, int64(3), status.Keys[keyWaiting])
	assert.Equal(t, int64(2), status.Keys[keyActive])
	assert.Equal(t, int64(0), status.Keys[keySucceeded])
	assert.Equal(t, int64(0), status.Keys[keyFailed])
	assert.Equal(t, int64(5), status.Keys[keyDelayed])
	assert.Equal(t, int64(10), status.NewestJobId)

	close(ch)
	waitSync()

	status, err = queue.CheckHealth(ctx)
	assert.NoError(t, err)
	assert.Len(t, status.Keys, 5)
	assert.Equal(t, int64(0), status.Keys[keyWaiting])
	assert.Equal(t, int64(0), status.Keys[keyActive])
	assert.Equal(t, int64(4), status.Keys[keySucceeded])
	assert.Equal(t, int64(1), status.Keys[keyFailed])
	assert.Equal(t, int64(5), status.Keys[keyDelayed])
	assert.Equal(t, int64(10), status.NewestJobId)
}

func TestQueuePrefix(t *testing.T) {
	t.Parallel()

	name := "test-queue-prefix"
	prefix := "beequeue"
	ctx := context.Background()
	queue, err := NewQueue(ctx, name, client, WithPrefix(prefix))
	assert.NoError(t, err)

	_, err = queue.CreateJob(mockData(0)).Save(ctx)
	assert.NoError(t, err)

	keys, err := client.Keys(ctx, prefix+"*").Result()
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{
		"beequeue:test-queue-prefix:id",
		"beequeue:test-queue-prefix:jobs",
		"beequeue:test-queue-prefix:waiting",
	}, keys)
}

func TestQueueRemoveOnSuccess(t *testing.T) {
	t.Run("No", func(t *testing.T) {
		t.Parallel()
		name := "test-queue-remove-on-success-no"
		ctx := context.Background()
		queue, err := NewQueue(ctx, name, client)
		assert.NoError(t, err)

		err = queue.Process(func(ctx Context) error {
			return nil
		})
		assert.NoError(t, err)

		j, err := queue.CreateJob(mockData(0)).Save(ctx)
		assert.NoError(t, err)

		waitSync()
		j, err = queue.GetJob(ctx, j.Id)
		assert.NoError(t, err)
		assert.Equal(t, mockData(0), j.data)
	})

	t.Run("Yes", func(t *testing.T) {
		t.Parallel()
		name := "test-queue-remove-on-success-yes"
		ctx := context.Background()
		queue, err := NewQueue(ctx, name, client, WithRemoveOnSuccess(true))
		assert.NoError(t, err)

		err = queue.Process(func(ctx Context) error {
			return nil
		})
		assert.NoError(t, err)

		j, err := queue.CreateJob(mockData(0)).Save(ctx)
		assert.NoError(t, err)

		waitSync()
		_, err = queue.GetJob(ctx, j.Id)
		assert.EqualError(t, err, redis.Nil.Error())
	})
}

func TestQueueRemoveOnFailure(t *testing.T) {
	t.Run("No", func(t *testing.T) {
		t.Parallel()
		name := "test-queue-remove-on-failure-no"
		ctx := context.Background()
		queue, err := NewQueue(ctx, name, client)
		assert.NoError(t, err)

		err = queue.Process(func(ctx Context) error {
			t.Log("processing job:", ctx.GetId())
			return errors.New("fail as expected")
		})
		assert.NoError(t, err)

		j, err := queue.CreateJob(mockData(0)).Save(ctx)
		assert.NoError(t, err)

		waitSync()
		j, err = queue.GetJob(ctx, j.Id)
		assert.NoError(t, err)
		assert.Equal(t, mockData(0), j.data)
	})

	t.Run("Yes", func(t *testing.T) {
		t.Parallel()
		name := "test-queue-remove-on-failure-yes"
		ctx := context.Background()
		queue, err := NewQueue(ctx, name, client, WithRemoveOnFailure(true))
		assert.NoError(t, err)

		err = queue.Process(func(ctx Context) error {
			t.Log("processing job:", ctx.GetId())
			return errors.New("fail as expected")
		})
		assert.NoError(t, err)

		j, err := queue.CreateJob(mockData(0)).Save(ctx)
		assert.NoError(t, err)

		waitSync()
		_, err = queue.GetJob(ctx, j.Id)
		assert.EqualError(t, err, redis.Nil.Error())
	})

	t.Run("YesWithRetry", func(t *testing.T) {
		t.Parallel()
		name := "test-queue-remove-on-failure-yes-with-retry"
		ctx := context.Background()
		queue, err := NewQueue(ctx, name, client,
			WithActivateDelayedJobs(true, nil), WithRemoveOnFailure(true))
		assert.NoError(t, err)

		retries := 3
		ch := make(chan struct{})
		err = queue.Process(func(ctx Context) error {
			t.Log("processing job:", ctx.GetId())
			ch <- struct{}{}
			return errors.New("fail as expected")
		})
		assert.NoError(t, err)

		j, err := queue.CreateJob(mockData(0)).Retries(retries).Save(ctx)
		assert.NoError(t, err)

		for i := 0; i <= retries; i++ {
			<-ch
			waitSync() // wait for job finish and data sync to Redis
			j, err = queue.GetJob(ctx, j.Id)
			if i < retries {
				assert.NoError(t, err)
				assert.Equal(t, mockData(0), j.data)
			} else {
				assert.EqualError(t, err, redis.Nil.Error())
				assert.Nil(t, j)
			}
		}
	})
}

func TestQueueOnJobRetrying(t *testing.T) {
	t.Parallel()
	name := "test-queue-on-job-retrying"
	ctx := context.Background()

	ch := make(chan struct{})
	e := "fail as expected"
	id := "1"
	queue, err := NewQueue(ctx, name, client,
		WithSendEvents(true),
		WithOnJobRetrying(func(jobId string, err error) {
			assert.Equal(t, id, jobId)
			assert.EqualError(t, err, e)
			ch <- struct{}{}
		}),
	)
	assert.NoError(t, err)

	retries := 3
	_, err = queue.CreateJob(nil).SetId(id).Retries(retries).Save(ctx)
	assert.NoError(t, err)

	err = queue.Process(func(ctx Context) error {
		t.Log("processing job:", ctx.GetId())
		return errors.New(e)
	})
	assert.NoError(t, err)

	for i := 0; i < retries; i++ {
		<-ch
	}
}

func TestQueueOnJobFailed(t *testing.T) {
	t.Parallel()
	name := "test-queue-on-job-failed"
	ctx := context.Background()

	ch := make(chan struct{})
	e := "fail as expected"
	id := "1"
	queue, err := NewQueue(ctx, name, client,
		WithSendEvents(true),
		WithOnJobFailed(func(jobId string, err error) {
			assert.Equal(t, id, jobId)
			assert.EqualError(t, err, e)
			ch <- struct{}{}
		}),
	)
	assert.NoError(t, err)

	_, err = queue.CreateJob(nil).SetId(id).Save(ctx)
	assert.NoError(t, err)

	err = queue.Process(func(ctx Context) error {
		t.Log("processing job:", ctx.GetId())
		return errors.New(e)
	})
	assert.NoError(t, err)

	<-ch
}
