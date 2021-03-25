package gobeeq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestJob(t *testing.T) {
	name := "test-job-basic"
	ctx := context.Background()
	queue, err := NewQueue(ctx, name, client)
	assert.NoError(t, err)

	data := struct {
		Foo string `json:"foo"`
	}{
		Foo: "bar",
	}
	db, _ := json.Marshal(data)
	j, err := queue.NewJob(string(db)).Save(ctx)
	assert.NoError(t, err)

	assert.Equal(t, "1", j.Id)
	assert.Equal(t, queue, j.queue)
	assert.Equal(t, `{"foo":"bar"}`, j.data)
	assert.Equal(t, StatusCreated, j.status)

	// options
	assert.Equal(t, Backoff{
		Strategy: BackoffImmediate,
	}, j.options.Backoff)
	assert.LessOrEqual(t, j.options.Timestamp, timeToUnixMS(time.Now()))
	assert.EqualValues(t, 0, j.options.Timeout)
	assert.EqualValues(t, 0, j.options.Delay)
	assert.Equal(t, 0, j.options.Retries)
}

func TestJobGet(t *testing.T) {
	name := "test-job-get"
	ctx := context.Background()
	queue, err := NewQueue(ctx, name, client)
	assert.NoError(t, err)

	times := 10

	ids := make(map[int]string)
	for i := 0; i < times; i++ {
		j, err := queue.NewJob(MockData(i)).Save(ctx)
		assert.NoError(t, err)
		ids[i] = j.Id

		cur := i
		t.Run(fmt.Sprintf("Single-%d", cur), func(t *testing.T) {
			t.Parallel()
			j, err := queue.GetJob(ctx, ids[cur])
			assert.NoError(t, err)
			assert.Equal(t, ids[cur], j.Id)
			assert.Equal(t, MockData(cur), j.data)
			assert.Equal(t, StatusCreated, j.status)
		})
	}

	t.Run("BatchInvalidStatus", func(t *testing.T) {
		t.Parallel()
		_, err = queue.GetJobs(ctx, StatusCreated, 0, 0, 0)
		assert.Error(t, ErrInvalidJobStatus, err)
	})

	t.Run("BatchWaitingFirst", func(t *testing.T) {
		t.Parallel()
		jobs, err := queue.GetJobs(ctx, StatusWaiting, 0, 0, 0)
		assert.NoError(t, err)
		assert.Len(t, jobs, 1)
		assert.Equal(t, ids[times-1], jobs[0].Id)
		assert.Equal(t, MockData(times-1), jobs[0].data)
		assert.Equal(t, StatusCreated, jobs[0].status)
	})

	t.Run("BatchWaitingPartial", func(t *testing.T) {
		t.Parallel()
		jobs, err := queue.GetJobs(ctx, StatusWaiting, 2, 4, 0)
		assert.NoError(t, err)
		assert.Len(t, jobs, 3)
		for i := range jobs {
			// reversed order
			assert.Equal(t, ids[times-1-2-i], jobs[i].Id)
			assert.Equal(t, MockData(times-1-2-i), jobs[i].data)
			assert.Equal(t, StatusCreated, jobs[i].status)
		}
	})

	t.Run("BatchWaitingAll", func(t *testing.T) {
		t.Parallel()
		jobs, err := queue.GetJobs(ctx, StatusWaiting, 0, 100, 0)
		assert.NoError(t, err)
		assert.Len(t, jobs, times)
		for i := range jobs {
			// reversed order
			assert.Equal(t, ids[times-1-i], jobs[i].Id)
			assert.Equal(t, MockData(times-1-i), jobs[i].data)
			assert.Equal(t, StatusCreated, jobs[i].status)
		}
	})
}

func TestJobGetProcessed(t *testing.T) {
	name := "test-job-get-processed"
	ctx := context.Background()
	queue, err := NewQueue(ctx, name, client)
	assert.NoError(t, err)

	times := 20

	ids := make(map[int]string)
	for i := 0; i < times; i++ {
		j, err := queue.NewJob(MockData(i)).Save(ctx)
		assert.NoError(t, err)
		ids[i] = j.Id
	}

	ch := make(chan struct{}, times)
	// success 1 ~ 5, fail 6 ~ 8, active 9 ~ 13, waiting 14 ~
	err = queue.ProcessConcurrently(4, func(ctx Context) error {
		t.Log("processing job:", ctx.GetId())
		defer func() {
			ch <- struct{}{}
		}()
		id, err := strconv.Atoi(ctx.GetId())
		assert.NoError(t, err)
		if id <= 5 {
			return nil
		} else if id <= 8 {
			return errors.New("fail as expected")
		}
		select {}
	})
	assert.NoError(t, err)

	for i := 0; i < 8; i++ {
		<-ch
	}
	time.Sleep(time.Second)

	t.Run("BatchWaitingFirst", func(t *testing.T) {
		t.Parallel()
		jobs, err := queue.GetJobs(ctx, StatusWaiting, 0, 0, 0)
		assert.NoError(t, err)
		assert.Len(t, jobs, 1)
		assert.Equal(t, ids[times-1], jobs[0].Id)
		assert.Equal(t, MockData(times-1), jobs[0].data)
		assert.Equal(t, StatusCreated, jobs[0].status)
	})

	t.Run("BatchWaitingPartial", func(t *testing.T) {
		t.Parallel()
		jobs, err := queue.GetJobs(ctx, StatusWaiting, 2, 4, 0)
		assert.NoError(t, err)
		assert.Len(t, jobs, 3)
		for i := range jobs {
			// reversed order
			assert.Equal(t, ids[times-1-2-i], jobs[i].Id)
			assert.Equal(t, MockData(times-1-2-i), jobs[i].data)
			assert.Equal(t, StatusCreated, jobs[i].status)
		}
	})

	t.Run("BatchWaitingAll", func(t *testing.T) {
		t.Parallel()
		jobs, err := queue.GetJobs(ctx, StatusWaiting, 0, 100, 0)
		assert.NoError(t, err)
		assert.Len(t, jobs, times-12)
		for i := range jobs {
			// reversed order
			assert.Equal(t, ids[times-1-i], jobs[i].Id)
			assert.Equal(t, MockData(times-1-i), jobs[i].data)
			assert.Equal(t, StatusCreated, jobs[i].status)
		}
	})

	t.Run("BatchActiveFirst", func(t *testing.T) {
		t.Parallel()
		jobs, err := queue.GetJobs(ctx, StatusActive, 0, 0, 0)
		assert.NoError(t, err)
		assert.Len(t, jobs, 1)
		assert.Equal(t, ids[12-1], jobs[0].Id)
		assert.Equal(t, MockData(12-1), jobs[0].data)
		assert.Equal(t, StatusCreated, jobs[0].status)
	})

	t.Run("BatchActivePartial", func(t *testing.T) {
		t.Parallel()
		jobs, err := queue.GetJobs(ctx, StatusActive, 1, 2, 0)
		assert.NoError(t, err)
		assert.Len(t, jobs, 2)
		for i := range jobs {
			// reversed order
			assert.Equal(t, ids[12-1-1-i], jobs[i].Id)
			assert.Equal(t, MockData(12-1-1-i), jobs[i].data)
			assert.Equal(t, StatusCreated, jobs[i].status)
		}
	})

	t.Run("BatchActiveAll", func(t *testing.T) {
		t.Parallel()
		jobs, err := queue.GetJobs(ctx, StatusActive, 0, 100, 0)
		assert.NoError(t, err)
		assert.Len(t, jobs, 4)
		for i := range jobs {
			// reversed order
			assert.Equal(t, ids[12-1-i], jobs[i].Id)
			assert.Equal(t, MockData(12-1-i), jobs[i].data)
			assert.Equal(t, StatusCreated, jobs[i].status)
		}
	})

	matchSucceeded := func(j Job) {
		var match bool
		for i := 0; i < 5; i++ {
			// random order
			if ids[i] == j.Id {
				match = true
				assert.Equal(t, MockData(i), j.data)
				assert.Equal(t, StatusSucceeded, j.status)
			}
		}
		assert.True(t, match)
	}

	t.Run("BatchSucceededFirst", func(t *testing.T) {
		t.Parallel()
		jobs, err := queue.GetJobs(ctx, StatusSucceeded, 0, 0, 0)
		assert.NoError(t, err)
		assert.Len(t, jobs, 1)
		matchSucceeded(jobs[0])
	})

	t.Run("BatchSucceededPartial", func(t *testing.T) {
		t.Parallel()
		jobs, err := queue.GetJobs(ctx, StatusSucceeded, 0, 0, 2)
		assert.NoError(t, err)
		assert.Len(t, jobs, 2)
		for _, job := range jobs {
			matchSucceeded(job)
		}
	})

	t.Run("BatchSucceededAll", func(t *testing.T) {
		t.Parallel()
		jobs, err := queue.GetJobs(ctx, StatusSucceeded, 0, 0, 100)
		assert.NoError(t, err)
		assert.Len(t, jobs, 5)
		for _, job := range jobs {
			matchSucceeded(job)
		}
	})

	matchFailed := func(j Job) {
		var match bool
		for i := 5; i < 8; i++ {
			// random order
			if ids[i] == j.Id {
				match = true
				assert.Equal(t, MockData(i), j.data)
				assert.Equal(t, StatusFailed, j.status)
			}
		}
		assert.True(t, match)
	}

	t.Run("BatchFailedFirst", func(t *testing.T) {
		t.Parallel()
		jobs, err := queue.GetJobs(ctx, StatusFailed, 0, 0, 0)
		assert.NoError(t, err)
		assert.Len(t, jobs, 1)
		matchFailed(jobs[0])
	})

	t.Run("BatchFailedPartial", func(t *testing.T) {
		t.Parallel()
		jobs, err := queue.GetJobs(ctx, StatusFailed, 0, 0, 2)
		assert.NoError(t, err)
		assert.Len(t, jobs, 2)
		for _, job := range jobs {
			matchFailed(job)
		}
	})

	t.Run("BatchFailedAll", func(t *testing.T) {
		t.Parallel()
		jobs, err := queue.GetJobs(ctx, StatusFailed, 0, 0, 100)
		assert.NoError(t, err)
		assert.Len(t, jobs, 3)
		for _, job := range jobs {
			matchFailed(job)
		}
	})
}

func TestJobTimeout(t *testing.T) {
	name := "test-job-timeout"
	ctx := context.Background()
	queue, err := NewQueue(ctx, name, client)
	assert.NoError(t, err)

	ch := make(chan struct{})
	err = queue.Process(func(ctx Context) error {
		t.Log("processing job:", ctx.GetId())
		time.Sleep(time.Second)
		ch <- struct{}{}
		return nil
	})
	assert.NoError(t, err)

	data := struct {
		Foo string `json:"foo"`
	}{
		Foo: "bar",
	}
	db, _ := json.Marshal(data)
	j := queue.NewJob(string(db)).Timeout(100)
	assert.Equal(t, 100*time.Millisecond, msToDuration(j.options.Timeout))
	assert.Equal(t, StatusCreated, j.status) // TODO: not really created?

	j, err = j.Save(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "1", j.Id)
	assert.Equal(t, StatusCreated, j.status)

	<-ch
	// get & check result
	j, err = Job{}.fromId(context.Background(), queue, j.Id)
	assert.NoError(t, err)
	assert.Equal(t, "1", j.Id)
	assert.Equal(t, string(db), j.data)
	assert.Equal(t, StatusFailed, j.status)
}
