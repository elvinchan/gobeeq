package gobeeq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestJob(t *testing.T) {
	t.Parallel()

	name := "test-job"
	ctx := context.Background()
	queue, err := NewQueue(ctx, name, client)
	assert.NoError(t, err)

	j, err := queue.CreateJob(mockData(0)).Save(ctx)
	assert.NoError(t, err)

	assert.Equal(t, "1", j.Id)
	assert.Equal(t, queue, j.queue)
	assert.Equal(t, mockData(0), j.data)
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
	t.Parallel()

	// note: assume count of every kind of status job >= 3
	cases := []struct {
		succeeded int
		failed    int
		delayed   int
		active    int
		waiting   int
	}{
		{
			succeeded: 5,
			failed:    3,
			delayed:   5,
			active:    4,
			waiting:   3,
		},
		{
			succeeded: 3,
			failed:    4,
			delayed:   3,
			active:    6,
			waiting:   5,
		},
		{
			succeeded: 6,
			failed:    5,
			delayed:   4,
			active:    3,
			waiting:   4,
		},
	}

	for i := range cases {
		i := i
		t.Run(fmt.Sprintf("Case-%d", i+1), func(t *testing.T) {
			t.Parallel()
			c := cases[i]
			name := fmt.Sprintf("test-job-get-%d", i)
			ctx := context.Background()
			queue, err := NewQueue(ctx, name, client)
			assert.NoError(t, err)

			total := c.succeeded + c.failed + c.delayed + c.active + c.waiting
			succeededIdx := 1
			failedIdx := succeededIdx + c.succeeded
			delayedIdx := failedIdx + c.failed
			activeIdx := delayedIdx + c.delayed
			waitingIdx := activeIdx + c.active

			ids := make(map[int]string)
			for i := 1; i <= total; i++ {
				j := queue.CreateJob(mockData(i))
				if i >= delayedIdx && i < activeIdx {
					j.DelayUntil(time.Now().Add(time.Duration(i) * time.Hour))
				}
				j, err = j.Save(ctx)
				assert.NoError(t, err)
				ids[i] = j.Id
			}

			ch := make(chan struct{}, total)
			err = queue.ProcessConcurrently(int64(c.active), func(ctx Context) error {
				t.Log("processing job:", ctx.GetId())
				defer func() {
					ch <- struct{}{}
				}()
				id, err := strconv.Atoi(ctx.GetId())
				assert.NoError(t, err)
				if id < failedIdx { // succeeded
					return nil
				} else if id < delayedIdx { // failed
					return errors.New("fail as expected")
				}
				select {}
			})
			assert.NoError(t, err)

			for i := 0; i < c.succeeded+c.failed; i++ {
				<-ch
			}
			waitSync()

			t.Run("Single", func(t *testing.T) {
				for _, id := range ids {
					job, err := queue.GetJob(ctx, id)
					assert.NoError(t, err)
					assert.Equal(t, job.Id, id)

					jobId, _ := strconv.Atoi(id)
					if jobId < succeededIdx+c.succeeded {
						assert.Equal(t, job.status, StatusSucceeded)
					} else if jobId < failedIdx+c.failed {
						assert.Equal(t, job.status, StatusFailed)
					} else if jobId < delayedIdx+c.delayed {
						assert.Equal(t, job.status, StatusCreated)
					} else if jobId < activeIdx+c.active {
						assert.Equal(t, job.status, StatusCreated)
					} else if jobId < waitingIdx+c.waiting {
						assert.Equal(t, job.status, StatusCreated)
					}
				}
			})

			t.Run("BatchWaitingFirst", func(t *testing.T) {
				jobs, err := queue.GetJobs(ctx, StatusWaiting, 0, 0, 0)
				assert.NoError(t, err)
				assert.Len(t, jobs, 1)
				assert.Equal(t, ids[total], jobs[0].Id)
				assert.Equal(t, mockData(total), jobs[0].data)
				assert.Equal(t, StatusCreated, jobs[0].status)
			})

			t.Run("BatchWaitingPartial", func(t *testing.T) {
				jobs, err := queue.GetJobs(ctx, StatusWaiting, 0, 1, 0)
				assert.NoError(t, err)
				assert.Len(t, jobs, 2)
				for i := range jobs {
					// desc order
					assert.Equal(t, ids[total-i], jobs[i].Id)
					assert.Equal(t, mockData(total-i), jobs[i].data)
					assert.Equal(t, StatusCreated, jobs[i].status)
				}
			})

			t.Run("BatchWaitingAll", func(t *testing.T) {
				jobs, err := queue.GetJobs(ctx, StatusWaiting, 0, 100, 0)
				assert.NoError(t, err)
				assert.Len(t, jobs, c.waiting)
				for i := range jobs {
					// desc order
					assert.Equal(t, ids[total-i], jobs[i].Id)
					assert.Equal(t, mockData(total-i), jobs[i].data)
					assert.Equal(t, StatusCreated, jobs[i].status)
				}
			})

			t.Run("BatchActiveFirst", func(t *testing.T) {
				jobs, err := queue.GetJobs(ctx, StatusActive, 0, 0, 0)
				assert.NoError(t, err)
				assert.Len(t, jobs, 1)
				assert.Equal(t, ids[waitingIdx-1], jobs[0].Id)
				assert.Equal(t, mockData(waitingIdx-1), jobs[0].data)
				assert.Equal(t, StatusCreated, jobs[0].status)
			})

			t.Run("BatchActivePartial", func(t *testing.T) {
				jobs, err := queue.GetJobs(ctx, StatusActive, 1, 2, 0)
				assert.NoError(t, err)
				assert.Len(t, jobs, 2)
				for i := range jobs {
					// desc order
					assert.Equal(t, ids[waitingIdx-1-1-i], jobs[i].Id)
					assert.Equal(t, mockData(waitingIdx-1-1-i), jobs[i].data)
					assert.Equal(t, StatusCreated, jobs[i].status)
				}
			})

			t.Run("BatchActiveAll", func(t *testing.T) {
				jobs, err := queue.GetJobs(ctx, StatusActive, 0, 100, 0)
				assert.NoError(t, err)
				assert.Len(t, jobs, c.active)
				for i := range jobs {
					// desc order
					assert.Equal(t, ids[waitingIdx-1-i], jobs[i].Id)
					assert.Equal(t, mockData(waitingIdx-1-i), jobs[i].data)
					assert.Equal(t, StatusCreated, jobs[i].status)
				}
			})

			t.Run("BatchDelayedFirst", func(t *testing.T) {
				jobs, err := queue.GetJobs(ctx, StatusDelayed, 0, 0, 0)
				assert.NoError(t, err)
				assert.Len(t, jobs, 1)
				assert.Equal(t, ids[delayedIdx], jobs[0].Id)
				assert.Equal(t, mockData(delayedIdx), jobs[0].data)
				assert.Equal(t, StatusCreated, jobs[0].status)
			})

			t.Run("BatchDelayedPartial", func(t *testing.T) {
				jobs, err := queue.GetJobs(ctx, StatusDelayed, 1, 2, 0)
				assert.NoError(t, err)
				assert.Len(t, jobs, 2)
				for i := range jobs {
					// asc order
					assert.Equal(t, ids[delayedIdx+1+i], jobs[i].Id)
					assert.Equal(t, mockData(delayedIdx+1+i), jobs[i].data)
					assert.Equal(t, StatusCreated, jobs[i].status)
				}
			})

			t.Run("BatchDelayedAll", func(t *testing.T) {
				jobs, err := queue.GetJobs(ctx, StatusDelayed, 0, 100, 0)
				assert.NoError(t, err)
				assert.Len(t, jobs, c.delayed)
				for i := range jobs {
					// asc order
					assert.Equal(t, ids[delayedIdx+i], jobs[i].Id)
					assert.Equal(t, mockData(delayedIdx+i), jobs[i].data)
					assert.Equal(t, StatusCreated, jobs[i].status)
				}
			})

			matchSucceeded := func(j Job) {
				var match bool
				for i := succeededIdx; i < succeededIdx+c.succeeded; i++ {
					// random order
					if ids[i] == j.Id {
						match = true
						assert.Equal(t, mockData(i), j.data)
						assert.Equal(t, StatusSucceeded, j.status)
					}
				}
				assert.True(t, match)
			}

			t.Run("BatchSucceededFirst", func(t *testing.T) {
				jobs, err := queue.GetJobs(ctx, StatusSucceeded, 0, 0, 0)
				assert.NoError(t, err)
				assert.Len(t, jobs, 1)
				matchSucceeded(jobs[0])
			})

			t.Run("BatchSucceededPartial", func(t *testing.T) {
				jobs, err := queue.GetJobs(ctx, StatusSucceeded, 0, 0, 2)
				assert.NoError(t, err)
				assert.Len(t, jobs, 2)
				for _, job := range jobs {
					matchSucceeded(job)
				}
			})

			t.Run("BatchSucceededAll", func(t *testing.T) {
				jobs, err := queue.GetJobs(ctx, StatusSucceeded, 0, 0, 100)
				assert.NoError(t, err)
				assert.Len(t, jobs, c.succeeded)
				for _, job := range jobs {
					matchSucceeded(job)
				}
			})

			matchFailed := func(j Job) {
				var match bool
				for i := failedIdx; i < failedIdx+c.failed; i++ {
					// random order
					if ids[i] == j.Id {
						match = true
						assert.Equal(t, mockData(i), j.data)
						assert.Equal(t, StatusFailed, j.status)
					}
				}
				assert.True(t, match)
			}

			t.Run("BatchFailedFirst", func(t *testing.T) {
				jobs, err := queue.GetJobs(ctx, StatusFailed, 0, 0, 0)
				assert.NoError(t, err)
				assert.Len(t, jobs, 1)
				matchFailed(jobs[0])
			})

			t.Run("BatchFailedPartial", func(t *testing.T) {
				jobs, err := queue.GetJobs(ctx, StatusFailed, 0, 0, 2)
				assert.NoError(t, err)
				assert.Len(t, jobs, 2)
				for _, job := range jobs {
					matchFailed(job)
				}
			})

			t.Run("BatchFailedAll", func(t *testing.T) {
				jobs, err := queue.GetJobs(ctx, StatusFailed, 0, 0, 100)
				assert.NoError(t, err)
				assert.Len(t, jobs, c.failed)
				for _, job := range jobs {
					matchFailed(job)
				}
			})
		})
	}
}

func TestJobGetSpecial(t *testing.T) {
	t.Parallel()

	name := "test-job-get-special"
	ctx := context.Background()
	queue, err := NewQueue(ctx, name, client)
	assert.NoError(t, err)

	times := 4

	ids := make(map[int]string)
	for i := 0; i < times; i++ {
		j, err := queue.CreateJob(mockData(i)).Save(ctx)
		assert.NoError(t, err)
		ids[i] = j.Id

		cur := i
		t.Run(fmt.Sprintf("Single-%d", cur), func(t *testing.T) {
			j, err := queue.GetJob(ctx, ids[cur])
			assert.NoError(t, err)
			assert.Equal(t, ids[cur], j.Id)
			assert.Equal(t, mockData(cur), j.data)
			assert.Equal(t, StatusCreated, j.status)
		})
	}

	t.Run("NotExist", func(t *testing.T) {
		j, err := queue.GetJob(ctx, "-1")
		assert.Equal(t, "redis: nil", err.Error())
		assert.Nil(t, j)
	})

	t.Run("BatchInvalidStatus", func(t *testing.T) {
		_, err = queue.GetJobs(ctx, StatusCreated, 0, 0, 0)
		assert.Error(t, ErrInvalidJobStatus, err)
	})

	t.Run("BatchNegativeRange", func(t *testing.T) {
		jobs, err := queue.GetJobs(ctx, StatusWaiting, -1, -1, 0)
		assert.NoError(t, err)
		assert.Len(t, jobs, 1)
		assert.Equal(t, mockData(times-1), jobs[0].data)
		assert.Equal(t, StatusCreated, jobs[0].status)
	})

	ch := make(chan struct{}, times)
	err = queue.Process(func(ctx Context) error {
		t.Log("processing job:", ctx.GetId())
		defer func() {
			ch <- struct{}{}
		}()
		return nil
	})
	assert.NoError(t, err)

	for i := 0; i < times; i++ {
		<-ch
	}
	waitSync()

	t.Run("BatchNegativeSize", func(t *testing.T) {
		jobs, err := queue.GetJobs(ctx, StatusSucceeded, 0, 0, -1)
		assert.NoError(t, err)
		assert.Len(t, jobs, 1)
		assert.Equal(t, StatusSucceeded, jobs[0].status)
	})

	t.Run("BatchNotExist", func(t *testing.T) {
		jobs, err := queue.GetJobs(ctx, StatusFailed, 0, 0, 2)
		assert.NoError(t, err)
		assert.Len(t, jobs, 0)
	})
}

func TestJobTimeout(t *testing.T) {
	t.Parallel()

	name := "test-job-timeout"
	ctx := context.Background()
	queue, err := NewQueue(ctx, name, client)
	assert.NoError(t, err)

	ch := make(chan struct{})
	times := int32(3)
	err = queue.Process(func(ctx Context) error {
		t.Log("processing job:", ctx.GetId())
		<-ctx.Done()
		if ctx.GetId() == "1" {
			ch <- struct{}{}
		} else {
			times := atomic.AddInt32(&times, -1)
			if times == 0 {
				ch <- struct{}{}
			}
		}
		return nil
	})
	assert.NoError(t, err)

	t.Run("Normal", func(t *testing.T) {
		j := queue.CreateJob(mockData(0)).Timeout(100)
		assert.Equal(t, 100*time.Millisecond, msToDuration(j.options.Timeout))
		assert.Equal(t, StatusCreated, j.status)

		j, err = j.Save(ctx)
		assert.NoError(t, err)
		assert.Equal(t, "1", j.Id)
		assert.Equal(t, StatusCreated, j.status)

		<-ch
		waitSync()

		// get & check result
		j, err = Job{}.fromId(context.Background(), queue, j.Id)
		assert.NoError(t, err)
		assert.Equal(t, "1", j.Id)
		assert.Equal(t, mockData(0), j.data)
		assert.Equal(t, StatusFailed, j.status)
	})

	t.Run("Retry", func(t *testing.T) {
		times := atomic.LoadInt32(&times)
		j := queue.CreateJob(mockData(1)).Timeout(100).Retries(int(times) - 1)
		assert.Equal(t, 100*time.Millisecond, msToDuration(j.options.Timeout))
		assert.Equal(t, StatusCreated, j.status)

		j, err = j.Save(ctx)
		assert.NoError(t, err)
		assert.Equal(t, "2", j.Id)
		assert.Equal(t, StatusCreated, j.status)

		<-ch
		waitSync()

		// get & check result
		j, err = Job{}.fromId(context.Background(), queue, j.Id)
		assert.NoError(t, err)
		assert.Equal(t, "2", j.Id)
		assert.Equal(t, mockData(1), j.data)
		assert.Equal(t, StatusFailed, j.status)
	})
}

func TestJobBackoff(t *testing.T) {
	t.Parallel()

	name := "test-job-backoff"
	ctx := context.Background()
	queue, err := NewQueue(ctx, name, client,
		WithActivateDelayedJobs(true, nil),
	)
	assert.NoError(t, err)

	records := make(map[string][]time.Time)
	ch := make(map[string]chan struct{})
	times := 4

	err = queue.Process(func(ctx Context) error {
		t.Log("processing job:", ctx.GetId())
		id := ctx.GetId()
		records[id] = append(records[id], time.Now())
		if len(records[id]) == times {
			close(ch[id])
			return nil
		}
		return errors.New("expected fail")
	})
	assert.NoError(t, err)

	t.Run("Immediate", func(t *testing.T) {
		j := queue.CreateJob(mockData(0)).Retries(times - 1).Backoff(Backoff{
			Strategy: BackoffImmediate,
		})
		assert.Equal(t, Backoff{
			Strategy: BackoffImmediate,
		}, j.options.Backoff)
		assert.Equal(t, StatusCreated, j.status)
		ch["1"] = make(chan struct{})

		j, err = j.Save(ctx)
		assert.NoError(t, err)
		assert.Equal(t, "1", j.Id)
		assert.Equal(t, StatusCreated, j.status)

		select {
		case <-ch[j.Id]:
		case <-time.After(time.Second * 8):
			assert.FailNow(t, "timeout waiting for job finish")
		}
		waitSync()

		// check time records
		for i := 1; i < len(records[j.Id]); i++ {
			diff := records[j.Id][i].Sub(records[j.Id][i-1])
			assert.LessOrEqual(t, diff.Milliseconds(), int64(100))
		}
	})

	t.Run("Fixed", func(t *testing.T) {
		j := queue.CreateJob(mockData(0)).Retries(times - 1).Backoff(Backoff{
			Strategy: BackoffFixed,
			Delay:    1000,
		})
		assert.Equal(t, Backoff{
			Strategy: BackoffFixed,
			Delay:    1000,
		}, j.options.Backoff)
		assert.Equal(t, StatusCreated, j.status)
		ch["2"] = make(chan struct{})

		j, err = j.Save(ctx)
		assert.NoError(t, err)
		assert.Equal(t, "2", j.Id)
		assert.Equal(t, StatusCreated, j.status)

		select {
		case <-ch[j.Id]:
		case <-time.After(time.Second * 8):
			assert.FailNow(t, "timeout waiting for job finish")
		}
		waitSync()

		// check time records
		for i := 1; i < len(records[j.Id]); i++ {
			diff := records[j.Id][i].Sub(records[j.Id][i-1])
			assert.LessOrEqual(t, diff.Milliseconds(), int64(1000+100))
			assert.GreaterOrEqual(t, diff.Milliseconds(), int64(1000))
		}
	})

	t.Run("Exponential", func(t *testing.T) {
		j := queue.CreateJob(mockData(0)).Retries(times - 1).Backoff(Backoff{
			Strategy: BackoffExponential,
			Delay:    500,
		})
		assert.Equal(t, Backoff{
			Strategy: BackoffExponential,
			Delay:    500,
		}, j.options.Backoff)
		assert.Equal(t, StatusCreated, j.status)
		ch["3"] = make(chan struct{})

		j, err = j.Save(ctx)
		assert.NoError(t, err)
		assert.Equal(t, "3", j.Id)
		assert.Equal(t, StatusCreated, j.status)

		select {
		case <-ch[j.Id]:
		case <-time.After(time.Second * 8):
			assert.FailNow(t, "timeout waiting for job finish")
		}
		waitSync()

		// check time records
		for i := 1; i < len(records[j.Id]); i++ {
			diff := records[j.Id][i].Sub(records[j.Id][i-1])
			assert.LessOrEqual(t, diff.Milliseconds(), int64(500*math.Pow(2, float64(i-1))+100))
			assert.GreaterOrEqual(t, diff.Milliseconds(), int64(500*math.Pow(2, float64(i-1))))
		}
	})
}

func TestJobRemove(t *testing.T) {
	t.Parallel()

	name := "test-job-remove"
	ctx := context.Background()
	queue, err := NewQueue(ctx, name, client)
	assert.NoError(t, err)

	job1, err := queue.CreateJob(mockData(0)).Save(ctx)
	assert.NoError(t, err)

	job2, err := queue.CreateJob(mockData(1)).Save(ctx)
	assert.NoError(t, err)

	job1, err = queue.GetJob(ctx, job1.Id)
	assert.NoError(t, err)
	assert.Equal(t, StatusCreated, job1.status)
	assert.Equal(t, mockData(0), job1.data)

	job2, err = queue.GetJob(ctx, job2.Id)
	assert.NoError(t, err)
	assert.Equal(t, StatusCreated, job2.status)
	assert.Equal(t, mockData(1), job2.data)

	// remove 1
	err = job1.Remove(ctx)
	assert.NoError(t, err)

	job1, err = queue.GetJob(ctx, job1.Id)
	assert.Equal(t, redis.Nil, err)
	assert.Nil(t, job1)

	job2, err = queue.GetJob(ctx, job2.Id)
	assert.NoError(t, err)
	assert.Equal(t, StatusCreated, job2.status)
	assert.Equal(t, mockData(1), job2.data)

	// process
	err = queue.Process(func(ctx Context) error {
		t.Log("processing job:", ctx.GetId())
		return nil
	})
	assert.NoError(t, err)

	waitSync() // prevent case: job not captured by worker

	job2, err = queue.GetJob(ctx, job2.Id)
	assert.NoError(t, err)
	assert.Equal(t, StatusSucceeded, job2.status)
	assert.Equal(t, mockData(1), job2.data)

	// remove 2
	err = job2.Remove(ctx)
	assert.NoError(t, err)

	job2, err = queue.GetJob(ctx, job2.Id)
	assert.Equal(t, redis.Nil, err)
	assert.Nil(t, job2)
}
