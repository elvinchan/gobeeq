package gobeeq

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-redis/redis/v8"
)

type Status string

const (
	StatusCreated   Status = "created"
	StatusSucceeded Status = "succeeded"
	StatusRetrying  Status = "retrying"
	StatusFailed    Status = "failed"
	StatusWaiting   Status = "waiting"
	StatusActive    Status = "active"
	StatusDelayed   Status = "delayed"
)

type Job struct {
	Id      string
	queue   *Queue
	data    string
	options *Options
	status  Status
}

type Options struct {
	Timestamp int64   `json:"timestamp"` // ms
	Timeout   int64   `json:"timeout"`   // ms
	Delay     int64   `json:"delay"`     // ms
	Retries   int     `json:"retries"`
	Backoff   Backoff `json:"backoff"`
}

func defaultOptions() *Options {
	return &Options{
		Backoff: Backoff{
			Strategy: BackoffImmediate,
		},
	}
}

type Data struct {
	Data    string   `json:"data"`
	Options *Options `json:"options"`
	Status  `json:"status"`
}

// SetId explicitly sets the ID of the job. If a job with the given ID already
// exists, the Job will not be created, and `job.id` will be set to `null`.
// This method can be used to run a job once for each of an external resource
// by passing that resource's ID. For instance, you might run the setup job
// for a user only once by setting the job ID to the ID of the user.
// Furthermore, when this feature is used with queue settings
// `removeOnSuccess: true` and `removeOnFailure: true`, it will allow that
// job to be re-run again, effectively ensuring that jobId will have a global
// concurrency of 1.
//
// Avoid passing a numeric job ID, as it may conflict with an auto-generated ID.
func (j *Job) SetId(id string) *Job {
	j.Id = id
	return j
}

// Sets how many times the job should be automatically retried in case of failure.
//
// Stored in `job.options.retries` and decremented each time the job is retried.
//
// Defaults to 0.
func (j *Job) Retries(n int) *Job {
	if n >= 0 {
		j.options.Retries = n
	}
	return j
}

// Set backoff strategy when retry for job fails. Defaults to immediate.
func (j *Job) Backoff(b Backoff) *Job {
	if b.Strategy != BackoffImmediate &&
		b.Strategy != BackoffFixed &&
		b.Strategy != BackoffExponential {
		panic("gobeeq: invalid backoff strategy")
	} else if b.Strategy != BackoffImmediate && b.Delay <= 0 {
		panic("gobeeq: invalid backoff delay")
	}
	j.options.Backoff = b
	return j
}

// DelayUntil set the job Delay until the given time. See the `Queue` settings
// section for information on controlling the activation of delayed jobs.
//
// Defaults to enqueueing the job for immediate processing.
func (j *Job) DelayUntil(t time.Time) *Job {
	j.options.Delay = t.UnixNano() / int64(time.Millisecond)
	return j
}

// Timeout set timeout milliseconds for a job.
// If the job's handler function takes longer than the timeout to call `done`,
// the worker assumes the job has failed and reports it as such (causing the
// job to retry if applicable).
//
// Defaults to no timeout.
func (j *Job) Timeout(t int64) *Job {
	if t >= 0 {
		j.options.Timeout = t
	}
	return j
}

// Save save job and returns job pointer for chainable call.
func (j *Job) Save(ctx context.Context) (*Job, error) {
	cmder := j.save(ctx, j.queue.redis)
	if err := cmder.Err(); err != nil {
		return nil, err
	}
	j.Id = cmder.String()
	if j.options.Delay != 0 && j.queue.settings.ActivateDelayedJobs {
		j.queue.delayedTimer.Schedule(unixMSToTime(j.options.Delay))
	}
	return j, nil
}

func (j *Job) save(ctx context.Context, cmd redis.Cmdable) *redis.Cmd {
	data := j.toData()
	if j.options.Delay != 0 {
		// delay job
		return runScript(
			ctx,
			j.queue.provider.AddDelayedJob().Hash(),
			cmd,
			[]string{
				keyId.use(j.queue),
				keyJobs.use(j.queue),
				keyDelayed.use(j.queue),
				keyEarlierDelayed.use(j.queue),
			},
			j.Id,
			data,
			j.options.Delay,
		)
	}
	return runScript(
		ctx,
		j.queue.provider.AddJob().Hash(),
		cmd,
		[]string{
			keyId.use(j.queue),
			keyJobs.use(j.queue),
			keyWaiting.use(j.queue),
		},
		j.Id,
		data,
	)
}

// Remove removes a job from the queue.
//
// This may have unintended side-effect, e.g. if the job is currently being
// processed by another worker, so only use this method when you know it's safe.
func (j *Job) Remove(ctx context.Context) error {
	return j.queue.RemoveJob(ctx, j.Id)
}

func (Job) fromId(ctx context.Context, q *Queue, jobId string) (*Job, error) {
	data, err := q.redis.HGet(ctx, keyJobs.use(q), jobId).Result()
	if err != nil {
		return nil, err
	}
	return Job{}.fromData(q, jobId, data)
}

func (Job) fromIds(ctx context.Context, q *Queue, jobIds []string) ([]Job, error) {
	datas, err := q.redis.HMGet(ctx, keyJobs.use(q), jobIds...).Result()
	if err != nil {
		return nil, err
	}
	var jobs []Job
	for i, d := range datas {
		if d == nil {
			continue
		}
		job, err := Job{}.fromData(q, jobIds[i], d.(string))
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, *job)
	}
	return jobs, nil
}

func (Job) fromData(queue *Queue, jobId string, data string) (*Job, error) {
	var d Data
	if err := json.Unmarshal([]byte(data), &d); err != nil {
		return nil, err
	}
	j := queue.newJobWithId(jobId, d.Data, d.Options)
	j.status = d.Status
	return j, nil
}

func (j *Job) toData() string {
	b, _ := json.Marshal(Data{
		Data:    j.data,
		Options: j.options,
		Status:  j.status,
	})
	return string(b)
}
