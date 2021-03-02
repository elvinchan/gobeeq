package gobeeq

import (
	"encoding/json"
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
	Timestamp int64 `json:"timestamp"`
	Timeout   int64 `json:"timeout"` // ms
	Delay     int   `json:"delay"`
	Retries   int   `json:"retries"`
	// Stacktraces []interface{} `json:"stacktraces"`
	// Backoff struct {
	// 	Strategy string `json:"strategy"`
	// 	Delay    int    `json:"delay"`
	// } `json:"backoff"`
}

type Data struct {
	Data    string   `json:"data"`
	Options *Options `json:"options"`
	Status  `json:"status"`
}

func (j *Job) SetId(id string) *Job {
	j.Id = id
	return j
}

func (j *Job) Timeout(t int64) *Job {
	if t >= 0 {
		j.options.Timeout = t
	}
	return j
}

// Save save job and returns job pointer for chainable call.
func (j *Job) Save() (*Job, error) {
	data, err := j.ToData()
	if err != nil {
		return j, err
	}
	if j.options.Delay == 0 {
		res, err := j.queue.config.ScriptsProvider.AddJob().Run(
			j.queue.redis,
			[]string{
				keyId.use(j.queue),
				keyJobs.use(j.queue),
				keyWaiting.use(j.queue),
			},
			j.Id,
			data,
		).Result()
		if err != nil {
			return j, err
		}
		j.Id = res.(string)
	} else {
		// delay job
		res, err := j.queue.config.ScriptsProvider.AddDelayedJob().Run(
			j.queue.redis,
			[]string{
				keyId.use(j.queue),
				keyJobs.use(j.queue),
				keyDelayed.use(j.queue),
				keyEarlierDelayed.use(j.queue),
			},
			j.Id,
			data,
			j.options.Delay,
		).Result()
		if err != nil {
			return j, err
		}
		j.Id = res.(string)
		// TODO
		// this.queue._delayedTimer.schedule(this.options.delay);
	}
	return j, nil
}

func (Job) fromId(q *Queue, jobId string) (*Job, error) {
	data, err := q.redis.HGet(keyJobs.use(q), jobId).Result()
	if err != nil {
		return nil, err
	}
	return Job{}.fromData(q, jobId, data)
}

func (Job) fromIds(q *Queue, jobIds []string) ([]Job, error) {
	datas, err := q.redis.HMGet(keyJobs.use(q), jobIds...).Result()
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

func (j *Job) ToData() (string, error) {
	b, err := json.Marshal(Data{
		Data:    j.data,
		Options: j.options,
		Status:  j.status,
	})
	return string(b), err
}

func (j *Job) Remove() error {
	return j.queue.removeJob(j.Id)
}
