package gobeeq

import (
	"context"
	"encoding/json"
	"time"
)

type Context interface {
	context.Context

	GetId() string
	BindData(i interface{}) error
	SetResult(v interface{}) error
	ReportProgress(p interface{}) error
}

var _ Context = (*jobContext)(nil)

type jobContext struct {
	ctx    context.Context
	job    *Job
	result json.RawMessage
}

func (c *jobContext) GetId() string {
	return c.job.Id
}

// Bind binds the data of job into provided type `i`.
func (c *jobContext) BindData(i interface{}) error {
	return json.Unmarshal(c.job.data.(json.RawMessage), i)
}

// SetResult set the result of job. It must be JSON serializable.
func (c *jobContext) SetResult(v interface{}) error {
	result, err := json.Marshal(v)
	if err != nil {
		return err
	}
	c.result = json.RawMessage(result)
	return nil
}

// ReportProgress send event `progress` with the payload `p`. The payload
// must be JSON serializable.
func (c *jobContext) ReportProgress(p interface{}) error {
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	msg := Message{
		Id:    c.job.Id,
		Event: EventProgress,
		Data:  data,
	}
	v, _ := json.Marshal(msg)
	c.job.queue.redis.Publish(c, keyEvents.use(c.job.queue), string(v)) // TODO: handle error?
	return nil
}

func (c *jobContext) Deadline() (deadline time.Time, ok bool) {
	return c.ctx.Deadline()
}

func (c *jobContext) Done() <-chan struct{} {
	return c.ctx.Done()
}

func (c *jobContext) Err() error {
	return c.ctx.Err()
}

func (c *jobContext) Value(key interface{}) interface{} {
	return c.ctx.Value(key)
}
