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
	// TODO
	// ReportProgress
}

var _ Context = (*jobContext)(nil)

type jobContext struct {
	ctx    context.Context
	id     string
	data   json.RawMessage
	result json.RawMessage
}

func (c *jobContext) GetId() string {
	return c.id
}

// Bind binds the data of job into provided type `i`.
func (c *jobContext) BindData(i interface{}) error {
	return json.Unmarshal(c.data, i)
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
