package gobeeq

import (
	"context"
	"time"
)

type Context interface {
	context.Context

	GetId() string
	GetData() string
	SetResult(r string)
}

var _ Context = (*jobContext)(nil)

type jobContext struct {
	ctx    context.Context
	id     string
	data   string
	result string
}

func (c *jobContext) GetId() string {
	return c.id
}

func (c *jobContext) GetData() string {
	return c.data
}

func (c *jobContext) SetResult(v string) {
	c.result = v
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
