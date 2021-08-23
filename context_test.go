package gobeeq

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestContextBindData(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	queue, err := NewQueue(ctx, "test-context-bind-data", client)
	assert.NoError(t, err)

	type T struct {
		Name string `json:"name"`
	}
	strInput, strOutput := "hello", ""

	cases := map[string]struct {
		data             interface{}
		value            interface{}
		saveErr, bindErr string
	}{
		"1": {
			data:  &map[string]int64{"t": 1},
			value: &map[string]int64{},
		},
		"2": {
			data:  &T{Name: "hello"},
			value: &T{},
		},
		"3": {
			data:  &strInput,
			value: &strOutput,
		},
		"4": {
			data:    nil,
			value:   nil,
			bindErr: "json: Unmarshal(nil)",
		},
		"5": {
			data:    make(chan struct{}),
			saveErr: "json: unsupported type: chan struct {}",
		},
	}
	var succ int
	for id := range cases {
		c := cases[id]
		_, err := queue.CreateJob(c.data).SetId(id).Save(ctx)
		if c.saveErr == "" {
			assert.NoError(t, err)
			succ++
		} else {
			assert.EqualError(t, err, c.saveErr)
		}
	}

	ch := make(chan struct{})
	err = queue.Process(func(ctx Context) error {
		t.Log("processing job:", ctx.GetId())
		c := cases[ctx.GetId()]
		err := ctx.BindData(c.value)
		if c.bindErr != "" {
			assert.EqualError(t, err, c.bindErr)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, c.data, c.value)
		}
		ch <- struct{}{}
		return nil
	})
	assert.NoError(t, err)
	for i := 0; i < succ; i++ {
		<-ch
	}
}

func TestContextSetResult(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	type T struct {
		Name string `json:"name"`
	}
	cases := map[string]struct {
		data   interface{}
		value  json.RawMessage
		setErr string
	}{
		"1": {
			data:  map[string]int64{"t": 1},
			value: json.RawMessage(`{"t":1}`),
		},
		"2": {
			data:  T{Name: "hello"},
			value: json.RawMessage(`{"name":"hello"}`),
		},
		"3": {
			data:  "hello",
			value: json.RawMessage(`"hello"`),
		},
		"4": {
			data:  nil,
			value: json.RawMessage("null"),
		},
		"5": {
			data:   make(chan struct{}),
			value:  json.RawMessage("null"),
			setErr: "json: unsupported type: chan struct {}",
		},
	}

	ch := make(chan struct{})
	queue, err := NewQueue(ctx, "test-context-set-result", client,
		WithSendEvents(true),
		WithOnJobSucceeded(func(jobId string, result json.RawMessage) {
			c := cases[jobId]
			assert.Equal(t, c.value, result)
			ch <- struct{}{}
		}),
	)
	assert.NoError(t, err)

	for id := range cases {
		_, err = queue.CreateJob(nil).SetId(id).Save(ctx)
		assert.NoError(t, err)
	}

	err = queue.Process(func(ctx Context) error {
		t.Log("processing job:", ctx.GetId())
		c := cases[ctx.GetId()]
		err := ctx.SetResult(c.data)
		if c.setErr != "" {
			assert.EqualError(t, err, c.setErr)
		} else {
			assert.NoError(t, err)
		}
		return nil
	})
	assert.NoError(t, err)
	for i := 0; i < len(cases); i++ {
		<-ch
	}
}

func TestContextProgress(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	type T struct {
		Name string `json:"name"`
	}
	cases := []struct {
		data   interface{}
		value  json.RawMessage
		setErr string
	}{
		{
			data:  map[string]int64{"t": 1},
			value: json.RawMessage(`{"t":1}`),
		},
		{
			data:  T{Name: "hello"},
			value: json.RawMessage(`{"name":"hello"}`),
		},
		{
			data:  "hello",
			value: json.RawMessage(`"hello"`),
		},
		{
			data:  nil,
			value: json.RawMessage("null"),
		},
		{
			data:   make(chan struct{}),
			value:  json.RawMessage("null"),
			setErr: "json: unsupported type: chan struct {}",
		},
	}

	ch := make(chan struct{})
	var i int32
	queue, err := NewQueue(ctx, "test-context-report-progress", client,
		WithSendEvents(true),
		WithOnJobProgress(func(jobId string, progress json.RawMessage) {
			ni := atomic.AddInt32(&i, 1)
			c := cases[ni-1]
			assert.Equal(t, c.value, progress)
			ch <- struct{}{}
		}),
	)
	assert.NoError(t, err)

	_, err = queue.CreateJob(nil).Save(ctx)
	assert.NoError(t, err)

	err = queue.Process(func(ctx Context) error {
		t.Log("processing job:", ctx.GetId())
		for _, c := range cases {
			err := ctx.ReportProgress(c.data)
			if c.setErr != "" {
				assert.EqualError(t, err, c.setErr)
			} else {
				assert.NoError(t, err)
			}
			time.Sleep(time.Millisecond * 100)
		}
		return nil
	})
	assert.NoError(t, err)

	for _, c := range cases {
		if c.setErr == "" {
			<-ch
		}
	}
}
