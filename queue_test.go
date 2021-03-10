package gobeeq

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const testTimeout = 30 * time.Second

/*
func middle(h ProcessFunc) ProcessFunc {
	return func(ctx context.Context, j *Job) error {
		return nil
	}
}
*/

func TestQueue(t *testing.T) {
	name := "test-queue-basic"
	ctx := context.Background()
	queue, err := NewQueue(ctx, name, client)
	assert.NoError(t, err)
	assert.Equal(t, name, queue.name)
}

func TestQueueProcess(t *testing.T) {
	ctx := context.Background()
	cases := []int{1, 3, 5}
	chs := make(map[string]chan time.Time)
	for i := range cases {
		t.Run(fmt.Sprintf("Process delay: %d", i), func(t *testing.T) {
			t.Parallel()

			name := fmt.Sprintf("test-queue-process-%d", i)
			queue, err := NewQueue(ctx, name, client)
			assert.NoError(t, err)
			assert.Equal(t, name, queue.name)

			chs[name] = make(chan time.Time)
			err = queue.Process(func(ctx Context) error {
				t.Log("processing job:", ctx.GetId())
				assert.JSONEq(t, fmt.Sprintf(`{"foo": "bar-%d"}`, i), ctx.GetData())
				time.Sleep(time.Duration(cases[i]) * time.Second)
				chs[name] <- time.Now()
				return nil
			})
			assert.NoError(t, err)

			data := struct {
				Foo string `json:"foo"`
			}{
				Foo: fmt.Sprintf("bar-%d", i),
			}
			db, _ := json.Marshal(data)
			_, err = queue.NewJob(string(db)).Save(ctx)
			assert.NoError(t, err)

			select {
			case <-chs[name]:
			case <-time.After(testTimeout):
				t.Fatalf("job was not processed")
			}
		})
	}
}

func TestQueueRunning(t *testing.T) {
	ctx := context.Background()
	queue, err := NewQueue(ctx, "test-queue-running", client)
	assert.NoError(t, err)

	assert.Equal(t, true, queue.IsRunning())

	// TODO pause
}

func TestQueueClose(t *testing.T) {
	ctx := context.Background()
	var i int
	newQueue := func() *Queue {
		i++
		queue, err := NewQueue(ctx, fmt.Sprintf("test-queue-close-%d", i), client)
		assert.NoError(t, err)

		err = queue.Process(func(ctx Context) error {
			t.Log("processing job:", ctx.GetId())
			time.Sleep(5 * time.Second)
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

		time.Sleep(time.Second)
		err = queue.Close()
		assert.NoError(t, err)
	})

	t.Run("Not processed", func(t *testing.T) {
		queue := newQueue()
		j, err := queue.NewJob("data").Save(ctx)
		assert.NoError(t, err)
		assert.NotEmpty(t, j.Id)

		time.Sleep(time.Second)
		err = queue.CloseTimeout(2 * time.Second)
		assert.EqualError(t, err, "gobeeq: jobs are not processed after 2s")
	})
}

func TestQueueDestroy(t *testing.T) {
	ctx := context.Background()
	queue, err := NewQueue(ctx, "test-queue-destroy", client)
	assert.NoError(t, err)

	j, err := queue.NewJob("data").Save(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, j.Id)

	err = queue.Destory(ctx)
	assert.NoError(t, err)

	v, err := client.Keys(ctx, queue.keyPrefix()+"*").Result()
	assert.NoError(t, err)
	assert.Empty(t, v)
}
