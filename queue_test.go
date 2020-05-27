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
	queue, err := NewQueue(name, client, nil)
	assert.NoError(t, err)
	assert.Equal(t, name, queue.name)
}

func TestQueueProcess(t *testing.T) {
	cases := []int{1, 3, 5}
	for i := range cases {
		t.Run(fmt.Sprintf("Process delay: %d", i), func(t *testing.T) {
			t.Parallel()

			name := fmt.Sprintf("test-queue-process-%d", i)
			queue, err := NewQueue(name, client, nil)
			assert.NoError(t, err)
			assert.Equal(t, name, queue.name)

			ch := make(chan time.Time)
			err = queue.Process(func(ctx context.Context, j *Job) error {
				t.Log("processing job:", j)
				assert.JSONEq(t, fmt.Sprintf(`{"foo": "bar-%d"}`, i), j.data)
				time.Sleep(time.Duration(cases[i]) * time.Second)
				ch <- time.Now()
				return nil
			})
			assert.NoError(t, err)

			data := struct {
				Foo string `json:"foo"`
			}{
				Foo: fmt.Sprintf("bar-%d", i),
			}
			db, _ := json.Marshal(data)
			_, err = queue.NewJob(string(db)).Save()
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
	queue, err := NewQueue("test-queue-running", client, nil)
	assert.NoError(t, err)

	assert.Equal(t, true, queue.IsRunning())

	// TODO pause
}

func TestQueueClose(t *testing.T) {
	var i int
	newQueue := func() *Queue {
		i++
		queue, err := NewQueue(fmt.Sprintf("test-queue-close-%d", i), client, nil)
		assert.NoError(t, err)

		err = queue.Process(func(ctx context.Context, j *Job) error {
			t.Log("processing job:", j)
			time.Sleep(5 * time.Second)
			return nil
		})
		assert.NoError(t, err)
		return queue
	}

	t.Run("Wait for finish", func(t *testing.T) {
		queue := newQueue()
		j, err := queue.NewJob("data").Save()
		assert.NoError(t, err)
		assert.NotEmpty(t, j.Id)

		time.Sleep(time.Second)
		err = queue.Close()
		assert.NoError(t, err)
	})

	t.Run("Not processed", func(t *testing.T) {
		queue := newQueue()
		j, err := queue.NewJob("data").Save()
		assert.NoError(t, err)
		assert.NotEmpty(t, j.Id)

		time.Sleep(time.Second)
		err = queue.CloseTimeout(2 * time.Second)
		assert.EqualError(t, err, "bq: jobs are not processed after 2s")
	})
}

func TestQueueDestroy(t *testing.T) {
	queue, err := NewQueue("test-queue-destroy", client, nil)
	assert.NoError(t, err)

	j, err := queue.NewJob("data").Save()
	assert.NoError(t, err)
	assert.NotEmpty(t, j.Id)

	err = queue.Destory()
	assert.NoError(t, err)

	v, err := client.Keys(queue.keyPrefix() + "*").Result()
	assert.NoError(t, err)
	assert.Empty(t, v)
}
