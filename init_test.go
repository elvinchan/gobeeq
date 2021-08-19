package gobeeq

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

var client *redis.Client

func TestMain(m *testing.M) {
	client = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		panic(err)
	}
	code := m.Run()
	os.Exit(code)
}

func mockData(i int) json.RawMessage {
	d := struct {
		Foo string `json:"foo"`
	}{
		Foo: fmt.Sprintf("bar-%d", i),
	}
	v, _ := json.Marshal(d)
	return v
}

func waitSync() {
	time.Sleep(time.Millisecond * 200)
}
