package gobeeq

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
)

var client *redis.Client

func TestMain(m *testing.M) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()
	client = redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	code := m.Run()
	os.Exit(code)
}

func mockData(i int) string {
	d := struct {
		Foo string `json:"foo"`
	}{
		Foo: fmt.Sprintf("bar-%d", i),
	}
	v, _ := json.Marshal(d)
	return string(v)
}

func waitSync() {
	time.Sleep(time.Second)
}
