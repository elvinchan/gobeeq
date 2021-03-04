package gobeeq

import (
	"os"
	"testing"

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
