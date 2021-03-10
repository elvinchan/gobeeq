package gobeeq

import (
	"context"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

// timeToUnixMS returns milliseconds unix of t
func timeToUnixMS(t time.Time) int64 {
	return t.Unix() / int64(time.Millisecond)
}

// unixMSToTime returns time of milliseconds unix
func unixMSToTime(ms int64) time.Time {
	return time.Unix(0, ms*int64(time.Millisecond))
}

// msToDuration returns duration of milliseconds
func msToDuration(ms int64) time.Duration {
	return time.Duration(ms * int64(time.Millisecond))
}

func runScript(
	ctx context.Context,
	sha1 string,
	cmd redis.Cmdable,
	keys []string,
	args ...interface{},
) *redis.Cmd {
	r := cmd.EvalSha(ctx, sha1, keys, args...)
	if err := r.Err(); err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT ") {
		return cmd.Eval(ctx, sha1, keys, args...)
	}
	return r
}
