package gobeeq

import "time"

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
