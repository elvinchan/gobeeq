package gobeeq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimeToUnixMS(t *testing.T) {
	now := time.Now()
	assert.Equal(t, now.UnixNano()/int64(time.Millisecond), timeToUnixMS(now))
}

func TestUnixMSToTime(t *testing.T) {
	now := time.Now()
	target := unixMSToTime(now.UnixNano() / int64(time.Millisecond))
	assert.Equal(t, now.Unix(), target.Unix())
}

func TestMSToDuration(t *testing.T) {
	d := msToDuration(999)
	assert.Equal(t, d, 999*time.Millisecond)
}
