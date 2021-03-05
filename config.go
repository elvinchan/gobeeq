package gobeeq

import "time"

type Config struct {
	StallInterval time.Duration
	// Avoid scheduling timers for further out than this period of time. The
	// workers will all poll on this interval, at minimum, to find new delayed
	// jobs.
	NearTermWindow time.Duration
	// Avoids rapid churn during processing of nearly-concurrent events.
	DelayedDebounce   time.Duration
	EnsureScripts     bool
	Prefix            string
	Concurrency       uint
	GetEvents         bool
	ActiveDelayedJobs bool
	// Enable to have this worker automatically remove its successfully
	// completed jobs from Redis, so as to keep memory usage down.
	RemoveOnSuccess bool
	// Enable to have this worker automatically remove its failed jobs from
	// Redis, so as to keep memory usage down. This will not remove jobs that
	// are set to retry unless they fail all their retries.
	RemoveOnFailure bool
	SendEvents      bool
	RedisScanCount  int
	ScriptsProvider
}

var (
	defaultConfig = &Config{
		StallInterval:   time.Second * 5,
		NearTermWindow:  time.Second * 60 * 20,
		DelayedDebounce: time.Second,
		GetEvents:       true,
		EnsureScripts:   true,
		SendEvents:      true,
		Prefix:          "bq",
		Concurrency:     1,
		RedisScanCount:  100,
		ScriptsProvider: DefaultScriptsProvider{},
	}
)
