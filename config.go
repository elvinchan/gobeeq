package gobeeq

import "time"

type Config struct {
	// Duration of the window in which workers must report that they aren't
	// stalling. Higher values will reduce Redis/network overhead, but if a
	// worker stalls, it will take longer before its stalled job(s) will be
	// retried. A higher value will also result in a lower probability of
	// false-positives during stall detection.
	StallInterval time.Duration
	// The window during which delayed jobs will be specifically scheduled.
	// If all delayed jobs are further out that this window, the Queue will
	// double-check that it hasn't missed any jobs after the window elapses.
	NearTermWindow time.Duration
	// To avoid unnecessary churn for several jobs in short succession, the
	// Queue may delay individual jobs by up to this amount.
	DelayedDebounce   time.Duration
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
		SendEvents:      true,
		Prefix:          "bq",
		Concurrency:     1,
		RedisScanCount:  100,
		ScriptsProvider: DefaultScriptsProvider{},
	}
)
