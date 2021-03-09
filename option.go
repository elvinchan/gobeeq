package gobeeq

import "time"

type QueueOption func(*Queue)

// WithPrefix set prefix of redis key, default `bq`. Useful if the `bq:`
// namespace is, for whatever reason, unavailable or problematic on your redis
// instance.
func WithPrefix(s string) QueueOption {
	return func(q *Queue) {
		q.settings.Prefix = s
	}
}

// WithStallInterval set the window in which workers must report that they
// aren't stalling. Higher values will reduce Redis/network overhead, but if a
// worker stalls, it will take longer before its stalled job(s) will be
// retried. A higher value will also result in a lower probability of
// false-positives during stall detection.
func WithStallInterval(d time.Duration) QueueOption {
	return func(q *Queue) {
		q.settings.StallInterval = d
	}
}

// WithNearTermWindow set the window during which delayed jobs will be
// specifically scheduled. If all delayed jobs are further out that this window,
// the Queue will double-check that it hasn't missed any jobs after the window elapses.
func WithNearTermWindow(d time.Duration) QueueOption {
	return func(q *Queue) {
		q.settings.NearTermWindow = d
	}
}

// WithDelayedDebounce to avoid unnecessary churn for several jobs in short
// succession, the Queue may delay individual jobs by up to this duration.
func WithDelayedDebounce(d time.Duration) QueueOption {
	return func(q *Queue) {
		q.settings.DelayedDebounce = d
	}
}

// WithActivateDelayedJobs activate delayed jobs once they've passed their
// `delayUntil` timestamp. Note that this must be enabled on at least one
// `Queue` instance for the delayed retry strategies (`fixed` and `exponential`)
// - this will reactivate them after their computed delay.
func WithActivateDelayedJobs(b bool) QueueOption {
	return func(q *Queue) {
		q.settings.ActivateDelayedJobs = b
	}
}

// WithRemoveOnSuccess enable to have this worker automatically remove its
// successfully completed jobs from Redis, so as to keep memory usage down.
func WithRemoveOnSuccess(b bool) QueueOption {
	return func(q *Queue) {
		q.settings.RemoveOnSuccess = b
	}
}

// WithRemoveOnFailure enable to have this worker automatically remove its
// failed jobs from Redis, so as to keep memory usage down. This will not
// remove jobs that are set to retry unless they fail all their retries.
func WithRemoveOnFailure(b bool) QueueOption {
	return func(q *Queue) {
		q.settings.RemoveOnFailure = b
	}
}

// WithRedisScanCount set RedisScanCount which is the value of the `SSCAN`
// Redis command used in `Queue#GetJobs` for succeeded and failed job types.
func WithRedisScanCount(i int) QueueOption {
	return func(q *Queue) {
		q.settings.RedisScanCount = i
	}
}

// WithScriptsProvider set provider for lua scripts.
func WithScriptsProvider(i ScriptsProvider) QueueOption {
	return func(q *Queue) {
		q.provider = i
	}
}
