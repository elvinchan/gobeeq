package gobeeq

import "errors"

type prefix interface {
	keyPrefix() string
}

type key string

const (
	keyId             key = "id"             // Integer, incremented to determine the next Job ID.
	keyJobs           key = "jobs"           // Hash from Job ID to a JSON string containing its data and options.
	keyWaiting        key = "waiting"        // List of IDs of jobs waiting to be processed.
	keyActive         key = "active"         // List of IDs jobs currently being processed.
	keySucceeded      key = "succeeded"      // Set of IDs of jobs which succeeded.
	keyFailed         key = "failed"         // Set of IDs of jobs which failed.
	keyDelayed        key = "delayed"        // Ordered Set of IDs corresponding to delayed jobs - this set maps delayed timestamp to IDs.
	keyStalling       key = "stalling"       // Set of IDs of jobs which haven't 'checked in' during this interval.
	keyStallBlock     key = "stallBlock"     // Set of IDs of jobs which haven't 'checked in' during this interval.
	keyEvents         key = "events"         // Pub/Sub channel for workers to send out job results.
	keyEarlierDelayed key = "earlierDelayed" // When a new delayed job is added prior to all other jobs, the script creating the job will publish the job's timestamp over this Pub/Sub channel.
)

func (k key) use(p prefix) string {
	return p.keyPrefix() + string(k)
}

var (
	ErrRedisClientRequired      = errors.New("gobeeq: Redis client is required")
	ErrInvalidResult            = errors.New("gobeeq: invalid Redis result")
	ErrInvalidJobStatus         = errors.New("gobeeq: invalid job status")
	ErrQueueClosed              = errors.New("gobeeq: queue is already closed")
	ErrHandlerAlreadyRegistered = errors.New("gobeeq: handler already registered")
)
