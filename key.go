package gobeeq

type prefix interface {
	keyPrefix() string
}

type key string

const (
	keyId         key = "id"         // Integer, incremented to determine the next Job ID.
	keyJobs       key = "jobs"       // Hash from Job ID to a JSON string containing its data and options.
	keyWaiting    key = "waiting"    // List of IDs of jobs waiting to be processed.
	keyActive     key = "active"     // List of IDs jobs currently being processed.
	keySucceeded  key = "succeeded"  // Set of IDs of jobs which succeeded.
	keyFailed     key = "failed"     // Set of IDs of jobs which failed.
	keyDelayed    key = "delayed"    // Ordered Set of IDs corresponding to delayed jobs - this set maps delayed timestamp to IDs.
	keyStalling   key = "stalling"   // Set of IDs of jobs which haven't 'checked in' during this interval.
	keyStallBlock key = "stallBlock" // Set of IDs of jobs which haven't 'checked in' during this interval.
)

func (k key) use(p prefix) string {
	return p.keyPrefix() + string(k)
}
