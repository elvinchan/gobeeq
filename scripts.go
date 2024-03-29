package gobeeq

import "github.com/go-redis/redis/v8"

type (
	ScriptsProvider interface {
		CheckStalledJobs() *redis.Script
		AddJob() *redis.Script
		RemoveJob() *redis.Script
		AddDelayedJob() *redis.Script
		RaiseDelayedJobs() *redis.Script
	}

	DefaultScriptsProvider struct {
		checkStalledJobs *redis.Script
		addJob           *redis.Script
		removeJob        *redis.Script
		addDelayedJob    *redis.Script
		raiseDelayedJobs *redis.Script
	}
)

var defaultScriptsProvider = DefaultScriptsProvider{
	checkStalledJobs: redis.NewScript(scriptCheckStalledJobs),
	addJob:           redis.NewScript(scriptAddJob),
	removeJob:        redis.NewScript(scriptRemoveJob),
	addDelayedJob:    redis.NewScript(scriptAddDelayedJob),
	raiseDelayedJobs: redis.NewScript(scriptRaiseDelayedJobs),
}

func (p DefaultScriptsProvider) CheckStalledJobs() *redis.Script {
	return p.checkStalledJobs
}

func (p DefaultScriptsProvider) AddJob() *redis.Script {
	return p.addJob
}

func (p DefaultScriptsProvider) RemoveJob() *redis.Script {
	return p.removeJob
}

func (p DefaultScriptsProvider) AddDelayedJob() *redis.Script {
	return p.addDelayedJob
}

func (p DefaultScriptsProvider) RaiseDelayedJobs() *redis.Script {
	return p.raiseDelayedJobs
}

/*
key 1 -> bq:name:stallBlock
key 2 -> bq:name:stalling
key 3 -> bq:name:waiting
key 4 -> bq:name:active
arg 1 -> ms stallInterval

returns {resetJobId1, resetJobId2, ...}

workers are responsible for removing their jobId from the stalling set every stallInterval ms
if a jobId is not removed from the stalling set within a stallInterval window,
we assume the job has stalled and should be reset (moved from active back to waiting)
*/
const scriptCheckStalledJobs = `-- try to update the stallBlock key
if not redis.call("set", KEYS[1], "1", "PX", tonumber(ARGV[1]), "NX") then
  -- hasn't been long enough (stallInterval) since last check
  return {}
end

-- reset any stalling jobs by moving from active to waiting
local stalling, stalled = redis.call("smembers", KEYS[2]), {}
if next(stalling) ~= nil then
  -- not worth optimizing - this should be a rare occurrence, better to keep it straightforward
  for i, jobId in ipairs(stalling) do
    local removed = redis.call("lrem", KEYS[4], 0, jobId)
    -- safety belts: we only restart stalled jobs if we can find them in the active list
    -- the only place we add jobs to the stalling set is in this script, and the two places we
    -- remove jobs from the active list are in this script, and in the MULTI after the job finishes
    if removed > 0 then
      stalled[#stalled + 1] = jobId
    end
  end
  -- don't lpush zero jobs (the redis command will fail)
  if #stalled > 0 then
    -- lpush instead of rpush so that jobs which cause uncaught exceptions don't
    -- hog the job consumers and starve the whole system. not a great situation
    -- to be in, but this is fairer.
    redis.call("lpush", KEYS[3], unpack(stalled))
  end
  redis.call("del", KEYS[2])
end

-- copy currently active jobs into stalling set
local actives = redis.call("lrange", KEYS[4], 0, -1)
if next(actives) ~= nil then
  redis.call("sadd", KEYS[2], unpack(actives))
end

return stalled`

/*
key 1 -> bq:name:id (job ID counter)
key 2 -> bq:name:jobs
key 3 -> bq:name:waiting
arg 1 -> job id
arg 2 -> job data
*/
const scriptAddJob = `local jobId = ARGV[1]
if jobId == "" then
  jobId = "" .. redis.call("incr", KEYS[1])
end
if redis.call("hexists", KEYS[2], jobId) == 1 then return nil end
redis.call("hset", KEYS[2], jobId, ARGV[2])
redis.call("lpush", KEYS[3], jobId)

return jobId`

/*
key 1 -> bq:test:succeeded
key 2 -> bq:test:failed
key 3 -> bq:test:waiting
key 4 -> bq:test:active
key 5 -> bq:test:stalling
key 6 -> bq:test:jobs
key 7 -> bq:test:delayed
arg 1 -> jobId
*/
const scriptRemoveJob = `local jobId = ARGV[1]

if (redis.call("sismember", KEYS[1], jobId) + redis.call("sismember", KEYS[2], jobId)) == 0 then
  redis.call("lrem", KEYS[3], 0, jobId)
  redis.call("lrem", KEYS[4], 0, jobId)
end

redis.call("srem", KEYS[1], jobId)
redis.call("srem", KEYS[2], jobId)
redis.call("srem", KEYS[5], jobId)
redis.call("hdel", KEYS[6], jobId)
redis.call("zrem", KEYS[7], jobId)`

/*
key 1 -> bq:name:id (job ID counter)
key 2 -> bq:name:jobs
key 3 -> bq:name:delayed
key 4 -> bq:name:earlierDelayed
arg 1 -> job id
arg 2 -> job data
arg 3 -> job delay timestamp
*/
const scriptAddDelayedJob = `local jobId = ARGV[1]
if jobId == "" then
  jobId = "" .. redis.call("incr", KEYS[1])
end
if redis.call("hexists", KEYS[2], jobId) == 1 then return nil end
redis.call("hset", KEYS[2], jobId, ARGV[2])
redis.call("zadd", KEYS[3], tonumber(ARGV[3]), jobId)

-- if this job is the new head, alert the workers that they need to update their timers
-- if we try to do something tricky like checking the delta between this job and the next job, we
-- can enter a pathological case where jobs incrementally creep sooner, and each one never updates
-- the timers
local head = redis.call("zrange", KEYS[3], 0, 0)
if head[1] == jobId then
  redis.call("publish", KEYS[4], ARGV[3])
end

return jobId`

/*
key 1 -> bq:name:delayed
key 2 -> bq:name:waiting
arg 1 -> ms timestamp ("now")
arg 2 -> debounce window (in milliseconds)

returns number of jobs raised and the timestamp of the next job (within the near-term window)
*/
const scriptRaiseDelayedJobs = `local now = tonumber(ARGV[1])

-- raise any delayed jobs that are now valid by moving from delayed to waiting
local raising = redis.call("zrangebyscore", KEYS[1], 0, ARGV[1])
local numRaising = #raising

if numRaising > 0 then
  redis.call("lpush", KEYS[2], unpack(raising))
  redis.call("zremrangebyscore", KEYS[1], 0, ARGV[1])
end

local head = redis.call("zrange", KEYS[1], 0, 0, "WITHSCORES")
local nearTerm = -1
if next(head) ~= nil then
  local proximal = redis.call("zrevrangebyscore", KEYS[1], head[2] + tonumber(ARGV[2]), 0, "WITHSCORES", "LIMIT", 0, 1)
  nearTerm = proximal[2]
end

return {numRaising, nearTerm}`
