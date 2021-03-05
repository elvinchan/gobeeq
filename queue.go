package gobeeq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/elvinchan/util-collects/retry"
	"github.com/go-redis/redis/v8"
)

var logger *log.Logger

func init() {
	SetLogger(log.New(os.Stderr, "bq: ", log.LstdFlags|log.Lshortfile))
}

func SetLogger(l *log.Logger) {
	logger = l
}

type Queue struct {
	redis           *redis.Client
	config          *Config
	name            string
	handler         ProcessFunc
	concurrency     int64
	queued, running int64
	status          uint32 // 0 -> running, 1 -> closing, 2 -> closed
	checkTimer      *time.Timer
	delayedTimer    *EagerTimer
	stopCh          chan struct{}
	events          map[string]func(args ...interface{})
	wg              sync.WaitGroup
	mu              *sync.Mutex
}

func NewQueue(ctx context.Context, name string, r *redis.Client, config *Config,
) (*Queue, error) {
	if r == nil {
		panic(ErrRedisClientRequired)
	}
	if config == nil {
		config = defaultConfig
	}
	q := &Queue{
		redis:  r,
		config: config,
		name:   name,
		stopCh: make(chan struct{}),
		mu:     &sync.Mutex{},
	}
	if q.config.ActiveDelayedJobs {
		var err error
		q.delayedTimer, err = NewEagerTimer(
			q.config.NearTermWindow, q.activateDelayed,
		)
		if err != nil {
			return nil, err
		}
	}
	var channels []string
	if q.config.GetEvents {
		channels = append(channels, keyEvents.use(q))
	}
	if q.config.ActiveDelayedJobs {
		channels = append(channels, keyEarlierDelayed.use(q))
	}
	if len(channels) > 0 {
		pb := q.redis.Subscribe(ctx, channels...)
		go func() {
			for {
				select {
				case m := <-pb.Channel():
					q.handleMessage(m)
				case <-q.stopCh:
					return
				}
			}
		}()
	}
	return q, q.ensureScripts(ctx)
}

func (q *Queue) handleMessage(m *redis.Message) {
	if m.Channel == keyEarlierDelayed.use(q) {
		// We should only receive these messages if activateDelayedJobs is
		// enabled.
		t, _ := strconv.ParseInt(m.Payload, 10, 64)
		q.delayedTimer.Schedule(time.Unix(t, 0))
		return
	}

	type Message struct {
		Id    int64  `json:"id"`
		Event string `json:"event"`
		Data  string `json:"data"`
	}
	var msg Message
	if err := json.Unmarshal([]byte(m.Payload), &msg); err != nil {
		return
	}
	if fn := q.events["job "+msg.Event]; fn != nil {
		fn(msg.Id, msg.Data)
	}
	// TODO
}

func (q *Queue) keyPrefix() string {
	return q.config.Prefix + ":" + q.name + ":"
}

func (q *Queue) ensureScripts(ctx context.Context) error {
	if !q.commandable(false) {
		return ErrQueueClosed
	}
	scripts := []*redis.Script{
		q.config.ScriptsProvider.CheckStalledJobs(),
		q.config.ScriptsProvider.AddJob(),
		q.config.ScriptsProvider.RemoveJob(),
		q.config.ScriptsProvider.AddDelayedJob(),
		q.config.ScriptsProvider.RaiseDelayedJobs(),
	}
	var shas []string
	for _, s := range scripts {
		shas = append(shas, s.Hash())
	}
	evs, err := q.redis.ScriptExists(ctx, shas...).Result()
	if err != nil {
		return err
	} else if len(evs) != len(shas) {
		return ErrInvalidResult
	}
	for i := range evs {
		if evs[i] {
			continue
		}
		if _, err := scripts[i].Load(ctx, q.redis).Result(); err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) NewJob(data string, options *Options) *Job {
	return q.newJobWithId("", data, options)
}

func (q *Queue) newJobWithId(id string, data string, options *Options) *Job {
	if options == nil {
		options = defaultOptions()
	}
	if options.Timestamp == 0 {
		options.Timestamp = time.Now().UnixNano() / int64(time.Millisecond) // ms
	}
	return &Job{
		Id:      id,
		queue:   q,
		data:    data,
		options: options,
		status:  StatusCreated,
	}
}

func (q *Queue) GetJob(ctx context.Context, id string) (*Job, error) {
	if !q.commandable(false) {
		return nil, nil
	}
	return Job{}.fromId(ctx, q, id)
}

// GetJobs Get jobs from queue type.
func (q *Queue) GetJobs(ctx context.Context,
	s Status, start, end int64, size int) ([]Job, error) {
	if start <= 0 {
		start = 1
	}
	if end <= 0 {
		end = 1
	}
	if !q.commandable(false) {
		return nil, nil
	}
	k := key(s).use(q)
	var (
		ids []string
		err error
	)
	switch s {
	case StatusFailed, StatusSucceeded:
		ids, err = q.scanForJobs(ctx, k, 0, size, nil)
	case StatusWaiting, StatusActive:
		ids, err = q.redis.LRange(ctx, k, start, end).Result()
	case StatusDelayed:
		ids, err = q.redis.ZRange(ctx, k, start, end).Result()
	default:
		return nil, errors.New("invalid status")
	}
	if err != nil {
		return nil, err
	}
	return Job{}.fromIds(ctx, q, ids)
}

func (q *Queue) scanForJobs(ctx context.Context,
	key string, cursor uint64, size int, ids map[string]struct{},
) ([]string, error) {
	if size > q.config.RedisScanCount {
		size = q.config.RedisScanCount
	}
	if ids == nil {
		ids = make(map[string]struct{})
	}
	keys, nextCursor, err := q.redis.SScan(ctx, key, cursor, "COUNT", int64(size)).Result()
	if err != nil {
		return nil, err
	}
	for i := range keys {
		if len(ids) == int(size) {
			break
		}
		ids[keys[i]] = struct{}{}
	}
	if nextCursor == 0 || len(ids) >= int(size) {
		results := make([]string, 0, len(ids))
		for id := range ids {
			results = append(results, id)
		}
		return results, nil
	}
	return q.scanForJobs(ctx, key, nextCursor, size-len(ids), ids)
}

type ProcessFunc func(context.Context, *Job) error

func (q *Queue) ProcessMulti(
	concurrency int64,
	h func(context.Context, *Job) error,
) error {
	q.concurrency = concurrency
	return q.Process(h)
}

func (q *Queue) Process(h func(context.Context, *Job) error) error {
	if !q.commandable(true) {
		return ErrQueueClosed
	}
	q.mu.Lock()
	if q.handler != nil {
		q.mu.Unlock()
		return ErrHandlerAlreadyRegistered
	}
	q.handler = ProcessFunc(h)
	q.mu.Unlock()
	atomic.StoreInt64(&q.running, 0)
	atomic.StoreInt64(&q.queued, 1)
	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			if err := q.doStalledJobCheck(ctx); err != nil {
				logger.Fatal(err)
			}
			for {
				if !q.jobTick(ctx) {
					break
				}
			}
		}()
		go q.activateDelayed(ctx)

		<-q.stopCh
		cancel()
	}()
	return nil
}

func (q *Queue) commandable(strict bool) bool {
	status := atomic.LoadUint32(&q.status)
	return status == 0 || (!strict && status == 1)
}

func (q *Queue) jobTick(ctx context.Context) bool {
	if !q.commandable(true) {
		atomic.AddInt64(&q.queued, -1)
		return false
	}
	j, err := q.getNextJob(ctx)
	if err != nil {
		logger.Fatal(err)
		return true
	}
	if !q.commandable(true) {
		// This job will get picked up later as a stalled job if we happen to get here.
		atomic.AddInt64(&q.queued, -1)
		return false
	}
	atomic.AddInt64(&q.running, 1)
	atomic.AddInt64(&q.queued, -1)
	nextTick := false
	if (q.running + q.queued) < q.concurrency { // TODO: need lock
		atomic.AddInt64(&q.queued, 1)
		nextTick = true
	}

	if j == nil {
		// Per comment in Queue#_waitForJob, this branch is possible when
		// the job is removed before processing can take place, but after
		// being initially acquired.
		return nextTick
	}
	go func() {
		if err := q.runJob(ctx, j); err != nil {
			logger.Fatal(err)
		}
		atomic.AddInt64(&q.running, -1)
		atomic.AddInt64(&q.queued, 1)
	}()
	return true
}

func (q *Queue) runJob(ctx context.Context, j *Job) error {
	done := make(chan struct{}, 1)
	go func() {
		// preventStalling
		interval := q.config.StallInterval / 2
		timer := time.NewTimer(interval)
		for {
			timer.Reset(interval)
			select {
			case <-timer.C:
				if err := q.preventStall(j.Id); err != nil {
					logger.Fatal(err)
				}
			case <-done:
				return
			}
		}
	}()

	q.wg.Add(1)
	defer func() {
		q.wg.Done()
		done <- struct{}{}
	}()

	var err error
	if j.options.Timeout == 0 {
		err = q.handler(ctx, j)
	} else {
		errc := make(chan error, 1)
		go func() {
			errc <- q.handler(ctx, j)
		}()
		select {
		case err = <-errc:
		case <-time.After(time.Duration(j.options.Timeout) * time.Second):
			err = ErrTimeout
		}
	}
	// TODO: backoff
	// finish job
	return q.finishJob(ctx, err, j.data, j)
}

func (q *Queue) preventStall(id string) error {
	return q.redis.SRem(context.Background(), keyStalling.use(q), id).Err()
}

// CheckStalledJobs Check for stalled jobs.
// The interval on which to check for stalled jobs. This should be set to half
// the stallInterval setting, to avoid unnecessary work.
func (q *Queue) CheckStalledJobs(interval time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.doStalledJobCheck(ctx); err != nil {
		logger.Fatal(err)
	}
	if q.checkTimer != nil {
		return
	}
	q.checkTimer = time.NewTimer(interval)
	for {
		q.checkTimer.Reset(interval)
		select {
		case <-q.checkTimer.C:
			if err := q.doStalledJobCheck(ctx); err != nil {
				logger.Fatal(err)
			}
		case <-q.stopCh:
			return
		}
	}
}

func (q *Queue) doStalledJobCheck(ctx context.Context) error {
	return q.config.ScriptsProvider.CheckStalledJobs().EvalSha(
		ctx,
		q.redis,
		[]string{
			keyStallBlock.use(q),
			keyStalling.use(q),
			keyWaiting.use(q),
			keyActive.use(q),
		},
		q.config.StallInterval.Microseconds(),
	).Err()
}

func (q *Queue) getNextJob(ctx context.Context) (*Job, error) {
	if !q.commandable(true) {
		return nil, ErrQueueClosed
	}
	id, err := q.redis.BRPopLPush(ctx, keyWaiting.use(q), keyActive.use(q), 0).Result()
	if err != nil {
		return nil, err
	}
	return Job{}.fromId(ctx, q, id)
}

func (q *Queue) finishJob(ctx context.Context, err error, data interface{}, job *Job) error {
	_, err = q.redis.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.LRem(ctx, keyActive.use(q), 0, job.Id)
		p.SRem(ctx, keyStalling.use(q), job.Id)
		if err != nil {
			delay := int64(-1) // no retry
			if job.options.Retries > 0 {
				delay = job.options.Backoff.Delay()
			}
			if delay < 0 {
				job.status = StatusFailed
				if q.config.RemoveOnFailure {
					p.HDel(ctx, keyJobs.use(q), job.Id)
				} else {
					data, err := job.ToData()
					if err != nil {
						logger.Fatal(err)
					}
					p.HSet(ctx, keyJobs.use(q), job.Id, data)
					p.SAdd(ctx, keyFailed.use(q), job.Id)
				}
			} else {
				job.status = StatusRetrying
				data, err := job.ToData()
				if err != nil {
					logger.Fatal(err)
				}
				p.HSet(ctx, keyJobs.use(q), job.Id, data)
				if delay == 0 {
					p.LPush(ctx, keyWaiting.use(q), job.Id)
				} else {
					t := time.Now().Add(time.Millisecond * time.Duration(delay)).Unix()
					p.ZAdd(ctx, keyDelayed.use(q), &redis.Z{
						Score:  float64(t),
						Member: job.Id,
					})
					p.Publish(ctx, keyEarlierDelayed.use(q), t)
				}
			}
		} else {
			job.status = StatusSucceeded
			if q.config.RemoveOnSuccess {
				p.HDel(ctx, keyJobs.use(q), job.Id)
			} else {
				data, err := job.ToData()
				if err != nil {
					logger.Fatal(err)
				}
				p.HSet(ctx, keyJobs.use(q), job.Id, data)
				p.SAdd(ctx, keySucceeded.use(q), job.Id)
			}
		}
		return nil
	})
	return err
}

func (q *Queue) removeJob(ctx context.Context, id string) error {
	return q.config.ScriptsProvider.RemoveJob().EvalSha(
		ctx,
		q.redis,
		[]string{
			keySucceeded.use(q),
			keyFailed.use(q),
			keyWaiting.use(q),
			keyActive.use(q),
			keyStalling.use(q),
			keyJobs.use(q),
			keyDelayed.use(q),
		},
		id,
	).Err()
}

type QueueStatus struct {
	Keys        map[key]int64
	NewestJobId int64
}

func (q *Queue) CheckHealth(ctx context.Context) (*QueueStatus, error) {
	if !q.commandable(false) {
		return nil, ErrQueueClosed
	}
	pip := q.redis.TxPipeline()
	wv := pip.LLen(ctx, keyWaiting.use(q))
	av := pip.LLen(ctx, keyActive.use(q))
	sv := pip.SCard(ctx, keySucceeded.use(q))
	fv := pip.SCard(ctx, keyFailed.use(q))
	dv := pip.ZCard(ctx, keyDelayed.use(q))
	kv := pip.Get(ctx, keyId.use(q))
	if _, err := pip.Exec(ctx); err != nil {
		return nil, err
	}
	if err := pip.Close(); err != nil {
		return nil, err
	}
	id, err := strconv.ParseInt(kv.Val(), 10, 64)
	if err != nil {
		return nil, err
	}
	return &QueueStatus{
		Keys: map[key]int64{
			keyWaiting:   wv.Val(),
			keyActive:    av.Val(),
			keySucceeded: sv.Val(),
			keyFailed:    fv.Val(),
			keyDelayed:   dv.Val(),
		},
		NewestJobId: id,
	}, nil
}

// Close close queue and wait for 30s to
func (q *Queue) Close() error {
	return q.CloseTimeout(30 * time.Second)
}

func (q *Queue) CloseTimeout(timeout time.Duration) error {
	if !atomic.CompareAndSwapUint32(&q.status, 0, 1) {
		return ErrQueueClosed
	}
	close(q.stopCh)
	if q.delayedTimer != nil {
		q.delayedTimer.Stop()
	}
	err := q.WaitTimeout(timeout)
	atomic.StoreUint32(&q.status, 2)
	return err
}

func (q *Queue) WaitTimeout(timeout time.Duration) error {
	done := make(chan struct{}, 1)
	go func() {
		q.wg.Wait()
		done <- struct{}{}
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("bq: jobs are not processed after %s", timeout)
	}
}

func (q *Queue) IsRunning() bool {
	return atomic.LoadUint32(&q.status) == 0
}

func (q *Queue) Destory(ctx context.Context) error {
	if !q.commandable(false) {
		return ErrQueueClosed
	}
	return q.redis.Del(
		ctx,
		keyId.use(q),
		keyJobs.use(q),
		keyStallBlock.use(q),
		keyStalling.use(q),
		keyWaiting.use(q),
		keyActive.use(q),
		keySucceeded.use(q),
		keyFailed.use(q),
		keyDelayed.use(q),
	).Err()
}

// SaveAll Save all the provided jobs, without waiting for each job to be created.
// This pipelines the requests which avoids the waiting 2N*RTT for N jobs -
// the client waits to receive each command result before sending the next
// command.
func (q *Queue) SaveAll(ctx context.Context, jobs []Job) error {
	if !q.commandable(true) {
		return ErrQueueClosed
	}
	cmders, err := q.redis.TxPipelined(ctx, func(p redis.Pipeliner) error {
		for i := range jobs {
			_, err := jobs[i].save(ctx, p)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := range jobs {
		jobs[i].Id, err = cmders[i].(*redis.StringCmd).Result()
		if err != nil {
			return err
		}
		if jobs[i].options.Delay != 0 && q.config.ActiveDelayedJobs {
			q.delayedTimer.Schedule(time.Unix(jobs[i].options.Delay, 0))
		}
	}
	return nil
}

// TODO: need prevent running concurrently?
func (q *Queue) activateDelayed(ctx context.Context) {
	var v interface{}
	err := retry.Do(ctx, func(ctx context.Context, attempt uint) error {
		var err error
		v, err = q.config.ScriptsProvider.RaiseDelayedJobs().EvalSha(
			ctx,
			q.redis,
			[]string{
				keyDelayed.use(q),
				keyWaiting.use(q),
			},
			time.Now().Unix(),
			q.config.DelayedDebounce.Milliseconds(),
		).Result()
		return err
	})
	if err != nil {
		logger.Fatal(err)
	}
	vs := v.([]interface{})
	if vs == nil {
		logger.Fatal("invalid result of raiseDelayedJobs")
	}
	numRaised := vs[0].(int)
	nextOpportunity := vs[1].(int)
	if numRaised > 0 {
		if fn, ok := q.events["raised jobs"]; ok {
			fn(numRaised)
		}
	}
	q.delayedTimer.Schedule(time.Unix(int64(nextOpportunity), 0))
}
