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
	SetLogger(log.New(os.Stderr, "gobeeq: ", log.LstdFlags|log.Lshortfile))
}

func SetLogger(l *log.Logger) {
	logger = l
}

type Queue struct {
	redis                *redis.Client
	name                 string
	settings             *Settings
	provider             ScriptsProvider
	onRaised             func(numRaised int64)
	onSucceeded          func(jobId, result string)
	onRetrying, onFailed func(jobId string, err error)
	onProgress           func(jobId, progress string)
	handler              ProcessFunc
	concurrency          int64
	queued, running      int64
	status               uint32 // 0 -> running, 1 -> closing, 2 -> closed
	checkTimer           *time.Timer
	delayedTimer         *EagerTimer
	stopCh               chan struct{}
	doneCh               chan struct{}
	once                 *sync.Once
	mu                   *sync.Mutex
}

type Settings struct {
	Prefix              string
	StallInterval       time.Duration
	NearTermWindow      time.Duration
	DelayedDebounce     time.Duration
	SendEvents          bool
	ActivateDelayedJobs bool
	RemoveOnSuccess     bool
	RemoveOnFailure     bool
	RedisScanCount      int
}

func defaultSettings() *Settings {
	return &Settings{
		Prefix:          "bq",
		StallInterval:   time.Second * 5,
		NearTermWindow:  time.Second * 60 * 20,
		DelayedDebounce: time.Second,
		RedisScanCount:  100,
	}
}

// NewQueue create a queue instance.
func NewQueue(
	ctx context.Context, name string, r *redis.Client, opts ...QueueOption,
) (*Queue, error) {
	if r == nil {
		panic(ErrRedisClientRequired)
	}
	q := &Queue{
		redis:    r,
		settings: defaultSettings(),
		provider: defaultScriptsProvider,
		name:     name,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
		once:     &sync.Once{},
		mu:       &sync.Mutex{},
	}
	for _, opt := range opts {
		opt(q)
	}
	if q.settings.ActivateDelayedJobs {
		var err error
		q.delayedTimer, err = NewEagerTimer(
			q.settings.NearTermWindow, q.activateDelayed,
		)
		if err != nil {
			return nil, fmt.Errorf("gobeeq: %v", err)
		}
	}
	var channels []string
	if q.onSucceeded != nil ||
		q.onRetrying != nil ||
		q.onFailed != nil ||
		q.onProgress != nil {
		channels = append(channels, keyEvents.use(q))
	}
	if q.settings.ActivateDelayedJobs {
		channels = append(channels, keyEarlierDelayed.use(q))
	}
	if len(channels) > 0 {
		pb := q.redis.Subscribe(ctx, channels...)
		go func() {
			for {
				select {
				case m := <-pb.Channel():
					go q.handleMessage(m)
				case <-q.stopCh:
					return
				}
			}
		}()
	}
	return q, nil
}

type Message struct {
	Id    string `json:"id"`
	Event string `json:"event"`
	Data  string `json:"data"`
}

func (q *Queue) handleMessage(m *redis.Message) {
	if m.Channel == keyEarlierDelayed.use(q) {
		// We should only receive these messages if activateDelayedJobs is
		// enabled.
		t, _ := strconv.ParseInt(m.Payload, 10, 64)
		q.delayedTimer.Schedule(unixMSToTime(t))
		return
	}
	var msg Message
	if err := json.Unmarshal([]byte(m.Payload), &msg); err != nil {
		return
	}
	switch msg.Event {
	case "succeeded":
		if q.onSucceeded != nil {
			q.onSucceeded(msg.Id, msg.Data)
		}
	case "retrying":
		if q.onRetrying != nil {
			q.onRetrying(msg.Id, errors.New(msg.Data))
		}
	case "failed":
		if q.onFailed != nil {
			q.onFailed(msg.Id, errors.New(msg.Data))
		}
	case "progress":
		if q.onProgress != nil {
			q.onProgress(msg.Id, msg.Data)
		}
	}
	// TODO: handle for stored job
}

func (q *Queue) keyPrefix() string {
	return q.settings.Prefix + ":" + q.name + ":"
}

// NewJob create a job instance with the associated user data.
func (q *Queue) NewJob(data string) *Job {
	return q.newJobWithId("", data, defaultOptions())
}

func (q *Queue) newJobWithId(id string, data string, options *Options) *Job {
	if options.Timestamp == 0 {
		options.Timestamp = timeToUnixMS(time.Now())
	}
	return &Job{
		Id:      id,
		queue:   q,
		data:    data,
		options: options,
		status:  StatusCreated,
	}
}

// GetJob get a job by its id.
func (q *Queue) GetJob(ctx context.Context, id string) (*Job, error) {
	if !q.commandable(false) {
		return nil, nil
	}
	return Job{}.fromId(ctx, q, id)
}

// GetJobs get jobs from queue.
// When get jobs of status `waiting`, active`, or `delayed`, set `start` and `end`
// to specify a range of job indices to return.
// Jobs of status `failed` and `succeeded` will return an arbitrary subset of the
// queue of size `size`.
// Note: This is because failed and succeeded jobs are represented by a Redis
// SET, which does not maintain a job ordering.
//
// Note that large values of the attributes of `page` may cause excess load on
// the Redis server.
func (q *Queue) GetJobs(ctx context.Context,
	s Status, start, end int64, size int) ([]Job, error) {
	if start < 0 {
		start = 0
	}
	if end < 0 {
		end = 0
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
		return nil, ErrInvalidJobStatus
	}
	if err != nil {
		return nil, err
	}
	return Job{}.fromIds(ctx, q, ids)
}

func (q *Queue) scanForJobs(ctx context.Context,
	key string, cursor uint64, size int, ids map[string]struct{},
) ([]string, error) {
	if size <= 0 {
		size = 1
	} else if size > q.settings.RedisScanCount {
		size = q.settings.RedisScanCount
	}
	if ids == nil {
		ids = make(map[string]struct{})
	}
	keys, nextCursor, err := q.redis.SScan(ctx, key, cursor, "", int64(size)).Result()
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

type ProcessFunc func(Context) error

// ProcessConcurrently begins processing jobs with the provided concurrency and
// handler function.
func (q *Queue) ProcessConcurrently(
	concurrency int64,
	h func(Context) error,
) error {
	if !q.commandable(true) {
		return ErrQueueClosed
	}
	q.mu.Lock()
	if q.handler != nil {
		q.mu.Unlock()
		return ErrHandlerAlreadyRegistered
	}
	q.concurrency = concurrency
	q.handler = ProcessFunc(h)
	q.mu.Unlock()
	q.queued = 1
	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		if err := q.doStalledJobCheck(ctx); err != nil {
			logger.Fatal(err)
		}
		go q.jobTick(ctx)
		<-q.stopCh
		cancel()
	}()
	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		if q.settings.ActivateDelayedJobs {
			q.activateDelayed(ctx)
		}
		<-q.stopCh
		cancel()
	}()
	return nil
}

// Process begins processing jobs with the provided handler function.
func (q *Queue) Process(h func(Context) error) error {
	return q.ProcessConcurrently(1, h)
}

func (q *Queue) commandable(strict bool) bool {
	status := atomic.LoadUint32(&q.status)
	return status == 0 || (!strict && status == 1)
}

func (q *Queue) jobTick(ctx context.Context) {
	if !q.commandable(true) {
		q.endTick(ctx)
		return
	}
	j, err := q.getNextJob(ctx)
	if err != nil {
		logger.Print(err)
		go q.jobTick(ctx) // TODO: use retry?
		return
	}
	if !q.commandable(true) {
		// This job will get picked up later as a stalled job if we happen to get here.
		q.endTick(ctx)
		return
	}
	q.nextTick(ctx)
	defer func() {
		q.endRun()
		go q.jobTick(ctx)
	}()

	q.startRun()
	if j == nil {
		// Per comment in Queue#_waitForJob, this branch is possible when
		// the job is removed before processing can take place, but after
		// being initially acquired.
		return
	}
	if err := q.runJob(ctx, j); err != nil {
		logger.Print(err)
	}
}

func (q *Queue) endTick(ctx context.Context) {
	q.mu.Lock()
	q.queued--
	q.mu.Unlock()
}

func (q *Queue) nextTick(ctx context.Context) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.running+q.queued >= q.concurrency {
		return
	}
	q.queued++
	go q.jobTick(ctx)
}

func (q *Queue) startRun() {
	q.mu.Lock()
	q.running++
	q.queued--
	q.mu.Unlock()
}

func (q *Queue) endRun() {
	q.mu.Lock()
	q.running--
	q.queued++
	if q.running == 0 && !q.commandable(true) {
		q.once.Do(func() {
			close(q.doneCh)
		})
	}
	q.mu.Unlock()
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

// TODO: recovery
func (q *Queue) runJob(ctx context.Context, j *Job) error {
	done := make(chan struct{}, 1)
	go func() {
		interval := q.settings.StallInterval / 2
		t := time.NewTimer(interval)
		for {
			t.Reset(interval)
			select {
			case <-t.C:
				if err := q.preventStall(j.Id); err != nil {
					logger.Fatal(err)
				}
			case <-done:
				if !t.Stop() {
					select {
					case <-t.C:
					default:
					}
				}
				return
			}
		}
	}()

	defer func() {
		done <- struct{}{}
	}()

	jobCtx := &jobContext{
		ctx:  ctx,
		id:   j.Id,
		data: j.data,
	}
	var err error
	if j.options.Timeout == 0 {
		err = q.handler(jobCtx)
	} else {
		var cancel context.CancelFunc
		jobCtx.ctx, cancel = context.WithTimeout(
			jobCtx.ctx, msToDuration(j.options.Timeout),
		)
		defer cancel()

		errc := make(chan error, 1)
		go func() {
			err := q.handler(jobCtx)
			// note: make sure err == DeadlineExceeded if jobCtx is done.
			select {
			case <-jobCtx.Done():
			default:
				errc <- err
			}
		}()
		select {
		case err = <-errc:
		case <-jobCtx.Done():
			err = jobCtx.Err()
		}
	}
	return q.finishJob(ctx, err, jobCtx.result, j)
}

func (q *Queue) preventStall(id string) error {
	return q.redis.SRem(context.Background(), keyStalling.use(q), id).Err()
}

func (q *Queue) finishJob(ctx context.Context, err error, result string, job *Job) error {
	_, err = q.redis.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.LRem(ctx, keyActive.use(q), 0, job.Id)
		p.SRem(ctx, keyStalling.use(q), job.Id)
		msg := Message{
			Id: job.Id,
		}
		if err != nil {
			job.status = StatusFailed
			msg.Data = err.Error()
			delay := int64(-1) // no retry
			if job.options.Retries > 0 {
				delay = job.options.Backoff.cal()
			}
			if delay < 0 {
				if q.settings.RemoveOnFailure {
					p.HDel(ctx, keyJobs.use(q), job.Id)
				} else {
					p.HSet(ctx, keyJobs.use(q), job.Id, job.toData())
					p.SAdd(ctx, keyFailed.use(q), job.Id)
				}
			} else {
				job.options.Retries -= 1
				job.status = StatusRetrying
				p.HSet(ctx, keyJobs.use(q), job.Id, job.toData())
				if delay == 0 {
					p.LPush(ctx, keyWaiting.use(q), job.Id)
				} else {
					t := timeToUnixMS(time.Now().Add(msToDuration(delay)))
					p.ZAdd(ctx, keyDelayed.use(q), &redis.Z{
						Score:  float64(t),
						Member: job.Id,
					})
					p.Publish(ctx, keyEarlierDelayed.use(q), t)
				}
			}
		} else {
			job.status = StatusSucceeded
			msg.Data = result
			if q.settings.RemoveOnSuccess {
				p.HDel(ctx, keyJobs.use(q), job.Id)
			} else {
				p.HSet(ctx, keyJobs.use(q), job.Id, job.toData())
				p.SAdd(ctx, keySucceeded.use(q), job.Id)
			}
		}
		if q.settings.SendEvents {
			msg.Event = string(job.status)
			v, _ := json.Marshal(msg)
			p.Publish(ctx, keyEvents.use(q), string(v))
		}
		return nil
	})
	return err
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
	q.mu.Lock()
	if q.checkTimer != nil {
		q.mu.Unlock()
		return
	}
	q.checkTimer = time.NewTimer(interval)
	q.mu.Unlock()
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
	return q.provider.CheckStalledJobs().Run(
		ctx,
		q.redis,
		[]string{
			keyStallBlock.use(q),
			keyStalling.use(q),
			keyWaiting.use(q),
			keyActive.use(q),
		},
		q.settings.StallInterval.Milliseconds(),
	).Err()
}

// RemoveJob removes a job from the queue by jobId.
//
// This may have unintended side-effect, e.g. if the job is currently being
// processed by another worker, so only use this method when you know it's safe.
func (q *Queue) RemoveJob(ctx context.Context, id string) error {
	err := q.provider.RemoveJob().Run(
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
	if err == redis.Nil {
		return nil
	}
	return err
}

type QueueStatus struct {
	Keys        map[key]int64
	NewestJobId int64
}

// CheckHealth check the "health" of the queue and returns queue status.
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

// Close close queue and wait for 30s before all processing jobs finished.
func (q *Queue) Close() error {
	return q.CloseTimeout(30 * time.Second)
}

// CloseTimeout close queue and wait for t before all processing jobs finished.
func (q *Queue) CloseTimeout(t time.Duration) error {
	if !atomic.CompareAndSwapUint32(&q.status, 0, 1) {
		return ErrQueueClosed
	}
	close(q.stopCh)
	if q.delayedTimer != nil {
		q.delayedTimer.Stop()
	}
	err := q.waitTimeout(t)
	atomic.StoreUint32(&q.status, 2)
	return err
}

func (q *Queue) waitTimeout(t time.Duration) error {
	q.mu.Lock()
	running := q.running
	q.mu.Unlock()
	if running == 0 {
		return nil
	}

	select {
	case <-q.doneCh:
		return nil
	case <-time.After(t):
		return fmt.Errorf("gobeeq: jobs are not processed after %s", t)
	}
}

// IsRunning returns `true` unless the Queue is shutting down
func (q *Queue) IsRunning() bool {
	return atomic.LoadUint32(&q.status) == 0
}

// Destory removes all Redis keys belonging to this queue.
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
	var loadAddJob, loadAddDelayedJob bool
	p := q.redis.Pipeline()
	for i := range jobs {
		_ = jobs[i].save(ctx, p)
		if jobs[i].options.Delay == 0 {
			loadAddJob = true
		} else {
			loadAddDelayedJob = true
		}
	}
	// note: this is to guarantee scipts exist since Run() may return NOSCRIPT
	// in pipeline.
	if loadAddJob {
		b, _ := q.provider.AddJob().Exists(ctx, q.redis).Result()
		if len(b) == 0 || !b[0] {
			err := q.provider.AddJob().Load(ctx, q.redis).Err()
			if err != nil {
				return err
			}
		}
	}
	if loadAddDelayedJob {
		b, _ := q.provider.AddDelayedJob().Exists(ctx, q.redis).Result()
		if len(b) == 0 || !b[0] {
			err := q.provider.AddDelayedJob().Load(ctx, q.redis).Err()
			if err != nil {
				return err
			}
		}
	}
	cmders, err := p.Exec(ctx)
	if err != nil {
		return err
	}
	for i := range jobs {
		v, err := cmders[i].(*redis.Cmd).Result()
		if err != nil {
			return err
		}
		jobs[i].Id = v.(string)
		if jobs[i].options.Delay != 0 && q.settings.ActivateDelayedJobs {
			q.delayedTimer.Schedule(unixMSToTime(jobs[i].options.Delay))
		}
	}
	return nil
}

func (q *Queue) activateDelayed(ctx context.Context) {
	var v interface{}
	err := retry.Do(ctx, func(ctx context.Context, attempt uint) error {
		var err error
		v, err = q.provider.RaiseDelayedJobs().Run(
			ctx,
			q.redis,
			[]string{
				keyDelayed.use(q),
				keyWaiting.use(q),
			},
			timeToUnixMS(time.Now()),
			q.settings.DelayedDebounce.Milliseconds(),
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
	numRaised := vs[0].(int64)
	nextOpportunity := vs[1].(int64)
	if numRaised > 0 {
		if fn := q.onRaised; fn != nil {
			fn(numRaised)
		}
	}
	if nextOpportunity == -1 {
		q.delayedTimer.Schedule(time.Time{})
	} else {
		q.delayedTimer.Schedule(unixMSToTime(nextOpportunity))
	}
}
