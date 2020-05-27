package gobeeq

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v7"
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
	closeStatus     uint32 // 0 -> normal, 1 -> closing, 2 -> closed
	checkTimer      *time.Timer
	stopCh          chan struct{}
	wg              sync.WaitGroup
}

func NewQueue(name string, r *redis.Client, config *Config) (*Queue, error) {
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
	}
	return q, q.ensureScripts()
}

func (q *Queue) keyPrefix() string {
	return q.config.Prefix + ":" + q.name + ":"
}

func (q *Queue) ensureScripts() error {
	if !q.commandable(false) {
		return ErrQueueClosed
	}
	scripts := []*redis.Script{
		q.config.ScriptsProvider.CheckStalledJobs(),
		q.config.ScriptsProvider.AddJob(),
		q.config.ScriptsProvider.RemoveJob(),
	}
	var shas []string
	for _, s := range scripts {
		shas = append(shas, s.Hash())
	}
	evs, err := q.redis.ScriptExists(shas...).Result()
	if err != nil {
		return err
	} else if len(evs) != len(shas) {
		return ErrInvalidResult
	}
	for i := range evs {
		if evs[i] {
			continue
		}
		if _, err := scripts[i].Load(q.redis).Result(); err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) NewJob(data string) *Job {
	return q.newJobWithId("", data, nil)
}

func (q *Queue) newJobWithId(id string, data string, options *Options) *Job {
	if options == nil {
		options = &Options{}
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

type ProcessFunc func(context.Context, *Job) error

func (q *Queue) ProcessMulti(concurrency int64, h func(context.Context, *Job) error) error {
	q.concurrency = concurrency
	return q.Process(h)
}

func (q *Queue) Process(h func(context.Context, *Job) error) error {
	if q.handler != nil {
		return ErrHandlerAlreadyRegistered
	}
	if !q.commandable(true) {
		return ErrQueueClosed
	}
	q.handler = ProcessFunc(h)
	atomic.StoreInt64(&q.running, 0)
	atomic.StoreInt64(&q.queued, 1)
	go func() {
		if err := q.doStalledJobCheck(); err != nil {
			logger.Fatal(err)
		}
		q.jobTick()
	}()
	return nil
}

func (q *Queue) commandable(strict bool) bool {
	status := atomic.LoadUint32(&q.closeStatus)
	return status == 0 || (!strict && status == 1)
}

func (q *Queue) jobTick() {
	if !q.commandable(true) {
		atomic.AddInt64(&q.queued, -1)
		return
	}
	j, err := q.getNextJob()
	if err != nil {
		logger.Fatal(err)
		go q.jobTick()
		return
	}
	if !q.commandable(true) {
		// This job will get picked up later as a stalled job if we happen to get here.
		atomic.AddInt64(&q.queued, -1)
		return
	}
	atomic.AddInt64(&q.running, 1)
	atomic.AddInt64(&q.queued, -1)
	if (q.running + q.queued) < q.concurrency { // TODO: need lock
		atomic.AddInt64(&q.queued, 1)
		go q.jobTick()
	}

	if j == nil {
		go q.jobTick()
		return
	}
	if err := q.runJob(j); err != nil {
		logger.Fatal(err)
	}
	atomic.AddInt64(&q.running, -1)
	atomic.AddInt64(&q.queued, 1)
	go q.jobTick()
}

func (q *Queue) runJob(j *Job) error {
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
		err = q.handler(context.Background(), j)
	} else {
		errc := make(chan error, 1)
		go func() {
			errc <- q.handler(context.Background(), j)
		}()
		select {
		case err = <-errc:
		case <-time.After(time.Duration(j.options.Timeout) * time.Second):
			err = ErrTimeout
		}
	}
	// TODO: backoff
	// finish job
	return q.finishJob(err, j.data, j)
}

func (q *Queue) preventStall(id string) error {
	return q.redis.SRem(keyStalling.use(q), id).Err()
}

func (q *Queue) CheckStalledJobs(interval time.Duration) {
	if err := q.doStalledJobCheck(); err != nil {
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
			if err := q.doStalledJobCheck(); err != nil {
				logger.Fatal(err)
			}
		case <-q.stopCh:
			return
		}
	}
}

func (q *Queue) doStalledJobCheck() error {
	return q.config.ScriptsProvider.CheckStalledJobs().Run(
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

func (q *Queue) getNextJob() (*Job, error) {
	if !q.commandable(true) {
		return nil, ErrQueueClosed
	}
	id, err := q.redis.BRPopLPush(keyWaiting.use(q), keyActive.use(q), 0).Result()
	if err != nil {
		return nil, err
	}
	return Job{}.FromId(q, id)
}

func (q *Queue) finishJob(err error, data interface{}, job *Job) error {
	pip := q.redis.TxPipeline()
	pip.LRem(keyActive.use(q), 0, job.Id)
	pip.SRem(keyStalling.use(q), job.Id)
	if err != nil {
		job.status = StatusRetrying
		// TODO retry
		data, err := job.ToData()
		if err != nil {
			logger.Fatal(err)
		}
		pip.HSet(keyJobs.use(q), job.Id, data)
		pip.LPush(keyWaiting.use(q), job.Id)
	} else {
		job.status = StatusSucceeded
		pip.HDel(keyJobs.use(q), string(job.Id))
	}
	_, err = pip.Exec()
	return err
}

func (q *Queue) removeJob(id string) error {
	return q.config.ScriptsProvider.RemoveJob().Run(
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

func (q *Queue) CheckHealth() (*QueueStatus, error) {
	if !q.commandable(false) {
		return nil, ErrQueueClosed
	}
	pip := q.redis.TxPipeline()
	wv := pip.LLen(keyWaiting.use(q))
	av := pip.LLen(keyActive.use(q))
	sv := pip.SCard(keySucceeded.use(q))
	fv := pip.SCard(keyFailed.use(q))
	dv := pip.ZCard(keyDelayed.use(q))
	kv := pip.Get(keyId.use(q))
	_, err := pip.Exec()
	if err != nil {
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

func (q *Queue) Close() error {
	return q.CloseTimeout(30 * time.Second)
}

func (q *Queue) CloseTimeout(timeout time.Duration) error {
	if !atomic.CompareAndSwapUint32(&q.closeStatus, 0, 1) {
		return ErrQueueClosed
	}
	close(q.stopCh)
	err := q.WaitTimeout(timeout)
	atomic.StoreUint32(&q.closeStatus, 2)
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
	case <-time.After(timeout):
		return fmt.Errorf("bq: jobs are not processed after %s", timeout)
	}
	return nil
}

func (q *Queue) IsRunning() bool {
	return atomic.LoadUint32(&q.closeStatus) == 0
}

func (q *Queue) Destory() error {
	if !q.commandable(false) {
		return ErrQueueClosed
	}
	return q.redis.Del(
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
