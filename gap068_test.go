package idempotency

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// GAP-068：无分布式锁时，并发同 key 请求必须串行，只能执行一次。
func TestExecute_ConcurrentSameKey_ExecutesOnceWithoutDistributedLock(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	cfg := DefaultIdempotencyConfig()
	cfg.Strategy = StrategyDatabaseOnly
	cfg.EnableDistributedLock = false
	svc := NewUnifiedIdempotencyService(nil, db, nil, cfg, nil, nil)

	const workers = 8
	var calls atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := svc.ExecuteWithIdempotencyAndResult(
				context.Background(), "gap068-concurrent", time.Minute,
				func() (interface{}, error) {
					calls.Add(1)
					time.Sleep(20 * time.Millisecond)
					return "done", nil
				},
			)
			errs <- err
		}()
	}

	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	assert.Equal(t, int32(1), calls.Load(),
		"concurrent requests with same key must execute business fn exactly once")
}

// GAP-068：失败记录不代表业务已完成，同一 key 必须允许重试。
func TestExecute_FailedRecord_AllowsRetry(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	cfg := DefaultIdempotencyConfig()
	cfg.Strategy = StrategyDatabaseOnly
	cfg.EnableDistributedLock = false
	svc := NewUnifiedIdempotencyService(nil, db, nil, cfg, nil, nil)

	firstErr := errors.New("transient error")
	_, err := svc.ExecuteWithIdempotencyAndResult(
		context.Background(), "gap068-retry", time.Minute,
		func() (interface{}, error) { return nil, firstErr },
	)
	require.ErrorIs(t, err, firstErr)

	rec, findErr := db.FindByKey(context.Background(), "gap068-retry")
	require.NoError(t, findErr)
	require.NotNil(t, rec)
	assert.Equal(t, RecordStatusFailed, rec.Status)

	result, err := svc.ExecuteWithIdempotencyAndResult(
		context.Background(), "gap068-retry", time.Minute,
		func() (interface{}, error) { return "recovered", nil },
	)
	require.NoError(t, err, "failed record must not block retry")
	assert.Equal(t, "recovered", result.Result)
	assert.False(t, result.IsHit)

	rec, findErr = db.FindByKey(context.Background(), "gap068-retry")
	require.NoError(t, findErr)
	require.NotNil(t, rec)
	assert.Equal(t, RecordStatusCompleted, rec.Status, "retry result must replace failed record")
}

// GAP-068：不同 key 不应被进程内锁互相阻塞（锁按 key 粒度，而非全局）。
func TestExecute_DifferentKeys_DoNotSerialize(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	cfg := DefaultIdempotencyConfig()
	cfg.Strategy = StrategyDatabaseOnly
	cfg.EnableDistributedLock = false
	svc := NewUnifiedIdempotencyService(nil, db, nil, cfg, nil, nil)

	release := make(chan struct{})
	started := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		_, err := svc.ExecuteWithIdempotencyAndResult(
			context.Background(), "gap068-slow", time.Minute,
			func() (interface{}, error) {
				close(started)
				<-release
				return "slow", nil
			},
		)
		done <- err
	}()

	<-started
	fastDone := make(chan error, 1)
	go func() {
		_, err := svc.ExecuteWithIdempotencyAndResult(
			context.Background(), "gap068-fast", time.Minute,
			func() (interface{}, error) { return "fast", nil },
		)
		fastDone <- err
	}()

	select {
	case err := <-fastDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("unrelated key was blocked by a slow in-flight key")
	}

	close(release)
	require.NoError(t, <-done)
}
