package idempotency

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// retryableErr satisfies interface{ Timeout() bool } so isRetryableError returns true.
type retryableErr struct{ msg string }

func (e *retryableErr) Error() string { return e.msg }
func (e *retryableErr) Timeout() bool { return true }

func newTestExecutor(strategy IdempotencyStrategy) *UnifiedExecutor {
	cache := newMockCache()
	db := NewInMemoryIdempotencyRepository()
	cfg := DefaultIdempotencyConfig()
	cfg.Strategy = strategy
	cfg.EnableDistributedLock = false
	cfg.DefaultTTL = time.Minute
	svc := NewUnifiedIdempotencyService(cache, db, nil, cfg, nil, nil)
	execCfg := DefaultExecutorConfig()
	execCfg.Timeout = 0 // disable per-call timeout so retry/cancel tests are deterministic
	execCfg.RetryCount = 0
	return NewUnifiedExecutor(svc, execCfg, nil, nil)
}

func newTestExecutorWithLock(strategy IdempotencyStrategy) (*UnifiedExecutor, *mockLock) {
	cache := newMockCache()
	db := NewInMemoryIdempotencyRepository()
	lock := newBlockingMockLock()
	cfg := DefaultIdempotencyConfig()
	cfg.Strategy = strategy
	cfg.EnableDistributedLock = true
	cfg.LockTTL = 5 * time.Second
	cfg.DefaultTTL = time.Minute
	svc := NewUnifiedIdempotencyService(cache, db, lock, cfg, nil, nil)
	execCfg := DefaultExecutorConfig()
	execCfg.Timeout = 0
	return NewUnifiedExecutor(svc, execCfg, nil, nil), lock
}

// ===== ExecuteWithIdempotency =====

func TestExecutor_ExecuteWithIdempotency_Miss(t *testing.T) {
	e := newTestExecutor(StrategyCacheOnly)
	res, err := e.ExecuteWithIdempotency(context.Background(), "k", time.Minute, func(ctx context.Context) (interface{}, error) {
		return "v", nil
	})
	require.NoError(t, err)
	assert.Equal(t, "v", res)
}

func TestExecutor_ExecuteWithIdempotency_Hit(t *testing.T) {
	e := newTestExecutor(StrategyCacheOnly)
	// prime cache
	_, _ = e.ExecuteWithIdempotency(context.Background(), "k", time.Minute, func(ctx context.Context) (interface{}, error) {
		return "v", nil
	})

	called := int32(0)
	res, err := e.ExecuteWithIdempotency(context.Background(), "k", time.Minute, func(ctx context.Context) (interface{}, error) {
		atomic.AddInt32(&called, 1)
		return "should-not-run", nil
	})
	require.NoError(t, err)
	assert.Equal(t, "v", res)
	assert.Equal(t, int32(0), atomic.LoadInt32(&called))
}

func TestExecutor_ExecuteWithIdempotency_Failure(t *testing.T) {
	e := newTestExecutor(StrategyCacheOnly)
	_, err := e.ExecuteWithIdempotency(context.Background(), "k", time.Minute, func(ctx context.Context) (interface{}, error) {
		return nil, errors.New("boom")
	})
	require.Error(t, err)
}

// ===== executeWithRetry (tested via Execute) =====

func TestExecutor_Retry_SuccessOnFirstAttempt(t *testing.T) {
	cfg := DefaultExecutorConfig()
	cfg.RetryCount = 2
	cfg.RetryDelay = time.Millisecond
	cfg.Timeout = 0
	e := newTestExecutorWithConfig(StrategyCacheOnly, cfg)

	calls := int32(0)
	res, err := e.Execute(context.Background(), func(ctx context.Context) (interface{}, error) {
		atomic.AddInt32(&calls, 1)
		return "ok", nil
	})
	require.NoError(t, err)
	assert.Equal(t, "ok", res)
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls))
}

func TestExecutor_Retry_SuccessAfterRetries(t *testing.T) {
	cfg := DefaultExecutorConfig()
	cfg.RetryCount = 3
	cfg.RetryDelay = time.Millisecond
	cfg.Timeout = 0
	e := newTestExecutorWithConfig(StrategyCacheOnly, cfg)

	calls := int32(0)
	res, err := e.Execute(context.Background(), func(ctx context.Context) (interface{}, error) {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			return nil, &retryableErr{msg: "transient"}
		}
		return "ok-after-retry", nil
	})
	require.NoError(t, err)
	assert.Equal(t, "ok-after-retry", res)
	assert.Equal(t, int32(3), atomic.LoadInt32(&calls))
}

func TestExecutor_Retry_ExceedMaxRetries(t *testing.T) {
	cfg := DefaultExecutorConfig()
	cfg.RetryCount = 2
	cfg.RetryDelay = time.Millisecond
	cfg.Timeout = 0
	e := newTestExecutorWithConfig(StrategyCacheOnly, cfg)

	calls := int32(0)
	_, err := e.Execute(context.Background(), func(ctx context.Context) (interface{}, error) {
		atomic.AddInt32(&calls, 1)
		return nil, &retryableErr{msg: "always-fails"}
	})
	require.Error(t, err)
	// RetryCount=2 → maxAttempts=3
	assert.Equal(t, int32(3), atomic.LoadInt32(&calls))
}

func TestExecutor_Retry_NonRetryableReturnsImmediately(t *testing.T) {
	cfg := DefaultExecutorConfig()
	cfg.RetryCount = 3
	cfg.RetryDelay = time.Millisecond
	cfg.Timeout = 0
	e := newTestExecutorWithConfig(StrategyCacheOnly, cfg)

	calls := int32(0)
	_, err := e.Execute(context.Background(), func(ctx context.Context) (interface{}, error) {
		atomic.AddInt32(&calls, 1)
		return nil, errors.New("non-retryable")
	})
	require.Error(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls), "non-retryable error must not retry")
}

func TestExecutor_Retry_ContextCanceled(t *testing.T) {
	cfg := DefaultExecutorConfig()
	cfg.RetryCount = 3
	cfg.RetryDelay = 50 * time.Millisecond
	cfg.Timeout = 0
	e := newTestExecutorWithConfig(StrategyCacheOnly, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	calls := int32(0)
	// cancel during retry delay
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	_, err := e.Execute(ctx, func(ctx context.Context) (interface{}, error) {
		atomic.AddInt32(&calls, 1)
		return nil, &retryableErr{msg: "transient"}
	})
	require.Error(t, err)
	// context cancelled should propagate
	assert.ErrorIs(t, err, context.Canceled)
}

func newTestExecutorWithConfig(strategy IdempotencyStrategy, execCfg *ExecutorConfig) *UnifiedExecutor {
	cache := newMockCache()
	db := NewInMemoryIdempotencyRepository()
	cfg := DefaultIdempotencyConfig()
	cfg.Strategy = strategy
	cfg.EnableDistributedLock = false
	cfg.DefaultTTL = time.Minute
	svc := NewUnifiedIdempotencyService(cache, db, nil, cfg, nil, nil)
	return NewUnifiedExecutor(svc, execCfg, nil, nil)
}

// ===== ExecuteWithOptions (executor wrapper) =====

func TestExecutor_ExecuteWithOptions_ForceExecute(t *testing.T) {
	e := newTestExecutor(StrategyCacheOnly)
	opts := &ExecutionOptions{Key: "k", TTL: time.Minute, ForceExecute: true}
	res, err := e.ExecuteWithOptions(context.Background(), opts, func(ctx context.Context) (interface{}, error) {
		return "forced", nil
	})
	require.NoError(t, err)
	assert.True(t, res.IsNew)
	assert.Equal(t, "forced", res.Result)
}

func TestExecutor_ExecuteWithOptions_StrategyOverride(t *testing.T) {
	e := newTestExecutor(StrategyCacheOnly)
	opts := &ExecutionOptions{
		Key:      "k",
		TTL:      time.Minute,
		Strategy: StrategyDatabaseOnly,
	}
	res, err := e.ExecuteWithOptions(context.Background(), opts, func(ctx context.Context) (interface{}, error) {
		return "v", nil
	})
	require.NoError(t, err)
	assert.Equal(t, StrategyDatabaseOnly, res.Strategy)
}

func TestExecutor_ExecuteWithOptions_Callbacks(t *testing.T) {
	e := newTestExecutor(StrategyCacheOnly)
	missCalled := int32(0)
	opts := &ExecutionOptions{
		Key: "k", TTL: time.Minute,
		OnMiss: func(*ExecutionResult) { atomic.AddInt32(&missCalled, 1) },
	}
	_, err := e.ExecuteWithOptions(context.Background(), opts, func(ctx context.Context) (interface{}, error) {
		return "v", nil
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&missCalled))
}

// ===== ExecuteBatch =====

func TestExecutor_ExecuteBatch(t *testing.T) {
	e := newTestExecutor(StrategyCacheOnly)
	items := []BatchItem{
		{Key: "a", TTL: time.Minute, Fn: func(ctx context.Context) (interface{}, error) { return 1, nil }},
		{Key: "b", TTL: time.Minute, Fn: func(ctx context.Context) (interface{}, error) { return 2, nil }},
		{Key: "c", TTL: time.Minute, Fn: func(ctx context.Context) (interface{}, error) { return nil, errors.New("fail") }},
	}

	results := e.ExecuteBatch(context.Background(), items)
	require.Len(t, results, 3)
	assert.Equal(t, 1, results[0].Value)
	assert.Equal(t, 2, results[1].Value)
	assert.Error(t, results[2].Error)
}

func TestExecutor_ExecuteBatch_Empty(t *testing.T) {
	e := newTestExecutor(StrategyCacheOnly)
	results := e.ExecuteBatch(context.Background(), nil)
	assert.Len(t, results, 0)
}

// ===== ExecuteParallel =====

func TestExecutor_ExecuteParallel(t *testing.T) {
	e := newTestExecutor(StrategyCacheOnly)
	fns := map[string]ExecutionFunc{
		"x": func(ctx context.Context) (interface{}, error) { return "xv", nil },
		"y": func(ctx context.Context) (interface{}, error) { return "yv", nil },
		"z": func(ctx context.Context) (interface{}, error) { return nil, errors.New("zfail") },
	}

	results := e.ExecuteParallel(context.Background(), fns)
	assert.Equal(t, "xv", results["x"])
	assert.Equal(t, "yv", results["y"])
	_, isErr := results["z"].(error)
	assert.True(t, isErr)
}

// ===== ExecuteWithLock =====

func TestExecutor_ExecuteWithLock(t *testing.T) {
	e, lock := newTestExecutorWithLock(StrategyCacheOnly)
	called := int32(0)
	res, err := e.ExecuteWithLock(context.Background(), "lk", time.Minute, func(ctx context.Context) (interface{}, error) {
		atomic.AddInt32(&called, 1)
		return "v", nil
	})
	require.NoError(t, err)
	assert.Equal(t, "v", res)
	assert.Equal(t, int32(1), atomic.LoadInt32(&called))
	assert.False(t, lock.held("lk"), "lock should be released after execution")
}

func TestExecutor_ExecuteWithLock_NoLockProvider(t *testing.T) {
	// lock=nil → executes without lock, returns result
	e := newTestExecutor(StrategyCacheOnly)
	res, err := e.ExecuteWithLock(context.Background(), "lk", time.Minute, func(ctx context.Context) (interface{}, error) {
		return "v", nil
	})
	require.NoError(t, err)
	assert.Equal(t, "v", res)
}

// ===== ExecuteWithLockAndIdempotency (double-check) =====

func TestExecutor_ExecuteWithLockAndIdempotency_FirstExecutes_SecondHits(t *testing.T) {
	e, _ := newTestExecutorWithLock(StrategyCacheFirst)

	called := int32(0)
	fn := func(ctx context.Context) (interface{}, error) {
		atomic.AddInt32(&called, 1)
		return "v", nil
	}

	// first call: miss → execute → save
	res1, err := e.ExecuteWithLockAndIdempotency(context.Background(), "k", "lk", time.Minute, time.Minute, fn)
	require.NoError(t, err)
	assert.Equal(t, "v", res1)
	assert.Equal(t, int32(1), atomic.LoadInt32(&called))

	// second call: hit → return cached, fn not executed
	res2, err := e.ExecuteWithLockAndIdempotency(context.Background(), "k", "lk", time.Minute, time.Minute, fn)
	require.NoError(t, err)
	assert.Equal(t, "v", res2)
	assert.Equal(t, int32(1), atomic.LoadInt32(&called), "fn must not re-execute on hit")
}

// ===== ExecuteAsync / ExecuteWithCallback =====

func TestExecutor_ExecuteAsync(t *testing.T) {
	e := newTestExecutor(StrategyCacheOnly)
	ch := e.ExecuteAsync(context.Background(), "k", time.Minute, func(ctx context.Context) (interface{}, error) {
		return "v", nil
	})
	require.NotNil(t, ch)

	select {
	case res := <-ch:
		require.NoError(t, res.Error)
		assert.Equal(t, "v", res.Value)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for async result")
	}
}

func TestExecutor_ExecuteWithCallback(t *testing.T) {
	e := newTestExecutor(StrategyCacheOnly)

	done := make(chan interface{}, 1)
	e.ExecuteWithCallback(context.Background(), "k", time.Minute,
		func(ctx context.Context) (interface{}, error) { return "v", nil },
		func(r interface{}) { done <- r },
		nil,
	)
	select {
	case r := <-done:
		assert.Equal(t, "v", r)
	case <-time.After(2 * time.Second):
		t.Fatal("onSuccess not called")
	}
}

func TestExecutor_ExecuteWithCallback_Error(t *testing.T) {
	e := newTestExecutor(StrategyCacheOnly)

	errCh := make(chan error, 1)
	e.ExecuteWithCallback(context.Background(), "k", time.Minute,
		func(ctx context.Context) (interface{}, error) { return nil, errors.New("boom") },
		nil,
		func(err error) { errCh <- err },
	)
	select {
	case err := <-errCh:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("onError not called")
	}
}

// Bug 8 关键测试：Shutdown 必须等待 in-flight goroutine 完成
func TestExecutor_Shutdown_WaitsForInFlight(t *testing.T) {
	e := newTestExecutor(StrategyCacheOnly)

	started := make(chan struct{})
	release := make(chan struct{})
	e.ExecuteWithCallback(context.Background(), "k", time.Minute,
		func(ctx context.Context) (interface{}, error) {
			close(started)
			<-release
			return "v", nil
		},
		nil, nil,
	)

	// wait until fn has started
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("fn never started")
	}

	// Shutdown should block because the goroutine is still running
	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		shutdownDone <- e.Shutdown(ctx)
	}()

	select {
	case <-shutdownDone:
		t.Fatal("Shutdown returned before in-flight goroutine completed")
	case <-time.After(50 * time.Millisecond):
		// expected: Shutdown is still blocking
	}

	// release the goroutine
	close(release)

	select {
	case err := <-shutdownDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown did not return after goroutine completed")
	}
}

func TestExecutor_Shutdown_NoInFlight(t *testing.T) {
	e := newTestExecutor(StrategyCacheOnly)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, e.Shutdown(ctx))
}

func TestExecutor_Shutdown_ContextCancelled(t *testing.T) {
	e := newTestExecutor(StrategyCacheOnly)

	release := make(chan struct{})
	e.ExecuteWithCallback(context.Background(), "k", time.Minute,
		func(ctx context.Context) (interface{}, error) {
			<-release
			return "v", nil
		},
		nil, nil,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := e.Shutdown(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	close(release)
	// allow goroutine to finish to avoid leaking into other tests
	time.Sleep(20 * time.Millisecond)
}

// Bug 9: MaxConcurrent==0 不应死锁，应兜底为默认值
func TestExecutor_MaxConcurrentZero_DoesNotDeadlock(t *testing.T) {
	cache := newMockCache()
	db := NewInMemoryIdempotencyRepository()
	svcCfg := DefaultIdempotencyConfig()
	svcCfg.Strategy = StrategyCacheOnly
	svcCfg.EnableDistributedLock = false
	svc := NewUnifiedIdempotencyService(cache, db, nil, svcCfg, nil, nil)

	execCfg := &ExecutorConfig{MaxConcurrent: 0} // should be guarded to 10
	e := NewUnifiedExecutor(svc, execCfg, nil, nil)

	done := make(chan error, 1)
	go func() {
		_, err := e.Execute(context.Background(), func(ctx context.Context) (interface{}, error) {
			return "ok", nil
		})
		done <- err
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("MaxConcurrent=0 caused deadlock")
	}
}

// MaxConcurrent 并发上限测试
func TestExecutor_MaxConcurrent_Limit(t *testing.T) {
	cache := newMockCache()
	db := NewInMemoryIdempotencyRepository()
	svcCfg := DefaultIdempotencyConfig()
	svcCfg.Strategy = StrategyCacheOnly
	svcCfg.EnableDistributedLock = false
	svc := NewUnifiedIdempotencyService(cache, db, nil, svcCfg, nil, nil)

	execCfg := &ExecutorConfig{MaxConcurrent: 2}
	e := NewUnifiedExecutor(svc, execCfg, nil, nil)

	var running int32
	var maxRunning int32
	var mu sync.Mutex

	block := make(chan struct{})
	fn := func(ctx context.Context) (interface{}, error) {
		cur := atomic.AddInt32(&running, 1)
		mu.Lock()
		if cur > maxRunning {
			maxRunning = cur
		}
		mu.Unlock()
		<-block
		atomic.AddInt32(&running, -1)
		return "ok", nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = e.Execute(context.Background(), func(ctx context.Context) (interface{}, error) {
				_ = i
				return fn(ctx)
			})
		}(i)
	}

	// let tasks run up to the limit
	time.Sleep(50 * time.Millisecond)
	close(block)
	wg.Wait()

	assert.Equal(t, int32(2), maxRunning, "at most MaxConcurrent tasks should run simultaneously")
}

// ===== GetStats =====

func TestExecutor_GetStats(t *testing.T) {
	e := newTestExecutor(StrategyCacheFirst)
	stats := e.GetStats()
	assert.Equal(t, 100, stats["max_concurrent"])
	assert.Equal(t, 0, stats["retry_count"])
	assert.True(t, stats["enable_tracing"].(bool))
}

func TestExecutorConfigBuilder(t *testing.T) {
	cfg := NewExecutorConfigBuilder().
		WithDefaultTTL(time.Hour).
		WithMaxConcurrent(50).
		WithTimeout(10*time.Second).
		WithTracing(false).
		WithMetrics(false).
		WithRetry(5, 200*time.Millisecond).
		Build()
	assert.Equal(t, time.Hour, cfg.DefaultTTL)
	assert.Equal(t, 50, cfg.MaxConcurrent)
	assert.Equal(t, 10*time.Second, cfg.Timeout)
	assert.False(t, cfg.EnableTracing)
	assert.False(t, cfg.EnableMetrics)
	assert.Equal(t, 5, cfg.RetryCount)
	assert.Equal(t, 200*time.Millisecond, cfg.RetryDelay)
}

func TestExecutorBuilder(t *testing.T) {
	cache := newMockCache()
	db := NewInMemoryIdempotencyRepository()
	svcCfg := DefaultIdempotencyConfig()
	svcCfg.EnableDistributedLock = false
	svc := NewUnifiedIdempotencyService(cache, db, nil, svcCfg, nil, nil)

	e := NewExecutorBuilder().
		WithService(svc).
		WithConfig(DefaultExecutorConfig()).
		Build()
	require.NotNil(t, e)
}

func TestExecutionContext(t *testing.T) {
	c := NewExecutionContext("k", "op").
		WithUserID(1).
		WithTenantID(2).
		WithRequestID("r").
		WithMetadata("key", "val").
		WithAttempt(1, 3)
	assert.Equal(t, "k", c.Key)
	assert.Equal(t, "op", c.Operation)
	assert.Equal(t, int64(1), c.UserID)
	assert.Equal(t, int64(2), c.TenantID)
	assert.Equal(t, "r", c.RequestID)
	assert.Equal(t, "val", c.Metadata["key"])
	assert.Equal(t, 1, c.Attempt)
	assert.Equal(t, 3, c.MaxAttempts)
	assert.True(t, c.Duration() >= 0)
}

func TestExecutionChain(t *testing.T) {
	e := newTestExecutor(StrategyCacheOnly)
	chain := NewExecutionChain(e).
		AddStep("s1", func(ctx context.Context) (interface{}, error) { return 1, nil }).
		AddStep("s2", func(ctx context.Context) (interface{}, error) { return 2, nil })

	results, err := chain.Execute(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, results["s1"])
	assert.Equal(t, 2, results["s2"])
}

func TestExecutionChain_StepFails(t *testing.T) {
	e := newTestExecutor(StrategyCacheOnly)
	chain := NewExecutionChain(e).
		AddStep("s1", func(ctx context.Context) (interface{}, error) { return 1, nil }).
		AddStep("s2", func(ctx context.Context) (interface{}, error) { return nil, fmt.Errorf("step err") })

	_, err := chain.Execute(context.Background())
	require.Error(t, err)
}

func TestExecutor_ExecuteWithTimeout(t *testing.T) {
	e := newTestExecutor(StrategyCacheOnly)
	res, err := e.ExecuteWithTimeout(context.Background(), 2*time.Second, func(ctx context.Context) (interface{}, error) {
		return "v", nil
	})
	require.NoError(t, err)
	assert.Equal(t, "v", res)
}
