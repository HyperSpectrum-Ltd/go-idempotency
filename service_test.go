package idempotency

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ===== Test helpers: mock cache / mock lock =====

type mockCache struct {
	mu       sync.Mutex
	data     map[string]interface{}
	setErr   error
	getErr   error
	delErr   error
	existsFn func(key string) bool
}

func newMockCache() *mockCache {
	return &mockCache{data: make(map[string]interface{})}
}

func (m *mockCache) Get(_ context.Context, key string) (interface{}, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getErr != nil {
		return nil, false, m.getErr
	}
	v, ok := m.data[key]
	return v, ok, nil
}

func (m *mockCache) Set(_ context.Context, key string, value interface{}, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.setErr != nil {
		return m.setErr
	}
	m.data[key] = value
	return nil
}

func (m *mockCache) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.delErr != nil {
		return m.delErr
	}
	delete(m.data, key)
	return nil
}

func (m *mockCache) Exists(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.data[key]
	return ok, nil
}

// mockLock is a distributed lock provider for tests.
// blocking=true: Lock blocks (polling) until available or ctx done.
// blocking=false: Lock returns acquired=false immediately if held.
type mockLock struct {
	mu       sync.Mutex
	holders  map[string]string
	blocking bool
	lockErr  error
}

func newBlockingMockLock() *mockLock {
	return &mockLock{holders: make(map[string]string), blocking: true}
}

func newFailFastMockLock() *mockLock {
	return &mockLock{holders: make(map[string]string), blocking: false}
}

func (m *mockLock) Lock(ctx context.Context, key string, _ time.Duration) (string, bool, error) {
	if m.lockErr != nil {
		return "", false, m.lockErr
	}
	for {
		m.mu.Lock()
		if _, exists := m.holders[key]; !exists {
			v := uuid.New().String()
			m.holders[key] = v
			m.mu.Unlock()
			return v, true, nil
		}
		m.mu.Unlock()
		if !m.blocking {
			return "", false, nil
		}
		select {
		case <-ctx.Done():
			return "", false, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func (m *mockLock) Unlock(_ context.Context, key, lockValue string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.holders[key]; ok && v == lockValue {
		delete(m.holders, key)
	}
	return nil
}

func (m *mockLock) WithLock(ctx context.Context, key string, ttl time.Duration, fn func() error) error {
	v, acquired, err := m.Lock(ctx, key, ttl)
	if err != nil {
		return err
	}
	if !acquired {
		return ErrDistributedLock
	}
	defer m.Unlock(ctx, key, v)
	return fn()
}

func (m *mockLock) held(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.holders[key]
	return ok
}

// newTestService builds a service with sensible defaults for tests.
func newTestService(cache CacheProvider, db IdempotencyRepository, lock DistributedLockProvider, strategy IdempotencyStrategy) *UnifiedIdempotencyService {
	cfg := DefaultIdempotencyConfig()
	cfg.Strategy = strategy
	cfg.DefaultTTL = time.Minute
	return NewUnifiedIdempotencyService(cache, db, lock, cfg, nil, nil)
}

// ===== Check: StrategyCacheOnly =====

func TestCheck_CacheOnly_Hit(t *testing.T) {
	cache := newMockCache()
	_ = cache.Set(context.Background(), "k", "v", time.Minute)
	svc := newTestService(cache, nil, nil, StrategyCacheOnly)

	exists, res, err := svc.Check(context.Background(), "k")
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, "v", res)
}

func TestCheck_CacheOnly_Miss(t *testing.T) {
	cache := newMockCache()
	svc := newTestService(cache, nil, nil, StrategyCacheOnly)

	exists, _, err := svc.Check(context.Background(), "missing")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestCheck_CacheOnly_NilCache(t *testing.T) {
	svc := newTestService(nil, nil, nil, StrategyCacheOnly)
	_, _, err := svc.Check(context.Background(), "k")
	require.Error(t, err)
}

func TestCheck_CacheOnly_GetError(t *testing.T) {
	cache := newMockCache()
	cache.getErr = errors.New("cache down")
	svc := newTestService(cache, nil, nil, StrategyCacheOnly)
	_, _, err := svc.Check(context.Background(), "k")
	require.Error(t, err)
}

// ===== Check: StrategyDatabaseOnly =====

func TestCheck_DatabaseOnly_Hit(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	now := time.Now()
	_ = db.Save(context.Background(), &IdempotencyRecord{
		Key: "k", Result: []byte(`"v"`), Status: RecordStatusCompleted,
		ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	})
	svc := newTestService(nil, db, nil, StrategyDatabaseOnly)

	exists, res, err := svc.Check(context.Background(), "k")
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, "v", res)
}

func TestCheck_DatabaseOnly_Miss(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	svc := newTestService(nil, db, nil, StrategyDatabaseOnly)

	exists, _, err := svc.Check(context.Background(), "missing")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestCheck_DatabaseOnly_Expired(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	now := time.Now()
	_ = db.Save(context.Background(), &IdempotencyRecord{
		Key: "k", Result: []byte(`"v"`), Status: RecordStatusCompleted,
		ExpiresAt: now.Add(-time.Minute), CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour),
	})
	svc := newTestService(nil, db, nil, StrategyDatabaseOnly)

	_, _, err := svc.Check(context.Background(), "k")
	require.ErrorIs(t, err, ErrKeyExpired)

	// record should have been deleted
	rec, _ := db.FindByKey(context.Background(), "k")
	assert.Nil(t, rec)
}

func TestCheck_DatabaseOnly_Processing(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	now := time.Now()
	_ = db.Save(context.Background(), &IdempotencyRecord{
		Key: "k", Status: RecordStatusProcessing,
		ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	})
	svc := newTestService(nil, db, nil, StrategyDatabaseOnly)

	exists, _, err := svc.Check(context.Background(), "k")
	require.ErrorIs(t, err, ErrProcessing)
	assert.True(t, exists)
}

func TestCheck_DatabaseOnly_Failed(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	now := time.Now()
	_ = db.Save(context.Background(), &IdempotencyRecord{
		Key: "k", Status: RecordStatusFailed,
		ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	})
	svc := newTestService(nil, db, nil, StrategyDatabaseOnly)

	exists, _, err := svc.Check(context.Background(), "k")
	require.ErrorIs(t, err, ErrExecutionFailed)
	assert.True(t, exists)
}

func TestCheck_DatabaseOnly_NilDB(t *testing.T) {
	svc := newTestService(nil, nil, nil, StrategyDatabaseOnly)
	_, _, err := svc.Check(context.Background(), "k")
	require.Error(t, err)
}

// ===== Check: StrategyCacheFirst =====

func TestCheck_CacheFirst_CacheHit(t *testing.T) {
	cache := newMockCache()
	_ = cache.Set(context.Background(), "k", "v", time.Minute)
	db := NewInMemoryIdempotencyRepository()
	svc := newTestService(cache, db, nil, StrategyCacheFirst)

	exists, res, err := svc.Check(context.Background(), "k")
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, "v", res)
}

func TestCheck_CacheFirst_CacheMissDbHit_Backfill(t *testing.T) {
	cache := newMockCache()
	db := NewInMemoryIdempotencyRepository()
	now := time.Now()
	_ = db.Save(context.Background(), &IdempotencyRecord{
		Key: "k", Result: []byte(`"v"`), Status: RecordStatusCompleted,
		ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	})
	svc := newTestService(cache, db, nil, StrategyCacheFirst)

	exists, res, err := svc.Check(context.Background(), "k")
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, "v", res)

	// cache should have been backfilled
	cached, ok, _ := cache.Get(context.Background(), "k")
	assert.True(t, ok)
	assert.Equal(t, "v", cached)
}

func TestCheck_CacheFirst_BothMiss(t *testing.T) {
	cache := newMockCache()
	db := NewInMemoryIdempotencyRepository()
	svc := newTestService(cache, db, nil, StrategyCacheFirst)

	exists, _, err := svc.Check(context.Background(), "missing")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestCheck_CacheFirst_DbProcessing(t *testing.T) {
	cache := newMockCache()
	db := NewInMemoryIdempotencyRepository()
	now := time.Now()
	_ = db.Save(context.Background(), &IdempotencyRecord{
		Key: "k", Status: RecordStatusProcessing,
		ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	})
	svc := newTestService(cache, db, nil, StrategyCacheFirst)

	_, _, err := svc.Check(context.Background(), "k")
	require.ErrorIs(t, err, ErrProcessing)
}

func TestCheck_CacheFirst_NilDB_NoPanic(t *testing.T) {
	cache := newMockCache()
	svc := newTestService(cache, nil, nil, StrategyCacheFirst)

	exists, _, err := svc.Check(context.Background(), "missing")
	require.NoError(t, err)
	assert.False(t, exists)
}

// ===== Check: StrategyWriteBoth =====

func TestCheck_WriteBoth_CacheHit(t *testing.T) {
	cache := newMockCache()
	_ = cache.Set(context.Background(), "k", "v", time.Minute)
	db := NewInMemoryIdempotencyRepository()
	svc := newTestService(cache, db, nil, StrategyWriteBoth)

	exists, res, err := svc.Check(context.Background(), "k")
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, "v", res)
}

func TestCheck_WriteBoth_DbHit(t *testing.T) {
	cache := newMockCache()
	db := NewInMemoryIdempotencyRepository()
	now := time.Now()
	_ = db.Save(context.Background(), &IdempotencyRecord{
		Key: "k", Result: []byte(`"v"`), Status: RecordStatusCompleted,
		ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	})
	svc := newTestService(cache, db, nil, StrategyWriteBoth)

	exists, res, err := svc.Check(context.Background(), "k")
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, "v", res)
}

func TestCheck_WriteBoth_BothMiss(t *testing.T) {
	cache := newMockCache()
	db := NewInMemoryIdempotencyRepository()
	svc := newTestService(cache, db, nil, StrategyWriteBoth)

	exists, _, err := svc.Check(context.Background(), "missing")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestCheck_WriteBoth_BothNil(t *testing.T) {
	svc := newTestService(nil, nil, nil, StrategyWriteBoth)
	_, _, err := svc.Check(context.Background(), "k")
	require.Error(t, err)
}

func TestCheck_InvalidStrategy(t *testing.T) {
	svc := newTestService(newMockCache(), nil, nil, "bogus")
	_, _, err := svc.Check(context.Background(), "k")
	require.ErrorIs(t, err, ErrInvalidStrategy)
}

// ===== Save =====

func TestSave_CacheOnly(t *testing.T) {
	cache := newMockCache()
	svc := newTestService(cache, nil, nil, StrategyCacheOnly)
	require.NoError(t, svc.Save(context.Background(), "k", "v", time.Minute))

	v, ok, _ := cache.Get(context.Background(), "k")
	assert.True(t, ok)
	assert.Equal(t, "v", v)
}

func TestSave_DatabaseOnly(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	svc := newTestService(nil, db, nil, StrategyDatabaseOnly)
	require.NoError(t, svc.Save(context.Background(), "k", "v", time.Minute))

	rec, err := db.FindByKey(context.Background(), "k")
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, RecordStatusCompleted, rec.Status)
	assert.Equal(t, `"v"`, string(rec.Result))
}

func TestSave_CacheFirst_Both(t *testing.T) {
	cache := newMockCache()
	db := NewInMemoryIdempotencyRepository()
	svc := newTestService(cache, db, nil, StrategyCacheFirst)
	require.NoError(t, svc.Save(context.Background(), "k", "v", time.Minute))

	_, ok, _ := cache.Get(context.Background(), "k")
	assert.True(t, ok)
	rec, _ := db.FindByKey(context.Background(), "k")
	require.NotNil(t, rec)
}

func TestSave_WriteBoth_CacheNil(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	svc := newTestService(nil, db, nil, StrategyWriteBoth)
	require.NoError(t, svc.Save(context.Background(), "k", "v", time.Minute))
	rec, _ := db.FindByKey(context.Background(), "k")
	require.NotNil(t, rec)
}

func TestSave_WriteBoth_DbNil(t *testing.T) {
	cache := newMockCache()
	svc := newTestService(cache, nil, nil, StrategyWriteBoth)
	err := svc.Save(context.Background(), "k", "v", time.Minute)
	require.Error(t, err)
}

func TestSave_CacheOnly_NilCache(t *testing.T) {
	svc := newTestService(nil, nil, nil, StrategyCacheOnly)
	require.Error(t, svc.Save(context.Background(), "k", "v", time.Minute))
}

func TestSave_DatabaseOnly_NilDB(t *testing.T) {
	svc := newTestService(nil, nil, nil, StrategyDatabaseOnly)
	require.Error(t, svc.Save(context.Background(), "k", "v", time.Minute))
}

func TestSave_InvalidStrategy(t *testing.T) {
	svc := newTestService(newMockCache(), NewInMemoryIdempotencyRepository(), nil, "bogus")
	require.ErrorIs(t, svc.Save(context.Background(), "k", "v", time.Minute), ErrInvalidStrategy)
}

func TestSave_DefaultTTL_WhenZero(t *testing.T) {
	cache := newMockCache()
	cfg := DefaultIdempotencyConfig()
	cfg.Strategy = StrategyCacheOnly
	cfg.DefaultTTL = 2 * time.Minute
	svc := NewUnifiedIdempotencyService(cache, nil, nil, cfg, nil, nil)
	require.NoError(t, svc.Save(context.Background(), "k", "v", 0))
	_, ok, _ := cache.Get(context.Background(), "k")
	assert.True(t, ok)
}

// ===== Delete =====

func TestDelete_BothSucceed(t *testing.T) {
	cache := newMockCache()
	db := NewInMemoryIdempotencyRepository()
	_ = cache.Set(context.Background(), "k", "v", time.Minute)
	_ = db.Save(context.Background(), &IdempotencyRecord{Key: "k", ExpiresAt: time.Now().Add(time.Hour)})
	svc := newTestService(cache, db, nil, StrategyCacheFirst)

	require.NoError(t, svc.Delete(context.Background(), "k"))
}

func TestDelete_CacheError_Wrapped(t *testing.T) {
	cache := newMockCache()
	cache.delErr = ErrKeyNotFound
	svc := newTestService(cache, nil, nil, StrategyCacheOnly)
	err := svc.Delete(context.Background(), "k")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestDelete_BothError_Joined(t *testing.T) {
	cache := newMockCache()
	cache.delErr = errors.New("cache boom")
	failingDB := &failingDeleteRepo{}
	cfg := DefaultIdempotencyConfig()
	cfg.Strategy = StrategyCacheFirst
	svc := NewUnifiedIdempotencyService(cache, failingDB, nil, cfg, nil, nil)

	err := svc.Delete(context.Background(), "k")
	require.Error(t, err)
	// errors.Join preserves both wrapped errors (Bug 11)
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

// failingDeleteRepo wraps InMemoryIdempotencyRepository but fails Delete.
type failingDeleteRepo struct {
	*InMemoryIdempotencyRepository
}

func (f *failingDeleteRepo) Delete(ctx context.Context, key string) error {
	return ErrKeyNotFound
}

func TestDelete_NilCache_NilDB(t *testing.T) {
	svc := newTestService(nil, nil, nil, StrategyCacheFirst)
	require.NoError(t, svc.Delete(context.Background(), "k"))
}

// ===== TryAcquire / Release (Bug 4 semantic fix) =====

func TestTryAcquire_DistributedLockProvider(t *testing.T) {
	lock := newFailFastMockLock()
	cfg := DefaultIdempotencyConfig()
	cfg.EnableDistributedLock = true
	svc := NewUnifiedIdempotencyService(nil, nil, lock, cfg, nil, nil)

	v, acquired, err := svc.TryAcquire(context.Background(), "k", time.Minute)
	require.NoError(t, err)
	assert.True(t, acquired)
	assert.NotEmpty(t, v)

	// second acquire fails (held)
	_, acquired2, err := svc.TryAcquire(context.Background(), "k", time.Minute)
	require.NoError(t, err)
	assert.False(t, acquired2, "lock already held should not be re-acquired")

	// release then re-acquire
	require.NoError(t, svc.Release(context.Background(), "k", v))
	v2, acquired3, _ := svc.TryAcquire(context.Background(), "k", time.Minute)
	assert.True(t, acquired3)
	_ = svc.Release(context.Background(), "k", v2)
}

func TestTryAcquire_DatabaseLock(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	cfg := DefaultIdempotencyConfig()
	cfg.EnableDistributedLock = false // skip distributed lock provider path
	svc := NewUnifiedIdempotencyService(nil, db, nil, cfg, nil, nil)

	v, acquired, err := svc.TryAcquire(context.Background(), "k", time.Minute)
	require.NoError(t, err)
	assert.True(t, acquired)
	assert.NotEmpty(t, v)

	// held by db
	_, acquired2, _ := svc.TryAcquire(context.Background(), "k", time.Minute)
	assert.False(t, acquired2)

	require.NoError(t, svc.Release(context.Background(), "k", v))
}

func TestTryAcquire_CacheLock_SemanticsFixed(t *testing.T) {
	cache := newMockCache()
	cfg := DefaultIdempotencyConfig()
	cfg.EnableDistributedLock = false // no lock provider, no db → cache lock path
	svc := NewUnifiedIdempotencyService(cache, nil, nil, cfg, nil, nil)

	// First acquire: not held → success
	v1, acquired1, err := svc.TryAcquire(context.Background(), "k", time.Minute)
	require.NoError(t, err)
	assert.True(t, acquired1, "first acquire on empty lock should succeed")
	assert.NotEmpty(t, v1)

	// Bug 4: lock already exists → acquired must be FALSE (previously returned true)
	_, acquired2, err := svc.TryAcquire(context.Background(), "k", time.Minute)
	require.NoError(t, err)
	assert.False(t, acquired2, "lock already held must NOT report acquired=true")

	require.NoError(t, svc.Release(context.Background(), "k", v1))
}

func TestTryAcquire_NoProvider_ReturnsError(t *testing.T) {
	cfg := DefaultIdempotencyConfig()
	cfg.EnableDistributedLock = false
	svc := NewUnifiedIdempotencyService(nil, nil, nil, cfg, nil, nil)

	// Bug 4: 无任何锁提供者时返回错误，而非假成功
	_, acquired, err := svc.TryAcquire(context.Background(), "k", time.Minute)
	require.Error(t, err)
	assert.False(t, acquired)
}

// ===== ExecuteWithIdempotencyAndResult =====

func TestExecute_Hit_ReturnsCached(t *testing.T) {
	cache := newMockCache()
	_ = cache.Set(context.Background(), "k", "cached", time.Minute)
	cfg := DefaultIdempotencyConfig()
	cfg.Strategy = StrategyCacheOnly
	cfg.EnableDistributedLock = false
	svc := NewUnifiedIdempotencyService(cache, nil, nil, cfg, nil, nil)

	called := int32(0)
	res, err := svc.ExecuteWithIdempotencyAndResult(context.Background(), "k", time.Minute, func() (interface{}, error) {
		atomic.AddInt32(&called, 1)
		return "fresh", nil
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.True(t, res.IsHit)
	assert.False(t, res.IsNew)
	assert.Equal(t, "cached", res.Result)
	assert.Equal(t, int32(0), atomic.LoadInt32(&called))
}

func TestExecute_Miss_ExecutesAndSaves(t *testing.T) {
	cache := newMockCache()
	cfg := DefaultIdempotencyConfig()
	cfg.Strategy = StrategyCacheOnly
	cfg.EnableDistributedLock = false
	svc := NewUnifiedIdempotencyService(cache, nil, nil, cfg, nil, nil)

	res, err := svc.ExecuteWithIdempotencyAndResult(context.Background(), "k", time.Minute, func() (interface{}, error) {
		return "fresh", nil
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.False(t, res.IsHit)
	assert.True(t, res.IsNew)
	assert.Equal(t, "fresh", res.Result)

	// saved to cache
	v, ok, _ := cache.Get(context.Background(), "k")
	assert.True(t, ok)
	assert.Equal(t, "fresh", v)
}

func TestExecute_FnError_SavesFailedStatus(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	cfg := DefaultIdempotencyConfig()
	cfg.Strategy = StrategyDatabaseOnly
	cfg.EnableDistributedLock = false
	svc := NewUnifiedIdempotencyService(nil, db, nil, cfg, nil, nil)

	execErr := errors.New("boom")
	_, err := svc.ExecuteWithIdempotencyAndResult(context.Background(), "k", time.Minute, func() (interface{}, error) {
		return nil, execErr
	})
	require.ErrorIs(t, err, execErr)

	rec, _ := db.FindByKey(context.Background(), "k")
	require.NotNil(t, rec)
	assert.Equal(t, RecordStatusFailed, rec.Status)
	assert.Equal(t, "boom", rec.ErrorMsg)
}

func TestExecute_FnError_CacheOnly_NoSave(t *testing.T) {
	cache := newMockCache()
	cfg := DefaultIdempotencyConfig()
	cfg.Strategy = StrategyCacheOnly
	cfg.EnableDistributedLock = false
	svc := NewUnifiedIdempotencyService(cache, nil, nil, cfg, nil, nil)

	_, err := svc.ExecuteWithIdempotencyAndResult(context.Background(), "k", time.Minute, func() (interface{}, error) {
		return nil, errors.New("fail")
	})
	require.Error(t, err)

	// CacheOnly should not save failed status (no db)
	_, ok, _ := cache.Get(context.Background(), "k")
	assert.False(t, ok)
}

func TestExecute_ErrProcessing_DoesNotReexecute(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	now := time.Now()
	_ = db.Save(context.Background(), &IdempotencyRecord{
		Key: "k", Status: RecordStatusProcessing,
		ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	})
	cfg := DefaultIdempotencyConfig()
	cfg.Strategy = StrategyDatabaseOnly
	cfg.EnableDistributedLock = false
	svc := NewUnifiedIdempotencyService(nil, db, nil, cfg, nil, nil)

	called := int32(0)
	_, err := svc.ExecuteWithIdempotencyAndResult(context.Background(), "k", time.Minute, func() (interface{}, error) {
		atomic.AddInt32(&called, 1)
		return "should-not-run", nil
	})
	// Bug 5: 正在处理中时不应重复执行
	require.ErrorIs(t, err, ErrProcessing)
	assert.Equal(t, int32(0), atomic.LoadInt32(&called), "fn must not execute when processing")
}

func TestExecute_KeyExpired_Reexecutes(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	now := time.Now()
	_ = db.Save(context.Background(), &IdempotencyRecord{
		Key: "k", Result: []byte(`"old"`), Status: RecordStatusCompleted,
		ExpiresAt: now.Add(-time.Minute), CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour),
	})
	cfg := DefaultIdempotencyConfig()
	cfg.Strategy = StrategyDatabaseOnly
	cfg.EnableDistributedLock = false
	svc := NewUnifiedIdempotencyService(nil, db, nil, cfg, nil, nil)

	res, err := svc.ExecuteWithIdempotencyAndResult(context.Background(), "k", time.Minute, func() (interface{}, error) {
		return "fresh", nil
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.True(t, res.IsNew)
	assert.Equal(t, "fresh", res.Result)
}

// Bug 1 关键测试：同一 key 并发调用，fn() 只执行一次（持锁 + 双重检查）
func TestExecute_ConcurrentSameKey_LockPath_FnRunsOnce(t *testing.T) {
	cache := newMockCache()
	db := NewInMemoryIdempotencyRepository()
	lock := newBlockingMockLock()
	cfg := DefaultIdempotencyConfig()
	cfg.Strategy = StrategyCacheFirst
	cfg.EnableDistributedLock = true
	cfg.LockTTL = 5 * time.Second
	cfg.DefaultTTL = time.Minute
	svc := NewUnifiedIdempotencyService(cache, db, lock, cfg, nil, nil)

	var execCount int32
	fn := func() (interface{}, error) {
		atomic.AddInt32(&execCount, 1)
		// 模拟耗时，使并发请求重叠
		time.Sleep(50 * time.Millisecond)
		return "result", nil
	}

	const n = 10
	var wg sync.WaitGroup
	results := make([]string, n)
	errs := make([]error, n)
	start := make(chan struct{})
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			res, err := svc.ExecuteWithIdempotencyAndResult(context.Background(), "key-concurrent", time.Minute, fn)
			errs[i] = err
			if err == nil && res != nil {
				if s, ok := res.Result.(string); ok {
					results[i] = s
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()

	// fn 必须只执行一次
	assert.Equal(t, int32(1), atomic.LoadInt32(&execCount),
		"fn() must execute exactly once under concurrent same-key requests (Bug 1)")

	// 所有请求都应成功且拿到相同结果
	for i, err := range errs {
		require.NoErrorf(t, err, "goroutine %d", i)
		assert.Equalf(t, "result", results[i], "goroutine %d result mismatch", i)
	}

	// 结果已落库
	rec, _ := db.FindByKey(context.Background(), "key-concurrent")
	require.NotNil(t, rec)
}

// Bug 1 辅助测试：fail-fast 锁下，并发持锁失败返回 ErrDistributedLock
func TestExecute_ConcurrentSameKey_FailFastLock_SecondGetsErr(t *testing.T) {
	cache := newMockCache()
	db := NewInMemoryIdempotencyRepository()
	lock := newFailFastMockLock()
	cfg := DefaultIdempotencyConfig()
	cfg.Strategy = StrategyCacheFirst
	cfg.EnableDistributedLock = true
	cfg.LockTTL = 5 * time.Second
	svc := NewUnifiedIdempotencyService(cache, db, lock, cfg, nil, nil)

	var execCount int32
	fn := func() (interface{}, error) {
		atomic.AddInt32(&execCount, 1)
		time.Sleep(50 * time.Millisecond)
		return "result", nil
	}

	const n = 5
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := svc.ExecuteWithIdempotencyAndResult(context.Background(), "k-ff", time.Minute, fn)
			errs[i] = err
		}(i)
	}
	close(start)
	wg.Wait()

	// fn 只执行一次
	assert.Equal(t, int32(1), atomic.LoadInt32(&execCount))

	// 至少一个请求拿到 ErrDistributedLock（其余可能成功）
	lockErrors := 0
	for _, err := range errs {
		if errors.Is(err, ErrDistributedLock) {
			lockErrors++
		}
	}
	assert.GreaterOrEqual(t, lockErrors, 1, "at least one goroutine should get ErrDistributedLock")
}

func TestExecute_DistributedLockEnabled_NoLockProvider_FallsBack(t *testing.T) {
	// EnableDistributedLock=true 但 lock=nil → 回退到无锁路径
	cache := newMockCache()
	cfg := DefaultIdempotencyConfig()
	cfg.Strategy = StrategyCacheOnly
	cfg.EnableDistributedLock = true
	svc := NewUnifiedIdempotencyService(cache, nil, nil, cfg, nil, nil)

	res, err := svc.ExecuteWithIdempotencyAndResult(context.Background(), "k", time.Minute, func() (interface{}, error) {
		return "v", nil
	})
	require.NoError(t, err)
	assert.Equal(t, "v", res.Result)
}

// ===== ExecuteWithOptions =====

func TestExecuteWithOptions_ForceExecute(t *testing.T) {
	cache := newMockCache()
	_ = cache.Set(context.Background(), "k", "cached", time.Minute)
	cfg := DefaultIdempotencyConfig()
	cfg.Strategy = StrategyCacheOnly
	cfg.EnableDistributedLock = false
	svc := NewUnifiedIdempotencyService(cache, nil, nil, cfg, nil, nil)

	called := int32(0)
	opts := &ExecutionOptions{
		Key:          "k",
		TTL:          time.Minute,
		ForceExecute: true,
	}
	res, err := svc.ExecuteWithOptions(context.Background(), opts, func() (interface{}, error) {
		atomic.AddInt32(&called, 1)
		return "forced", nil
	})
	require.NoError(t, err)
	assert.True(t, res.IsNew)
	assert.Equal(t, "forced", res.Result)
	assert.Equal(t, int32(1), atomic.LoadInt32(&called))
}

func TestExecuteWithOptions_Callbacks(t *testing.T) {
	cache := newMockCache()
	cfg := DefaultIdempotencyConfig()
	cfg.Strategy = StrategyCacheOnly
	cfg.EnableDistributedLock = false
	svc := NewUnifiedIdempotencyService(cache, nil, nil, cfg, nil, nil)

	// Miss path → OnMiss
	missCalled := int32(0)
	opts := &ExecutionOptions{
		Key: "k",
		TTL: time.Minute,
		OnMiss: func(*ExecutionResult) {
			atomic.AddInt32(&missCalled, 1)
		},
	}
	_, err := svc.ExecuteWithOptions(context.Background(), opts, func() (interface{}, error) {
		return "v", nil
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&missCalled))

	// Hit path → OnHit
	hitCalled := int32(0)
	opts2 := &ExecutionOptions{
		Key: "k",
		TTL: time.Minute,
		OnHit: func(*ExecutionResult) {
			atomic.AddInt32(&hitCalled, 1)
		},
	}
	_, err = svc.ExecuteWithOptions(context.Background(), opts2, func() (interface{}, error) {
		return "should-not-run", nil
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&hitCalled))
}

func TestExecuteWithOptions_StrategyOverride(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	cache := newMockCache()
	cfg := DefaultIdempotencyConfig()
	cfg.Strategy = StrategyCacheOnly // default
	cfg.EnableDistributedLock = false
	svc := NewUnifiedIdempotencyService(cache, db, nil, cfg, nil, nil)

	// Override to DatabaseOnly
	opts := &ExecutionOptions{
		Key:      "k",
		TTL:      time.Minute,
		Strategy: StrategyDatabaseOnly,
	}
	res, err := svc.ExecuteWithOptions(context.Background(), opts, func() (interface{}, error) {
		return "v", nil
	})
	require.NoError(t, err)
	assert.Equal(t, StrategyDatabaseOnly, res.Strategy)

	// result saved to db, NOT cache (strategy override took effect)
	rec, _ := db.FindByKey(context.Background(), "k")
	require.NotNil(t, rec)
	_, okCache, _ := cache.Get(context.Background(), "k")
	assert.False(t, okCache, "cache should be untouched under database_only override")

	// shared config strategy must be unchanged
	assert.Equal(t, StrategyCacheOnly, svc.GetConfig().Strategy)
}

func TestExecuteWithOptions_OnError(t *testing.T) {
	cache := newMockCache()
	cfg := DefaultIdempotencyConfig()
	cfg.Strategy = StrategyCacheOnly
	cfg.EnableDistributedLock = false
	svc := NewUnifiedIdempotencyService(cache, nil, nil, cfg, nil, nil)

	gotErr := make(chan error, 1)
	opts := &ExecutionOptions{
		Key:     "k",
		TTL:     time.Minute,
		OnError: func(err error) { gotErr <- err },
	}
	_, err := svc.ExecuteWithOptions(context.Background(), opts, func() (interface{}, error) {
		return nil, ErrExecutionFailed
	})
	require.ErrorIs(t, err, ErrExecutionFailed)
	select {
	case e := <-gotErr:
		assert.ErrorIs(t, e, ErrExecutionFailed)
	default:
		t.Fatal("OnError not called")
	}
}

// Bug 2 关键测试：并发传入不同 strategy 不应数据竞争
func TestExecuteWithOptions_ConcurrentDifferentStrategies_NoRace(t *testing.T) {
	cache := newMockCache()
	db := NewInMemoryIdempotencyRepository()
	cfg := DefaultIdempotencyConfig()
	cfg.Strategy = StrategyCacheFirst
	cfg.EnableDistributedLock = false
	svc := NewUnifiedIdempotencyService(cache, db, nil, cfg, nil, nil)

	strategies := []IdempotencyStrategy{
		StrategyCacheOnly, StrategyDatabaseOnly, StrategyCacheFirst, StrategyWriteBoth,
	}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			opts := &ExecutionOptions{
				Key:      fmt.Sprintf("k-%d", i%4),
				TTL:      time.Minute,
				Strategy: strategies[i%4],
			}
			_, _ = svc.ExecuteWithOptions(context.Background(), opts, func() (interface{}, error) {
				return i, nil
			})
		}(i)
	}
	wg.Wait()

	// 共享 config.Strategy 必须未被修改
	assert.Equal(t, StrategyCacheFirst, svc.GetConfig().Strategy)
}

// ===== InMemoryIdempotencyRepository =====

func TestInMemoryRepo_Save_FindByKey(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	ctx := context.Background()
	now := time.Now()
	rec := &IdempotencyRecord{
		Key: "k", Result: []byte("data"), Status: RecordStatusCompleted,
		ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	}
	require.NoError(t, db.Save(ctx, rec))
	assert.NotZero(t, rec.ID)

	got, err := db.FindByKey(ctx, "k")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "k", got.Key)
	assert.Equal(t, []byte("data"), got.Result)

	// missing
	missing, err := db.FindByKey(ctx, "nope")
	require.NoError(t, err)
	assert.Nil(t, missing)
}

func TestInMemoryRepo_Save_KeepsID(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	ctx := context.Background()
	now := time.Now()
	rec := &IdempotencyRecord{Key: "k", ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now}
	require.NoError(t, db.Save(ctx, rec))
	id := rec.ID

	rec.Result = []byte("updated")
	require.NoError(t, db.Save(ctx, rec))
	assert.Equal(t, id, rec.ID, "ID should be preserved on re-save")
}

// Bug 7 关键测试：FindByKey 返回深拷贝，修改返回值不影响内部状态
func TestInMemoryRepo_FindByKey_ReturnsDeepCopy(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	ctx := context.Background()
	now := time.Now()
	require.NoError(t, db.Save(ctx, &IdempotencyRecord{
		Key: "k", Result: []byte("orig"), Status: RecordStatusCompleted,
		ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	}))

	got, err := db.FindByKey(ctx, "k")
	require.NoError(t, err)
	require.NotNil(t, got)

	// mutate the returned copy
	got.Result[0] = 'X'
	got.Status = RecordStatusFailed

	// internal state should be unaffected
	again, _ := db.FindByKey(ctx, "k")
	require.NotNil(t, again)
	assert.Equal(t, []byte("orig"), again.Result)
	assert.Equal(t, RecordStatusCompleted, again.Status)
}

func TestInMemoryRepo_UpdateResult(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	ctx := context.Background()
	now := time.Now()
	require.NoError(t, db.Save(ctx, &IdempotencyRecord{
		Key: "k", ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	}))

	require.NoError(t, db.UpdateResult(ctx, "k", []byte("new"), RecordStatusCompleted))
	got, _ := db.FindByKey(ctx, "k")
	require.NotNil(t, got)
	assert.Equal(t, []byte("new"), got.Result)
	assert.Equal(t, RecordStatusCompleted, got.Status)

	// missing
	require.ErrorIs(t, db.UpdateResult(ctx, "missing", nil, 0), ErrKeyNotFound)
}

func TestInMemoryRepo_Delete(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	ctx := context.Background()
	now := time.Now()
	require.NoError(t, db.Save(ctx, &IdempotencyRecord{Key: "k", ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now}))

	require.NoError(t, db.Delete(ctx, "k"))
	got, _ := db.FindByKey(ctx, "k")
	assert.Nil(t, got)
}

func TestInMemoryRepo_DeleteExpired(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	ctx := context.Background()
	now := time.Now()
	_ = db.Save(ctx, &IdempotencyRecord{Key: "expired", ExpiresAt: now.Add(-time.Minute), CreatedAt: now, UpdatedAt: now})
	_ = db.Save(ctx, &IdempotencyRecord{Key: "alive", ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now})

	require.NoError(t, db.DeleteExpired(ctx))

	// expired should be gone
	got, _ := db.FindByKey(ctx, "expired")
	assert.Nil(t, got)
	// alive should remain
	got2, _ := db.FindByKey(ctx, "alive")
	assert.NotNil(t, got2)
}

func TestInMemoryRepo_AcquireLock_ReleaseLock(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	ctx := context.Background()

	v, acquired, err := db.AcquireLock(ctx, "k", time.Now().Add(time.Minute))
	require.NoError(t, err)
	assert.True(t, acquired)
	assert.NotEmpty(t, v)

	// re-acquire fails while held
	_, acquired2, _ := db.AcquireLock(ctx, "k", time.Now().Add(time.Minute))
	assert.False(t, acquired2)

	// release with wrong owner fails
	require.Error(t, db.ReleaseLock(ctx, "k", "wrong-owner"))

	// release with correct owner succeeds
	require.NoError(t, db.ReleaseLock(ctx, "k", v))

	// can re-acquire after release
	_, acquired3, _ := db.AcquireLock(ctx, "k", time.Now().Add(time.Minute))
	assert.True(t, acquired3)
}

func TestInMemoryRepo_AcquireLock_ExpiredReacquires(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	ctx := context.Background()

	// acquire with already-past expiry
	v, acquired, err := db.AcquireLock(ctx, "k", time.Now().Add(-time.Minute))
	require.NoError(t, err)
	assert.True(t, acquired)
	_ = v

	// since lock record is expired, can reacquire
	_, acquired2, err := db.AcquireLock(ctx, "k", time.Now().Add(time.Minute))
	require.NoError(t, err)
	assert.True(t, acquired2)
}

// ===== Exists / CleanupExpired =====

func TestExists(t *testing.T) {
	cache := newMockCache()
	_ = cache.Set(context.Background(), "k", "v", time.Minute)
	svc := newTestService(cache, nil, nil, StrategyCacheOnly)

	exists, err := svc.Exists(context.Background(), "k")
	require.NoError(t, err)
	assert.True(t, exists)

	exists2, err := svc.Exists(context.Background(), "missing")
	require.NoError(t, err)
	assert.False(t, exists2)
}

func TestCleanupExpired(t *testing.T) {
	db := NewInMemoryIdempotencyRepository()
	now := time.Now()
	_ = db.Save(context.Background(), &IdempotencyRecord{Key: "exp", ExpiresAt: now.Add(-time.Minute), CreatedAt: now, UpdatedAt: now})
	_ = db.Save(context.Background(), &IdempotencyRecord{Key: "alive", ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now})

	svc := newTestService(nil, db, nil, StrategyDatabaseOnly)
	require.NoError(t, svc.CleanupExpired(context.Background()))

	got, _ := db.FindByKey(context.Background(), "exp")
	assert.Nil(t, got)
	got2, _ := db.FindByKey(context.Background(), "alive")
	assert.NotNil(t, got2)
}

func TestCleanupExpired_NilDB(t *testing.T) {
	svc := newTestService(nil, nil, nil, StrategyCacheOnly)
	require.NoError(t, svc.CleanupExpired(context.Background()))
}

// ===== SetStrategy / GetStats =====

func TestSetStrategy(t *testing.T) {
	svc := newTestService(newMockCache(), nil, nil, StrategyCacheOnly)
	svc.SetStrategy(StrategyDatabaseOnly)
	assert.Equal(t, StrategyDatabaseOnly, svc.GetConfig().Strategy)
}

func TestGetStats_Service(t *testing.T) {
	cfg := DefaultIdempotencyConfig()
	svc := NewUnifiedIdempotencyService(nil, nil, nil, cfg, nil, nil)
	stats := svc.GetStats()
	assert.Equal(t, StrategyCacheFirst, stats["strategy"])
	assert.Equal(t, true, stats["enable_distributed_lock"])
}
