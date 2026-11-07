package idempotency

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// UnifiedIdempotencyService 统一幂等性服务
type UnifiedIdempotencyService struct {
	cache      CacheProvider
	db         IdempotencyRepository
	lock       DistributedLockProvider
	config     *IdempotencyConfig
	logger     *zap.Logger
	metrics    MetricsCollector
	tracer     trace.Tracer
	serializer Serializer

	// 本地缓存（用于减少远程调用）
	localCache sync.Map

	// keyLocks 是进程内按幂等键的互斥锁（GAP-068）。
	// 无分布式锁时，两个并发请求会同时通过 check 并各自执行 fn，造成重复扣款/重复发信；
	// 多副本场景仍由分布式锁或数据库唯一约束兜底，这里只保证单副本内串行。
	keyLocksMu sync.Mutex
	keyLocks   map[string]*keyLockEntry
}

// keyLockEntry 是单个幂等键的进程内锁与引用计数。
type keyLockEntry struct {
	mu   sync.Mutex
	refs int
}

// lockKey 获取指定幂等键的进程内互斥锁，返回解锁函数。
// 引用计数归零时移除条目，避免键空间无界增长。
func (s *UnifiedIdempotencyService) lockKey(key string) func() {
	s.keyLocksMu.Lock()
	if s.keyLocks == nil {
		s.keyLocks = make(map[string]*keyLockEntry)
	}
	entry, ok := s.keyLocks[key]
	if !ok {
		entry = &keyLockEntry{}
		s.keyLocks[key] = entry
	}
	entry.refs++
	s.keyLocksMu.Unlock()

	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		s.keyLocksMu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(s.keyLocks, key)
		}
		s.keyLocksMu.Unlock()
	}
}

// NewUnifiedIdempotencyService 创建统一幂等性服务
func NewUnifiedIdempotencyService(
	cache CacheProvider,
	db IdempotencyRepository,
	lock DistributedLockProvider,
	config *IdempotencyConfig,
	logger *zap.Logger,
	metrics MetricsCollector,
) *UnifiedIdempotencyService {
	if config == nil {
		config = DefaultIdempotencyConfig()
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	return &UnifiedIdempotencyService{
		cache:      cache,
		db:         db,
		lock:       lock,
		config:     config,
		logger:     logger,
		metrics:    metrics,
		tracer:     otel.Tracer("idempotency-service"),
		serializer: NewJSONSerializer(),
		keyLocks:   make(map[string]*keyLockEntry),
	}
}

// Check 检查幂等性键
func (s *UnifiedIdempotencyService) Check(ctx context.Context, key string) (bool, interface{}, error) {
	return s.checkWithStrategy(ctx, key, s.config.Strategy)
}

// checkWithStrategy 按指定策略检查幂等性键（请求级策略，不修改共享 config）
func (s *UnifiedIdempotencyService) checkWithStrategy(ctx context.Context, key string, strategy IdempotencyStrategy) (bool, interface{}, error) {
	ctx, span := s.tracer.Start(ctx, "UnifiedIdempotencyService.Check",
		trace.WithAttributes(attribute.String("key", key)))
	defer span.End()

	switch strategy {
	case StrategyCacheOnly:
		return s.checkFromCache(ctx, key)
	case StrategyDatabaseOnly:
		return s.checkFromDatabase(ctx, key)
	case StrategyCacheFirst:
		return s.checkCacheFirst(ctx, key)
	case StrategyWriteBoth:
		return s.checkFromBoth(ctx, key)
	default:
		return false, nil, ErrInvalidStrategy
	}
}

// checkFromCache 从缓存检查
func (s *UnifiedIdempotencyService) checkFromCache(ctx context.Context, key string) (bool, interface{}, error) {
	if s.cache == nil {
		s.logger.Warn("cache provider is nil, cannot check from cache", zap.String("key", key))
		return false, nil, fmt.Errorf("cache provider is nil")
	}

	result, exists, err := s.cache.Get(ctx, key)
	if err != nil {
		s.logger.Error("failed to check cache", zap.String("key", key), zap.Error(err))
		return false, nil, err
	}

	if exists {
		s.recordHit(StrategyCacheOnly, "cache")
		return true, result, nil
	}

	s.recordMiss(StrategyCacheOnly)
	return false, nil, nil
}

// checkFromDatabase 从数据库检查
func (s *UnifiedIdempotencyService) checkFromDatabase(ctx context.Context, key string) (bool, interface{}, error) {
	if s.db == nil {
		s.logger.Warn("database repository is nil, cannot check from database", zap.String("key", key))
		return false, nil, fmt.Errorf("database repository is nil")
	}

	record, err := s.db.FindByKey(ctx, key)
	if err != nil {
		s.logger.Error("failed to check database", zap.String("key", key), zap.Error(err))
		return false, nil, err
	}

	if record == nil {
		s.recordMiss(StrategyDatabaseOnly)
		return false, nil, nil
	}

	// 检查是否过期
	if time.Now().After(record.ExpiresAt) {
		_ = s.db.Delete(ctx, key)
		s.recordMiss(StrategyDatabaseOnly)
		return false, nil, ErrKeyExpired
	}

	// 检查是否正在处理
	if record.Status == RecordStatusProcessing {
		return true, nil, ErrProcessing
	}

	// 检查是否失败
	if record.Status == RecordStatusFailed {
		return true, nil, ErrExecutionFailed
	}

	// 反序列化结果
	var result interface{}
	if len(record.Result) > 0 {
		if err := s.serializer.Unmarshal(record.Result, &result); err != nil {
			s.logger.Error("failed to unmarshal result", zap.String("key", key), zap.Error(err))
			return false, nil, err
		}
	}

	s.recordHit(StrategyDatabaseOnly, "database")
	return true, result, nil
}

// checkCacheFirst 缓存优先检查
func (s *UnifiedIdempotencyService) checkCacheFirst(ctx context.Context, key string) (bool, interface{}, error) {
	// 先查缓存
	if s.cache != nil {
		result, exists, err := s.cache.Get(ctx, key)
		if err != nil {
			s.logger.Warn("cache check failed, falling back to database", zap.String("key", key), zap.Error(err))
		} else if exists {
			s.recordHit(StrategyCacheFirst, "cache")
			return true, result, nil
		}
	}

	// 缓存未命中，查数据库
	if s.db == nil {
		s.logger.Warn("database repository is nil, cannot fall back to database", zap.String("key", key))
		s.recordMiss(StrategyCacheFirst)
		return false, nil, nil
	}

	record, err := s.db.FindByKey(ctx, key)
	if err != nil {
		s.logger.Error("database check failed", zap.String("key", key), zap.Error(err))
		return false, nil, err
	}

	if record == nil {
		s.recordMiss(StrategyCacheFirst)
		return false, nil, nil
	}

	// 检查是否过期
	if time.Now().After(record.ExpiresAt) {
		_ = s.db.Delete(ctx, key)
		s.recordMiss(StrategyCacheFirst)
		return false, nil, ErrKeyExpired
	}

	// 检查是否正在处理
	if record.Status == RecordStatusProcessing {
		return true, nil, ErrProcessing
	}

	// 反序列化结果
	var dbResult interface{}
	if len(record.Result) > 0 {
		if err := s.serializer.Unmarshal(record.Result, &dbResult); err != nil {
			s.logger.Error("failed to unmarshal result", zap.String("key", key), zap.Error(err))
			return false, nil, err
		}
	}

	// 回填缓存
	ttl := time.Until(record.ExpiresAt)
	if ttl > 0 && s.cache != nil {
		if err := s.cache.Set(ctx, key, dbResult, ttl); err != nil {
			s.logger.Warn("failed to backfill cache", zap.String("key", key), zap.Error(err))
		}
	}

	s.recordHit(StrategyCacheFirst, "database")
	return true, dbResult, nil
}

// checkFromBoth 从缓存和数据库检查
func (s *UnifiedIdempotencyService) checkFromBoth(ctx context.Context, key string) (bool, interface{}, error) {
	if s.cache == nil && s.db == nil {
		s.logger.Warn("both cache and database are nil, cannot check", zap.String("key", key))
		return false, nil, fmt.Errorf("cache and database are both nil")
	}

	// 并行检查缓存和数据库
	type checkResult struct {
		source string
		exists bool
		result interface{}
		err    error
	}

	resultChan := make(chan checkResult, 2)

	// 检查缓存
	if s.cache != nil {
		go func() {
			result, exists, err := s.cache.Get(ctx, key)
			if err != nil {
				resultChan <- checkResult{source: "cache", err: err}
				return
			}
			resultChan <- checkResult{source: "cache", exists: exists, result: result}
		}()
	} else {
		resultChan <- checkResult{source: "cache", exists: false}
	}

	// 检查数据库
	if s.db != nil {
		go func() {
			record, err := s.db.FindByKey(ctx, key)
			if err != nil {
				resultChan <- checkResult{source: "database", err: err}
				return
			}
			if record == nil {
				resultChan <- checkResult{source: "database", exists: false}
				return
			}

			var dbResult interface{}
			if len(record.Result) > 0 {
				_ = s.serializer.Unmarshal(record.Result, &dbResult)
			}
			resultChan <- checkResult{source: "database", exists: true, result: dbResult}
		}()
	} else {
		resultChan <- checkResult{source: "database", exists: false}
	}

	// 收集结果
	var cacheResult, dbResult checkResult
	for i := 0; i < 2; i++ {
		r := <-resultChan
		if r.source == "cache" {
			cacheResult = r
		} else {
			dbResult = r
		}
	}

	// 优先返回缓存结果
	if cacheResult.exists && cacheResult.err == nil {
		s.recordHit(StrategyWriteBoth, "cache")
		return true, cacheResult.result, nil
	}

	if dbResult.exists && dbResult.err == nil {
		s.recordHit(StrategyWriteBoth, "database")
		return true, dbResult.result, nil
	}

	s.recordMiss(StrategyWriteBoth)
	return false, nil, nil
}

// Save 保存幂等性结果
func (s *UnifiedIdempotencyService) Save(ctx context.Context, key string, result interface{}, ttl time.Duration) error {
	return s.saveWithStrategy(ctx, key, result, ttl, s.config.Strategy)
}

// saveWithStrategy 按指定策略保存幂等性结果（请求级策略，不修改共享 config）
func (s *UnifiedIdempotencyService) saveWithStrategy(ctx context.Context, key string, result interface{}, ttl time.Duration, strategy IdempotencyStrategy) error {
	ctx, span := s.tracer.Start(ctx, "UnifiedIdempotencyService.Save",
		trace.WithAttributes(
			attribute.String("key", key),
			attribute.String("strategy", string(strategy))))
	defer span.End()

	if ttl == 0 {
		ttl = s.config.DefaultTTL
	}

	startTime := time.Now()
	defer func() {
		s.recordLatency(strategy, time.Since(startTime))
	}()

	switch strategy {
	case StrategyCacheOnly:
		return s.saveToCache(ctx, key, result, ttl)
	case StrategyDatabaseOnly:
		return s.saveToDatabase(ctx, key, result, ttl)
	case StrategyCacheFirst, StrategyWriteBoth:
		return s.saveToBoth(ctx, key, result, ttl)
	default:
		return ErrInvalidStrategy
	}
}

// saveToCache 保存到缓存
func (s *UnifiedIdempotencyService) saveToCache(ctx context.Context, key string, result interface{}, ttl time.Duration) error {
	if s.cache == nil {
		s.logger.Warn("cache provider is nil, cannot save to cache", zap.String("key", key))
		return fmt.Errorf("cache provider is nil")
	}
	return s.cache.Set(ctx, key, result, ttl)
}

// saveToDatabase 保存到数据库
func (s *UnifiedIdempotencyService) saveToDatabase(ctx context.Context, key string, result interface{}, ttl time.Duration) error {
	if s.db == nil {
		s.logger.Warn("database repository is nil, cannot save to database", zap.String("key", key))
		return fmt.Errorf("database repository is nil")
	}

	data, err := s.serializer.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal result: %w", err)
	}

	now := time.Now()
	record := &IdempotencyRecord{
		Key:       key,
		Result:    data,
		Status:    RecordStatusCompleted,
		CreatedAt: now,
		UpdatedAt: now,
		ExpiresAt: now.Add(ttl),
	}
	completedAt := now
	record.CompletedAt = &completedAt

	return s.db.Save(ctx, record)
}

// saveToBoth 保存到缓存和数据库
func (s *UnifiedIdempotencyService) saveToBoth(ctx context.Context, key string, result interface{}, ttl time.Duration) error {
	// 先保存到缓存（快速返回）
	if s.cache != nil {
		if cacheErr := s.cache.Set(ctx, key, result, ttl); cacheErr != nil {
			s.logger.Warn("failed to save to cache", zap.String("key", key), zap.Error(cacheErr))
		}
	}

	// 再保存到数据库（持久化）
	if s.db == nil {
		s.logger.Warn("database repository is nil, cannot persist result", zap.String("key", key))
		return fmt.Errorf("database repository is nil")
	}

	if dbErr := s.saveToDatabase(ctx, key, result, ttl); dbErr != nil {
		s.logger.Error("failed to save to database", zap.String("key", key), zap.Error(dbErr))
		return dbErr
	}

	return nil
}

// Delete 删除幂等性键
func (s *UnifiedIdempotencyService) Delete(ctx context.Context, key string) error {
	var errs []error

	// 从缓存删除
	if s.cache != nil {
		if err := s.cache.Delete(ctx, key); err != nil {
			errs = append(errs, fmt.Errorf("cache delete: %w", err))
		}
	}

	// 从数据库删除
	if s.db != nil {
		if err := s.db.Delete(ctx, key); err != nil {
			errs = append(errs, fmt.Errorf("database delete: %w", err))
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

// Exists 检查键是否存在
func (s *UnifiedIdempotencyService) Exists(ctx context.Context, key string) (bool, error) {
	exists, _, err := s.Check(ctx, key)
	return exists, err
}

// TryAcquire 尝试获取处理锁
func (s *UnifiedIdempotencyService) TryAcquire(ctx context.Context, key string, ttl time.Duration) (string, bool, error) {
	if s.lock != nil && s.config.EnableDistributedLock {
		lockKey := s.config.KeyPrefix + ":lock:" + key
		return s.lock.Lock(ctx, lockKey, ttl)
	}

	// 使用数据库锁
	if s.db != nil {
		lockValue, acquired, err := s.db.AcquireLock(ctx, key, time.Now().Add(ttl))
		return lockValue, acquired, err
	}

	// 使用缓存锁
	if s.cache != nil {
		lockKey := s.config.KeyPrefix + ":lock:" + key
		exists, err := s.cache.Exists(ctx, lockKey)
		if err != nil {
			return "", false, err
		}
		// Bug 4: 锁已存在表示已被持有，获取失败（语义修复）
		if exists {
			return "", false, nil
		}
		// 尽力获取锁（CacheProvider 无 SetNX 语义，存在 TOCTOU 竞态）
		lockValue := uuid.New().String()
		if err := s.cache.Set(ctx, lockKey, lockValue, ttl); err != nil {
			return "", false, err
		}
		return lockValue, true, nil
	}

	// Bug 4: 无任何锁提供者时返回错误，而非假成功
	return "", false, fmt.Errorf("no lock provider available")
}

// Release 释放处理锁
func (s *UnifiedIdempotencyService) Release(ctx context.Context, key string, lockValue string) error {
	if s.lock != nil && s.config.EnableDistributedLock {
		lockKey := s.config.KeyPrefix + ":lock:" + key
		return s.lock.Unlock(ctx, lockKey, lockValue)
	}

	if s.db != nil {
		return s.db.ReleaseLock(ctx, key, lockValue)
	}

	if s.cache != nil {
		lockKey := s.config.KeyPrefix + ":lock:" + key
		return s.cache.Delete(ctx, lockKey)
	}

	return nil
}

// ExecuteWithIdempotency 使用幂等性执行函数。
//
// Deprecated: 使用 ExecuteWithOptions（&ExecutionOptions{Key: key, TTL: ttl}），
// 以便携带策略覆盖与回调。
func (s *UnifiedIdempotencyService) ExecuteWithIdempotency(
	ctx context.Context,
	key string,
	ttl time.Duration,
	fn func() (interface{}, error),
) (interface{}, error) {
	result, err := s.ExecuteWithIdempotencyAndResult(ctx, key, ttl, fn)
	if err != nil {
		return nil, err
	}
	return result.Result, nil
}

// ExecuteWithIdempotencyAndResult 使用幂等性执行函数并返回详细结果。
//
// Deprecated: 使用 ExecuteWithOptions（&ExecutionOptions{Key: key, TTL: ttl}）。
func (s *UnifiedIdempotencyService) ExecuteWithIdempotencyAndResult(
	ctx context.Context,
	key string,
	ttl time.Duration,
	fn func() (interface{}, error),
) (*ExecutionResult, error) {
	return s.executeWithIdempotencyAndResult(ctx, key, ttl, fn, s.config.Strategy)
}

// executeWithIdempotencyAndResult 内部实现，接受请求级策略参数（不修改共享 config）
func (s *UnifiedIdempotencyService) executeWithIdempotencyAndResult(
	ctx context.Context,
	key string,
	ttl time.Duration,
	fn func() (interface{}, error),
	strategy IdempotencyStrategy,
) (*ExecutionResult, error) {
	ctx, span := s.tracer.Start(ctx, "UnifiedIdempotencyService.ExecuteWithIdempotency",
		trace.WithAttributes(attribute.String("key", key)))
	defer span.End()

	startTime := time.Now()

	if ttl == 0 {
		ttl = s.config.DefaultTTL
	}

	// Bug 1: 当启用分布式锁且存在锁提供者时，主路径使用锁 + 双重检查。
	// 该路径保持 fail-fast：拿不到锁的并发请求返回 ErrDistributedLock。
	if s.config.EnableDistributedLock && s.lock != nil {
		return s.executeWithDistributedLock(ctx, key, ttl, fn, strategy, startTime)
	}

	// GAP-068：无分布式锁（或未配置锁提供者）时，单副本内按 key 串行，
	// 避免并发请求同时通过 check 后各自执行 fn（重复扣款/重复发信）。
	unlock := s.lockKey(key)
	defer unlock()

	return s.executeWithoutLock(ctx, key, ttl, fn, strategy, startTime)
}

// executeWithDistributedLock 使用分布式锁执行（双重检查保证幂等性）
func (s *UnifiedIdempotencyService) executeWithDistributedLock(
	ctx context.Context,
	key string,
	ttl time.Duration,
	fn func() (interface{}, error),
	strategy IdempotencyStrategy,
	startTime time.Time,
) (*ExecutionResult, error) {
	lockKey := s.config.KeyPrefix + ":lock:" + key
	lockTTL := s.config.LockTTL
	if lockTTL == 0 {
		lockTTL = 30 * time.Second
	}

	lockValue, acquired, err := s.lock.Lock(ctx, lockKey, lockTTL)
	if err != nil {
		s.recordError(strategy, err)
		return nil, err
	}
	if !acquired {
		return nil, ErrDistributedLock
	}
	defer s.lock.Unlock(ctx, lockKey, lockValue)

	// 双重检查：获取锁后再次检查幂等性
	exists, cachedResult, checkErr := s.checkWithStrategy(ctx, key, strategy)
	if checkErr != nil && !errors.Is(checkErr, ErrKeyExpired) &&
		!errors.Is(checkErr, ErrProcessing) && !errors.Is(checkErr, ErrExecutionFailed) {
		s.recordError(strategy, checkErr)
		return nil, checkErr
	}
	// Bug 5: 正在处理中时不应重复执行
	if errors.Is(checkErr, ErrProcessing) {
		return nil, ErrProcessing
	}
	// GAP-068：失败记录允许重试，清理后按未命中继续执行。
	if errors.Is(checkErr, ErrExecutionFailed) {
		if s.db != nil {
			if delErr := s.db.Delete(ctx, key); delErr != nil {
				s.logger.Warn("failed to clear failed idempotency record",
					zap.String("key", key), zap.Error(delErr))
			}
		}
		exists, cachedResult, checkErr = false, nil, nil
	}
	if exists && !errors.Is(checkErr, ErrKeyExpired) {
		s.logger.Info("idempotency key hit", zap.String("key", key))
		return &ExecutionResult{
			Key:           key,
			Result:        cachedResult,
			IsHit:         true,
			IsNew:         false,
			Strategy:      strategy,
			ExpiresAt:     time.Now().Add(ttl),
			ExecutionTime: time.Since(startTime),
			Source:        s.detectSourceForStrategy(strategy),
		}, nil
	}

	// 执行业务逻辑
	newResult, execErr := fn()

	executionTime := time.Since(startTime)

	if execErr != nil {
		s.recordError(strategy, execErr)
		s.logger.Error("execution failed",
			zap.String("key", key),
			zap.Error(execErr),
			zap.Duration("execution_time", executionTime))

		// 保存失败状态
		if s.db != nil && strategy != StrategyCacheOnly {
			s.saveErrorToDatabase(ctx, key, execErr, ttl)
		}

		return nil, execErr
	}

	// 保存结果
	if saveErr := s.saveWithStrategy(ctx, key, newResult, ttl, strategy); saveErr != nil {
		s.logger.Warn("failed to save idempotency result",
			zap.String("key", key),
			zap.Error(saveErr))
	}

	s.logger.Info("idempotency key saved",
		zap.String("key", key),
		zap.Duration("execution_time", executionTime))

	return &ExecutionResult{
		Key:           key,
		Result:        newResult,
		IsHit:         false,
		IsNew:         true,
		Strategy:      strategy,
		ExpiresAt:     time.Now().Add(ttl),
		ExecutionTime: executionTime,
		Source:        "execution",
	}, nil
}

// executeWithoutLock 不持锁执行（启用分布式锁但无 lock provider 时回退路径）
func (s *UnifiedIdempotencyService) executeWithoutLock(
	ctx context.Context,
	key string,
	ttl time.Duration,
	fn func() (interface{}, error),
	strategy IdempotencyStrategy,
	startTime time.Time,
) (*ExecutionResult, error) {
	// 检查幂等性
	exists, cachedResult, err := s.checkWithStrategy(ctx, key, strategy)
	if err != nil && !errors.Is(err, ErrKeyExpired) &&
		!errors.Is(err, ErrProcessing) && !errors.Is(err, ErrExecutionFailed) {
		s.recordError(strategy, err)
		return nil, err
	}

	// Bug 5: 正在处理中时不应重复执行
	if errors.Is(err, ErrProcessing) {
		return nil, ErrProcessing
	}

	// GAP-068：失败记录允许重试，清理后按未命中继续执行。
	if errors.Is(err, ErrExecutionFailed) {
		if s.db != nil {
			if delErr := s.db.Delete(ctx, key); delErr != nil {
				s.logger.Warn("failed to clear failed idempotency record",
					zap.String("key", key), zap.Error(delErr))
			}
		}
		exists, cachedResult, err = false, nil, nil
	}

	if exists && !errors.Is(err, ErrKeyExpired) {
		s.logger.Info("idempotency key hit", zap.String("key", key))
		return &ExecutionResult{
			Key:           key,
			Result:        cachedResult,
			IsHit:         true,
			IsNew:         false,
			Strategy:      strategy,
			ExpiresAt:     time.Now().Add(ttl),
			ExecutionTime: time.Since(startTime),
			Source:        s.detectSourceForStrategy(strategy),
		}, nil
	}

	// 执行业务逻辑
	newResult, execErr := fn()

	executionTime := time.Since(startTime)

	if execErr != nil {
		s.recordError(strategy, execErr)
		s.logger.Error("execution failed",
			zap.String("key", key),
			zap.Error(execErr),
			zap.Duration("execution_time", executionTime))

		// 保存失败状态
		if s.db != nil && strategy != StrategyCacheOnly {
			s.saveErrorToDatabase(ctx, key, execErr, ttl)
		}

		return nil, execErr
	}

	// 保存结果
	if saveErr := s.saveWithStrategy(ctx, key, newResult, ttl, strategy); saveErr != nil {
		s.logger.Warn("failed to save idempotency result",
			zap.String("key", key),
			zap.Error(saveErr))
	}

	s.logger.Info("idempotency key saved",
		zap.String("key", key),
		zap.Duration("execution_time", executionTime))

	return &ExecutionResult{
		Key:           key,
		Result:        newResult,
		IsHit:         false,
		IsNew:         true,
		Strategy:      strategy,
		ExpiresAt:     time.Now().Add(ttl),
		ExecutionTime: executionTime,
		Source:        "execution",
	}, nil
}

// ExecuteWithOptions 使用选项执行
func (s *UnifiedIdempotencyService) ExecuteWithOptions(
	ctx context.Context,
	opts *ExecutionOptions,
	fn func() (interface{}, error),
) (*ExecutionResult, error) {
	// Bug 2: 不修改共享 config，使用请求级策略局部变量
	strategy := s.config.Strategy
	if opts.Strategy != "" {
		strategy = opts.Strategy
	}

	// 强制执行
	if opts.ForceExecute {
		result, err := fn()
		if err != nil {
			if opts.OnError != nil {
				opts.OnError(err)
			}
			return nil, err
		}
		return &ExecutionResult{
			Key:      opts.Key,
			Result:   result,
			IsNew:    true,
			Strategy: strategy,
		}, nil
	}

	// 执行（传递请求级策略）
	execResult, err := s.executeWithIdempotencyAndResult(ctx, opts.Key, opts.TTL, fn, strategy)
	if err != nil {
		if opts.OnError != nil {
			opts.OnError(err)
		}
		return nil, err
	}

	// 回调
	if execResult.IsHit && opts.OnHit != nil {
		opts.OnHit(execResult)
	} else if !execResult.IsHit && opts.OnMiss != nil {
		opts.OnMiss(execResult)
	}

	return execResult, nil
}

// saveErrorToDatabase 保存错误到数据库
func (s *UnifiedIdempotencyService) saveErrorToDatabase(ctx context.Context, key string, execErr error, ttl time.Duration) {
	data, _ := s.serializer.Marshal(map[string]string{"error": execErr.Error()})

	now := time.Now()
	record := &IdempotencyRecord{
		Key:       key,
		Result:    data,
		Status:    RecordStatusFailed,
		CreatedAt: now,
		UpdatedAt: now,
		ExpiresAt: now.Add(ttl),
		ErrorMsg:  execErr.Error(),
	}

	if err := s.db.Save(ctx, record); err != nil {
		s.logger.Error("failed to save error to database", zap.String("key", key), zap.Error(err))
	}
}

// detectSource 检测数据来源
func (s *UnifiedIdempotencyService) detectSource() string {
	return s.detectSourceForStrategy(s.config.Strategy)
}

// detectSourceForStrategy 按策略检测数据来源
func (s *UnifiedIdempotencyService) detectSourceForStrategy(strategy IdempotencyStrategy) string {
	switch strategy {
	case StrategyCacheOnly:
		return "cache"
	case StrategyDatabaseOnly:
		return "database"
	case StrategyCacheFirst, StrategyWriteBoth:
		return "cache_or_database"
	default:
		return "unknown"
	}
}

// recordHit 记录命中
func (s *UnifiedIdempotencyService) recordHit(strategy IdempotencyStrategy, source string) {
	if s.metrics != nil {
		s.metrics.RecordHit(strategy, source)
	}
}

// recordMiss 记录未命中
func (s *UnifiedIdempotencyService) recordMiss(strategy IdempotencyStrategy) {
	if s.metrics != nil {
		s.metrics.RecordMiss(strategy)
	}
}

// recordError 记录错误
func (s *UnifiedIdempotencyService) recordError(strategy IdempotencyStrategy, err error) {
	if s.metrics != nil {
		s.metrics.RecordError(strategy, err)
	}
}

// recordLatency 记录延迟
func (s *UnifiedIdempotencyService) recordLatency(strategy IdempotencyStrategy, duration time.Duration) {
	if s.metrics != nil {
		s.metrics.RecordLatency(strategy, duration)
	}
}

// GetConfig 获取配置
func (s *UnifiedIdempotencyService) GetConfig() *IdempotencyConfig {
	return s.config
}

// SetStrategy 设置策略
func (s *UnifiedIdempotencyService) SetStrategy(strategy IdempotencyStrategy) {
	s.config.Strategy = strategy
}

// CleanupExpired 清理过期记录
func (s *UnifiedIdempotencyService) CleanupExpired(ctx context.Context) error {
	if s.db != nil {
		return s.db.DeleteExpired(ctx)
	}
	return nil
}

// GetStats 获取统计信息
func (s *UnifiedIdempotencyService) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"strategy":                s.config.Strategy,
		"default_ttl":             s.config.DefaultTTL.String(),
		"enable_metrics":          s.config.EnableMetrics,
		"enable_distributed_lock": s.config.EnableDistributedLock,
	}
}

// InMemoryIdempotencyRepository 内存幂等性仓储（用于测试）
type InMemoryIdempotencyRepository struct {
	records map[string]*IdempotencyRecord
	mu      sync.RWMutex
}

// NewInMemoryIdempotencyRepository 创建内存幂等性仓储
func NewInMemoryIdempotencyRepository() *InMemoryIdempotencyRepository {
	return &InMemoryIdempotencyRepository{
		records: make(map[string]*IdempotencyRecord),
	}
}

// Save 保存记录
func (r *InMemoryIdempotencyRepository) Save(ctx context.Context, record *IdempotencyRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	existing, exists := r.records[record.Key]
	if exists {
		record.ID = existing.ID
	} else {
		record.ID = int64(len(r.records) + 1)
	}

	// 存储深拷贝，避免外部修改影响内部状态
	r.records[record.Key] = cloneRecord(record)
	return nil
}

// FindByKey 根据键查找
func (r *InMemoryIdempotencyRepository) FindByKey(ctx context.Context, key string) (*IdempotencyRecord, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	record, ok := r.records[key]
	if !ok {
		return nil, nil
	}
	// Bug 7: 返回深拷贝，避免读者拿到活指针后与写者竞争
	return cloneRecord(record), nil
}

// UpdateResult 更新结果
func (r *InMemoryIdempotencyRepository) UpdateResult(ctx context.Context, key string, result []byte, status int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	record, ok := r.records[key]
	if !ok {
		return ErrKeyNotFound
	}

	record.Result = append([]byte(nil), result...)
	record.Status = status
	record.UpdatedAt = time.Now()
	return nil
}

// Delete 删除记录
func (r *InMemoryIdempotencyRepository) Delete(ctx context.Context, key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.records, key)
	return nil
}

// DeleteExpired 删除过期记录
func (r *InMemoryIdempotencyRepository) DeleteExpired(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	for key, record := range r.records {
		if now.After(record.ExpiresAt) {
			delete(r.records, key)
		}
	}
	return nil
}

// AcquireLock 获取锁
func (r *InMemoryIdempotencyRepository) AcquireLock(ctx context.Context, key string, expiresAt time.Time) (string, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	lockKey := "lock:" + key
	if record, exists := r.records[lockKey]; exists {
		if time.Now().Before(record.ExpiresAt) {
			return "", false, nil
		}
	}

	// 生成锁值用于安全释放
	lockValue := uuid.New().String()
	r.records[lockKey] = &IdempotencyRecord{
		Key:       lockKey,
		Status:    RecordStatusProcessing,
		Result:    []byte(lockValue), // 存储锁值用于验证
		ExpiresAt: expiresAt,
		CreatedAt: time.Now(),
	}
	return lockValue, true, nil
}

// ReleaseLock 释放锁
func (r *InMemoryIdempotencyRepository) ReleaseLock(ctx context.Context, key string, lockValue string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	lockKey := "lock:" + key
	record, exists := r.records[lockKey]
	if !exists {
		return nil
	}

	// 验证锁值
	if record.Result != nil && string(record.Result) != lockValue {
		return fmt.Errorf("lock not held by this owner: %s", key)
	}

	delete(r.records, lockKey)
	return nil
}

// cloneRecord 深拷贝 IdempotencyRecord
func cloneRecord(src *IdempotencyRecord) *IdempotencyRecord {
	if src == nil {
		return nil
	}
	cp := *src
	if src.Result != nil {
		cp.Result = append([]byte(nil), src.Result...)
	}
	if src.CompletedAt != nil {
		t := *src.CompletedAt
		cp.CompletedAt = &t
	}
	return &cp
}
