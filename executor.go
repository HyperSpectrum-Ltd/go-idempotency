package idempotency

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// ExecutorConfig 执行器配置
type ExecutorConfig struct {
	// DefaultTTL 默认过期时间
	DefaultTTL time.Duration

	// MaxConcurrent 最大并发数
	MaxConcurrent int

	// Timeout 执行超时
	Timeout time.Duration

	// EnableTracing 是否启用追踪
	EnableTracing bool

	// EnableMetrics 是否启用指标
	EnableMetrics bool

	// RetryCount 重试次数
	RetryCount int

	// RetryDelay 重试延迟
	RetryDelay time.Duration
}

// DefaultExecutorConfig 默认执行器配置
func DefaultExecutorConfig() *ExecutorConfig {
	return &ExecutorConfig{
		DefaultTTL:    24 * time.Hour,
		MaxConcurrent: 100,
		Timeout:       30 * time.Second,
		EnableTracing: true,
		EnableMetrics: true,
		RetryCount:    0,
		RetryDelay:    100 * time.Millisecond,
	}
}

// ExecutorConfigBuilder 执行器配置构建器
type ExecutorConfigBuilder struct {
	config *ExecutorConfig
}

// NewExecutorConfigBuilder 创建执行器配置构建器
func NewExecutorConfigBuilder() *ExecutorConfigBuilder {
	return &ExecutorConfigBuilder{
		config: DefaultExecutorConfig(),
	}
}

// WithDefaultTTL 设置默认 TTL
func (b *ExecutorConfigBuilder) WithDefaultTTL(ttl time.Duration) *ExecutorConfigBuilder {
	b.config.DefaultTTL = ttl
	return b
}

// WithMaxConcurrent 设置最大并发数
func (b *ExecutorConfigBuilder) WithMaxConcurrent(max int) *ExecutorConfigBuilder {
	b.config.MaxConcurrent = max
	return b
}

// WithTimeout 设置超时
func (b *ExecutorConfigBuilder) WithTimeout(timeout time.Duration) *ExecutorConfigBuilder {
	b.config.Timeout = timeout
	return b
}

// WithTracing 启用追踪
func (b *ExecutorConfigBuilder) WithTracing(enable bool) *ExecutorConfigBuilder {
	b.config.EnableTracing = enable
	return b
}

// WithMetrics 启用指标
func (b *ExecutorConfigBuilder) WithMetrics(enable bool) *ExecutorConfigBuilder {
	b.config.EnableMetrics = enable
	return b
}

// WithRetry 设置重试
func (b *ExecutorConfigBuilder) WithRetry(count int, delay time.Duration) *ExecutorConfigBuilder {
	b.config.RetryCount = count
	b.config.RetryDelay = delay
	return b
}

// Build 构建
func (b *ExecutorConfigBuilder) Build() *ExecutorConfig {
	return b.config
}

// ExecutionFunc 执行函数类型
type ExecutionFunc func(ctx context.Context) (interface{}, error)

// ExecutionContext 执行上下文
type ExecutionContext struct {
	Key         string
	Operation   string
	UserID      int64
	TenantID    int64
	RequestID   string
	Metadata    map[string]interface{}
	StartTime   time.Time
	Attempt     int
	MaxAttempts int
}

// NewExecutionContext 创建执行上下文
func NewExecutionContext(key, operation string) *ExecutionContext {
	return &ExecutionContext{
		Key:         key,
		Operation:   operation,
		Metadata:    make(map[string]interface{}),
		StartTime:   time.Now(),
		Attempt:     1,
		MaxAttempts: 1,
	}
}

// WithUserID 设置用户 ID
func (c *ExecutionContext) WithUserID(userID int64) *ExecutionContext {
	c.UserID = userID
	return c
}

// WithTenantID 设置租户 ID
func (c *ExecutionContext) WithTenantID(tenantID int64) *ExecutionContext {
	c.TenantID = tenantID
	return c
}

// WithRequestID 设置请求 ID
func (c *ExecutionContext) WithRequestID(requestID string) *ExecutionContext {
	c.RequestID = requestID
	return c
}

// WithMetadata 设置元数据
func (c *ExecutionContext) WithMetadata(key string, value interface{}) *ExecutionContext {
	c.Metadata[key] = value
	return c
}

// WithAttempt 设置尝试次数
func (c *ExecutionContext) WithAttempt(attempt, maxAttempts int) *ExecutionContext {
	c.Attempt = attempt
	c.MaxAttempts = maxAttempts
	return c
}

// Duration 获取执行时长
func (c *ExecutionContext) Duration() time.Duration {
	return time.Since(c.StartTime)
}

// UnifiedExecutor 统一执行器
type UnifiedExecutor struct {
	service *UnifiedIdempotencyService
	config  *ExecutorConfig
	logger  *zap.Logger
	tracer  trace.Tracer
	metrics MetricsCollector

	// 并发控制
	semaphore chan struct{}
	wg        sync.WaitGroup
}

// NewUnifiedExecutor 创建统一执行器
func NewUnifiedExecutor(
	service *UnifiedIdempotencyService,
	config *ExecutorConfig,
	logger *zap.Logger,
	metrics MetricsCollector,
) *UnifiedExecutor {
	if config == nil {
		config = DefaultExecutorConfig()
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	executor := &UnifiedExecutor{
		service:   service,
		config:    config,
		logger:    logger,
		tracer:    otel.Tracer("unified-executor"),
		metrics:   metrics,
		semaphore: make(chan struct{}, config.MaxConcurrent),
	}

	return executor
}

// ExecuteWithIdempotency 使用幂等性执行
func (e *UnifiedExecutor) ExecuteWithIdempotency(
	ctx context.Context,
	key string,
	ttl time.Duration,
	fn ExecutionFunc,
) (interface{}, error) {
	result, err := e.ExecuteWithIdempotencyAndContext(ctx, key, ttl, fn, nil)
	if err != nil {
		return nil, err
	}
	return result.Result, nil
}

// ExecuteWithIdempotencyAndContext 使用幂等性和上下文执行
func (e *UnifiedExecutor) ExecuteWithIdempotencyAndContext(
	ctx context.Context,
	key string,
	ttl time.Duration,
	fn ExecutionFunc,
	execCtx *ExecutionContext,
) (*ExecutionResult, error) {
	// 创建追踪 span
	ctx, span := e.tracer.Start(ctx, "UnifiedExecutor.Execute",
		trace.WithAttributes(
			attribute.String("key", key),
			attribute.String("operation", "execute_with_idempotency")))
	defer span.End()

	// 设置超时
	if e.config.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.config.Timeout)
		defer cancel()
	}

	// 获取并发槽
	select {
	case e.semaphore <- struct{}{}:
		defer func() { <-e.semaphore }()
	case <-ctx.Done():
		span.SetStatus(codes.Error, "context cancelled")
		return nil, ctx.Err()
	}

	// 设置默认 TTL
	if ttl == 0 {
		ttl = e.config.DefaultTTL
	}

	// 执行
	startTime := time.Now()
	result, err := e.service.ExecuteWithIdempotencyAndResult(ctx, key, ttl, func() (interface{}, error) {
		return e.executeWithRetry(ctx, fn, execCtx)
	})

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return nil, err
	}

	// 记录指标
	if e.metrics != nil && e.config.EnableMetrics {
		e.metrics.RecordLatency(e.service.config.Strategy, time.Since(startTime))
	}

	return result, nil
}

// executeWithRetry 带重试执行
func (e *UnifiedExecutor) executeWithRetry(ctx context.Context, fn ExecutionFunc, execCtx *ExecutionContext) (interface{}, error) {
	var lastErr error
	maxAttempts := e.config.RetryCount + 1

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			select {
			case <-time.After(e.config.RetryDelay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}

			e.logger.Info("retrying execution",
				zap.Int("attempt", attempt),
				zap.Int("max_attempts", maxAttempts))
		}

		result, err := fn(ctx)
		if err == nil {
			return result, nil
		}

		lastErr = err

		// 检查是否可重试
		if !e.isRetryableError(err) {
			return nil, err
		}
	}

	return nil, fmt.Errorf("max retry attempts exceeded: %w", lastErr)
}

// isRetryableError 检查是否可重试错误
func (e *UnifiedExecutor) isRetryableError(err error) bool {
	// 网络错误、超时错误等可重试
	var netErr interface{ Timeout() bool }
	if errors.As(err, &netErr) {
		return true
	}

	// 上下文取消不可重试
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	return false
}

// ExecuteWithOptions 使用选项执行
func (e *UnifiedExecutor) ExecuteWithOptions(
	ctx context.Context,
	opts *ExecutionOptions,
	fn ExecutionFunc,
) (*ExecutionResult, error) {
	return e.service.ExecuteWithOptions(ctx, opts, func() (interface{}, error) {
		return fn(ctx)
	})
}

// Execute 执行（不使用幂等性）
func (e *UnifiedExecutor) Execute(
	ctx context.Context,
	fn ExecutionFunc,
) (interface{}, error) {
	ctx, span := e.tracer.Start(ctx, "UnifiedExecutor.Execute",
		trace.WithAttributes(attribute.String("operation", "execute")))
	defer span.End()

	// 设置超时
	if e.config.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.config.Timeout)
		defer cancel()
	}

	// 获取并发槽
	select {
	case e.semaphore <- struct{}{}:
		defer func() { <-e.semaphore }()
	case <-ctx.Done():
		span.SetStatus(codes.Error, "context cancelled")
		return nil, ctx.Err()
	}

	return e.executeWithRetry(ctx, fn, nil)
}

// ExecuteBatch 批量执行
func (e *UnifiedExecutor) ExecuteBatch(
	ctx context.Context,
	items []BatchItem,
) []BatchResult {
	results := make([]BatchResult, len(items))

	var wg sync.WaitGroup
	for i, item := range items {
		wg.Add(1)
		go func(idx int, item BatchItem) {
			defer wg.Done()

			result, err := e.ExecuteWithIdempotency(ctx, item.Key, item.TTL, item.Fn)
			results[idx] = BatchResult{
				Key:   item.Key,
				Value: result,
				Error: err,
			}
		}(i, item)
	}
	wg.Wait()

	return results
}

// BatchItem 批量执行项
type BatchItem struct {
	Key string
	TTL time.Duration
	Fn  ExecutionFunc
}

// BatchResult 批量执行结果
type BatchResult struct {
	Key   string
	Value interface{}
	Error error
}

// ExecuteParallel 并行执行
func (e *UnifiedExecutor) ExecuteParallel(
	ctx context.Context,
	fns map[string]ExecutionFunc,
) map[string]interface{} {
	results := make(map[string]interface{})
	var mu sync.Mutex

	var wg sync.WaitGroup
	for key, fn := range fns {
		wg.Add(1)
		go func(k string, f ExecutionFunc) {
			defer wg.Done()

			result, err := e.Execute(ctx, f)
			mu.Lock()
			if err != nil {
				results[k] = err
			} else {
				results[k] = result
			}
			mu.Unlock()
		}(key, fn)
	}
	wg.Wait()

	return results
}

// ExecuteWithLock 使用分布式锁执行
func (e *UnifiedExecutor) ExecuteWithLock(
	ctx context.Context,
	lockKey string,
	lockTTL time.Duration,
	fn ExecutionFunc,
) (interface{}, error) {
	ctx, span := e.tracer.Start(ctx, "UnifiedExecutor.ExecuteWithLock",
		trace.WithAttributes(attribute.String("lock_key", lockKey)))
	defer span.End()

	if e.service.lock == nil {
		e.logger.Warn("distributed lock not available, executing without lock")
		return e.Execute(ctx, fn)
	}

	var result interface{}
	err := e.service.lock.WithLock(ctx, lockKey, lockTTL, func() error {
		var execErr error
		result, execErr = e.Execute(ctx, fn)
		return execErr
	})

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	return result, nil
}

// ExecuteWithLockAndIdempotency 使用分布式锁和幂等性执行
func (e *UnifiedExecutor) ExecuteWithLockAndIdempotency(
	ctx context.Context,
	key string,
	lockKey string,
	ttl time.Duration,
	lockTTL time.Duration,
	fn ExecutionFunc,
) (interface{}, error) {
	ctx, span := e.tracer.Start(ctx, "UnifiedExecutor.ExecuteWithLockAndIdempotency",
		trace.WithAttributes(
			attribute.String("key", key),
			attribute.String("lock_key", lockKey)))
	defer span.End()

	// 先检查幂等性
	exists, result, err := e.service.Check(ctx, key)
	if err != nil {
		return nil, err
	}
	if exists {
		e.logger.Info("idempotency key hit, returning cached result",
			zap.String("key", key))
		return result, nil
	}

	// 使用分布式锁执行
	if e.service.lock != nil {
		err = e.service.lock.WithLock(ctx, lockKey, lockTTL, func() error {
			// 双重检查
			exists, result, err = e.service.Check(ctx, key)
			if err != nil {
				return err
			}
			if exists {
				return nil
			}

			// 执行业务逻辑
			result, err = e.Execute(ctx, fn)
			if err != nil {
				return err
			}

			// 保存结果
			return e.service.Save(ctx, key, result, ttl)
		})

		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}

		return result, nil
	}

	// 没有分布式锁，直接执行
	result, err = e.Execute(ctx, fn)
	if err != nil {
		return nil, err
	}

	// 保存结果
	if err := e.service.Save(ctx, key, result, ttl); err != nil {
		e.logger.Warn("failed to save idempotency result", zap.Error(err))
	}

	return result, nil
}

// ExecuteWithTimeout 使用超时执行
func (e *UnifiedExecutor) ExecuteWithTimeout(
	ctx context.Context,
	timeout time.Duration,
	fn ExecutionFunc,
) (interface{}, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return e.Execute(ctx, fn)
}

// ExecuteAsync 异步执行
func (e *UnifiedExecutor) ExecuteAsync(
	ctx context.Context,
	key string,
	ttl time.Duration,
	fn ExecutionFunc,
) <-chan AsyncResult {
	resultChan := make(chan AsyncResult, 1)

	go func() {
		defer close(resultChan)

		result, err := e.ExecuteWithIdempotency(ctx, key, ttl, fn)
		resultChan <- AsyncResult{
			Key:   key,
			Value: result,
			Error: err,
		}
	}()

	return resultChan
}

// AsyncResult 异步执行结果
type AsyncResult struct {
	Key   string
	Value interface{}
	Error error
}

// ExecuteWithCallback 带回调执行
func (e *UnifiedExecutor) ExecuteWithCallback(
	ctx context.Context,
	key string,
	ttl time.Duration,
	fn ExecutionFunc,
	onSuccess func(result interface{}),
	onError func(err error),
) {
	go func() {
		result, err := e.ExecuteWithIdempotency(ctx, key, ttl, fn)
		if err != nil {
			if onError != nil {
				onError(err)
			}
			return
		}

		if onSuccess != nil {
			onSuccess(result)
		}
	}()
}

// GetStats 获取统计信息
func (e *UnifiedExecutor) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"max_concurrent": e.config.MaxConcurrent,
		"timeout":        e.config.Timeout.String(),
		"retry_count":    e.config.RetryCount,
		"enable_tracing": e.config.EnableTracing,
		"enable_metrics": e.config.EnableMetrics,
	}
}

// Shutdown 关闭执行器
func (e *UnifiedExecutor) Shutdown(ctx context.Context) error {
	e.logger.Info("shutting down executor")

	// 等待所有任务完成
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ExecutorBuilder 执行器构建器
type ExecutorBuilder struct {
	service *UnifiedIdempotencyService
	config  *ExecutorConfig
	logger  *zap.Logger
	metrics MetricsCollector
}

// NewExecutorBuilder 创建执行器构建器
func NewExecutorBuilder() *ExecutorBuilder {
	return &ExecutorBuilder{
		config: DefaultExecutorConfig(),
	}
}

// WithService 设置服务
func (b *ExecutorBuilder) WithService(service *UnifiedIdempotencyService) *ExecutorBuilder {
	b.service = service
	return b
}

// WithConfig 设置配置
func (b *ExecutorBuilder) WithConfig(config *ExecutorConfig) *ExecutorBuilder {
	b.config = config
	return b
}

// WithLogger 设置日志
func (b *ExecutorBuilder) WithLogger(logger *zap.Logger) *ExecutorBuilder {
	b.logger = logger
	return b
}

// WithMetrics 设置指标
func (b *ExecutorBuilder) WithMetrics(metrics MetricsCollector) *ExecutorBuilder {
	b.metrics = metrics
	return b
}

// Build 构建
func (b *ExecutorBuilder) Build() *UnifiedExecutor {
	return NewUnifiedExecutor(b.service, b.config, b.logger, b.metrics)
}

// ExecutionChain 执行链
type ExecutionChain struct {
	executor *UnifiedExecutor
	steps    []ExecutionStep
}

// ExecutionStep 执行步骤
type ExecutionStep struct {
	Name string
	Fn   ExecutionFunc
}

// NewExecutionChain 创建执行链
func NewExecutionChain(executor *UnifiedExecutor) *ExecutionChain {
	return &ExecutionChain{
		executor: executor,
		steps:    make([]ExecutionStep, 0),
	}
}

// AddStep 添加步骤
func (c *ExecutionChain) AddStep(name string, fn ExecutionFunc) *ExecutionChain {
	c.steps = append(c.steps, ExecutionStep{Name: name, Fn: fn})
	return c
}

// Execute 执行链
func (c *ExecutionChain) Execute(ctx context.Context) (map[string]interface{}, error) {
	results := make(map[string]interface{})

	for _, step := range c.steps {
		result, err := c.executor.Execute(ctx, step.Fn)
		if err != nil {
			return results, fmt.Errorf("step %s failed: %w", step.Name, err)
		}
		results[step.Name] = result
	}

	return results, nil
}
