package idempotency

import "time"

// RuntimeConfig 是服务与执行器共享的运行参数，避免两份配置重复定义
// DefaultTTL/EnableMetrics/RetryDelay。
type RuntimeConfig struct {
	// DefaultTTL 默认过期时间
	DefaultTTL time.Duration `json:"default_ttl"`

	// EnableMetrics 是否启用指标收集
	EnableMetrics bool `json:"enable_metrics"`

	// RetryDelay 重试延迟
	RetryDelay time.Duration `json:"retry_delay"`
}

// IdempotencyConfig 幂等性配置
type IdempotencyConfig struct {
	RuntimeConfig

	// Strategy 幂等性策略
	Strategy IdempotencyStrategy `json:"strategy"`

	// CacheTTL 缓存过期时间（用于双写模式）
	CacheTTL time.Duration `json:"cache_ttl"`

	// KeyPrefix 键前缀
	KeyPrefix string `json:"key_prefix"`

	// MaxRetryCount 最大重试次数
	MaxRetryCount int `json:"max_retry_count"`

	// LockTTL 分布式锁 TTL
	LockTTL time.Duration `json:"lock_ttl"`

	// EnableDistributedLock 是否启用分布式锁
	EnableDistributedLock bool `json:"enable_distributed_lock"`
}

// DefaultIdempotencyConfig 默认配置
func DefaultIdempotencyConfig() *IdempotencyConfig {
	return &IdempotencyConfig{
		RuntimeConfig: RuntimeConfig{
			DefaultTTL:    24 * time.Hour,
			EnableMetrics: true,
			RetryDelay:    100 * time.Millisecond,
		},
		Strategy:              StrategyCacheFirst,
		CacheTTL:              24 * time.Hour,
		KeyPrefix:             "idempotency",
		MaxRetryCount:         3,
		LockTTL:               30 * time.Second,
		EnableDistributedLock: true,
	}
}

// IdempotencyConfigBuilder 配置构建器
type IdempotencyConfigBuilder struct {
	config *IdempotencyConfig
}

// NewIdempotencyConfigBuilder 创建配置构建器
func NewIdempotencyConfigBuilder() *IdempotencyConfigBuilder {
	return &IdempotencyConfigBuilder{
		config: DefaultIdempotencyConfig(),
	}
}

// WithStrategy 设置策略
func (b *IdempotencyConfigBuilder) WithStrategy(strategy IdempotencyStrategy) *IdempotencyConfigBuilder {
	b.config.Strategy = strategy
	return b
}

// WithDefaultTTL 设置默认 TTL
func (b *IdempotencyConfigBuilder) WithDefaultTTL(ttl time.Duration) *IdempotencyConfigBuilder {
	b.config.DefaultTTL = ttl
	return b
}

// WithCacheTTL 设置缓存 TTL
func (b *IdempotencyConfigBuilder) WithCacheTTL(ttl time.Duration) *IdempotencyConfigBuilder {
	b.config.CacheTTL = ttl
	return b
}

// WithKeyPrefix 设置键前缀
func (b *IdempotencyConfigBuilder) WithKeyPrefix(prefix string) *IdempotencyConfigBuilder {
	b.config.KeyPrefix = prefix
	return b
}

// WithMetrics 启用指标
func (b *IdempotencyConfigBuilder) WithMetrics(enable bool) *IdempotencyConfigBuilder {
	b.config.EnableMetrics = enable
	return b
}

// WithDistributedLock 启用分布式锁
func (b *IdempotencyConfigBuilder) WithDistributedLock(enable bool) *IdempotencyConfigBuilder {
	b.config.EnableDistributedLock = enable
	return b
}

// WithLockTTL 设置锁 TTL
func (b *IdempotencyConfigBuilder) WithLockTTL(ttl time.Duration) *IdempotencyConfigBuilder {
	b.config.LockTTL = ttl
	return b
}

// WithRetry 设置重试参数
func (b *IdempotencyConfigBuilder) WithRetry(maxCount int, delay time.Duration) *IdempotencyConfigBuilder {
	b.config.MaxRetryCount = maxCount
	b.config.RetryDelay = delay
	return b
}

// Build 构建配置
func (b *IdempotencyConfigBuilder) Build() *IdempotencyConfig {
	return b.config
}

// ExecutionOptionsBuilder 执行选项构建器
type ExecutionOptionsBuilder struct {
	opts *ExecutionOptions
}

// NewExecutionOptionsBuilder 创建执行选项构建器
func NewExecutionOptionsBuilder() *ExecutionOptionsBuilder {
	return &ExecutionOptionsBuilder{
		opts: &ExecutionOptions{
			TTL: 24 * time.Hour,
		},
	}
}

// WithKey 设置键
func (b *ExecutionOptionsBuilder) WithKey(key string) *ExecutionOptionsBuilder {
	b.opts.Key = key
	return b
}

// WithTTL 设置 TTL
func (b *ExecutionOptionsBuilder) WithTTL(ttl time.Duration) *ExecutionOptionsBuilder {
	b.opts.TTL = ttl
	return b
}

// WithStrategy 设置策略
func (b *ExecutionOptionsBuilder) WithStrategy(strategy IdempotencyStrategy) *ExecutionOptionsBuilder {
	b.opts.Strategy = strategy
	return b
}

// SkipCache 跳过缓存
func (b *ExecutionOptionsBuilder) SkipCache() *ExecutionOptionsBuilder {
	b.opts.SkipCache = true
	return b
}

// SkipDatabase 跳过数据库
func (b *ExecutionOptionsBuilder) SkipDatabase() *ExecutionOptionsBuilder {
	b.opts.SkipDatabase = true
	return b
}

// ForceExecute 强制执行
func (b *ExecutionOptionsBuilder) ForceExecute() *ExecutionOptionsBuilder {
	b.opts.ForceExecute = true
	return b
}

// OnHit 设置命中回调
func (b *ExecutionOptionsBuilder) OnHit(fn func(result *ExecutionResult)) *ExecutionOptionsBuilder {
	b.opts.OnHit = fn
	return b
}

// OnMiss 设置未命中回调
func (b *ExecutionOptionsBuilder) OnMiss(fn func(result *ExecutionResult)) *ExecutionOptionsBuilder {
	b.opts.OnMiss = fn
	return b
}

// OnError 设置错误回调
func (b *ExecutionOptionsBuilder) OnError(fn func(err error)) *ExecutionOptionsBuilder {
	b.opts.OnError = fn
	return b
}

// Build 构建选项
func (b *ExecutionOptionsBuilder) Build() *ExecutionOptions {
	return b.opts
}
