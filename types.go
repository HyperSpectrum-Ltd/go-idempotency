package idempotency

import "time"

// IdempotencyStrategy 幂等性策略
type IdempotencyStrategy string

const (
	// StrategyCacheOnly 仅使用缓存（默认，高性能，适合短期幂等）
	StrategyCacheOnly IdempotencyStrategy = "cache_only"

	// StrategyDatabaseOnly 仅使用数据库（持久化，适合长期幂等）
	StrategyDatabaseOnly IdempotencyStrategy = "database_only"

	// StrategyCacheFirst 缓存优先，缓存未命中时查数据库（推荐，兼顾性能和持久化）
	StrategyCacheFirst IdempotencyStrategy = "cache_first"

	// StrategyWriteBoth 双写模式，同时写入缓存和数据库（最高可靠性）
	StrategyWriteBoth IdempotencyStrategy = "write_both"
)

// ExecutionResult 执行结果
type ExecutionResult struct {
	// Key 幂等性键
	Key string `json:"key"`

	// Result 业务结果
	Result interface{} `json:"result,omitempty"`

	// IsHit 是否命中缓存/数据库
	IsHit bool `json:"is_hit"`

	// IsNew 是否是新执行
	IsNew bool `json:"is_new"`

	// Strategy 使用的策略
	Strategy IdempotencyStrategy `json:"strategy"`

	// ExpiresAt 过期时间
	ExpiresAt time.Time `json:"expires_at"`

	// ExecutionTime 执行耗时
	ExecutionTime time.Duration `json:"execution_time"`

	// Source 数据来源（cache/database）
	Source string `json:"source"`
}

// ExecutionOptions 执行选项
type ExecutionOptions struct {
	// Key 幂等性键
	Key string

	// TTL 过期时间
	TTL time.Duration

	// Strategy 覆盖策略
	Strategy IdempotencyStrategy

	// SkipCache 跳过缓存
	SkipCache bool

	// SkipDatabase 跳过数据库
	SkipDatabase bool

	// ForceExecute 强制执行（忽略幂等性检查）
	ForceExecute bool

	// OnHit 命中回调
	OnHit func(result *ExecutionResult)

	// OnMiss 未命中回调
	OnMiss func(result *ExecutionResult)

	// OnError 错误回调
	OnError func(err error)
}

// IdempotencyRecord 幂等性记录（用于数据库存储）
type IdempotencyRecord struct {
	ID          int64      `json:"id"`
	Key         string     `json:"key"`
	Result      []byte     `json:"result"`
	Status      int        `json:"status"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	ExpiresAt   time.Time  `json:"expires_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	ErrorMsg    string     `json:"error_msg,omitempty"`
}

const (
	RecordStatusProcessing = 0
	RecordStatusCompleted  = 1
	RecordStatusFailed     = 2
)
