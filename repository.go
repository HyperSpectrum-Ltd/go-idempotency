package idempotency

import (
	"context"
	"time"
)

// IdempotencyRepository 幂等性仓储接口（用于数据库持久化）
type IdempotencyRepository interface {
	// Save 保存幂等性记录
	Save(ctx context.Context, record *IdempotencyRecord) error

	// FindByKey 根据键查找记录
	FindByKey(ctx context.Context, key string) (*IdempotencyRecord, error)

	// UpdateResult 更新结果
	UpdateResult(ctx context.Context, key string, result []byte, status int) error

	// Delete 删除记录
	Delete(ctx context.Context, key string) error

	// DeleteExpired 删除过期记录
	DeleteExpired(ctx context.Context) error

	// AcquireLock 获取处理锁
	// 返回值：lockValue 为锁的唯一标识，用于安全释放锁；acquired 表示是否获取成功
	AcquireLock(ctx context.Context, key string, expiresAt time.Time) (string, bool, error)

	// ReleaseLock 释放处理锁
	// lockValue 为获取锁时返回的唯一标识，用于验证锁的持有者身份
	ReleaseLock(ctx context.Context, key string, lockValue string) error
}

// DistributedLockProvider 分布式锁提供者接口
type DistributedLockProvider interface {
	Lock(ctx context.Context, key string, ttl time.Duration) (string, bool, error)
	Unlock(ctx context.Context, key string, lockValue string) error
	WithLock(ctx context.Context, key string, ttl time.Duration, fn func() error) error
}
