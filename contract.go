package idempotency

import (
	dkport "github.com/HyperSpectrum-Ltd/go-domainkit/port"
)

// DistributedLockProvider 的 Lock/Unlock 方法集已满足 domainkit 的基础锁契约；
// WithLock 是本模块的便捷方法，不参与契约判定。
var _ dkport.DistributedLock = (DistributedLockProvider)(nil)

// BackoffPolicy 把幂等配置的固定重试延迟映射为 domainkit 统一退避策略。
//
// 本模块只有 MaxRetryCount + RetryDelay（固定间隔），因此 MaxDelay=RetryDelay、
// Multiplier=1；需要指数退避的调用方应显式构造 BackoffPolicy。
func (c *IdempotencyConfig) BackoffPolicy() dkport.BackoffPolicy {
	return dkport.BackoffPolicy{
		MaxAttempts:  c.MaxRetryCount + 1,
		InitialDelay: c.RetryDelay,
		MaxDelay:     c.RetryDelay,
		Multiplier:   1,
	}
}
