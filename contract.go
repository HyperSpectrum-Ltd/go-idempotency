package idempotency

import (
	dkport "github.com/HyperSpectrum-Ltd/go-domainkit/port"
)

// DistributedLockProvider 的 Lock/Unlock 方法集已满足 domainkit 的基础锁契约；
// WithLock 是本模块的便捷方法，不参与契约判定。
var _ dkport.DistributedLock = (DistributedLockProvider)(nil)
