package idempotency

import "errors"

var (
	// ErrKeyNotFound 幂等性键未找到
	ErrKeyNotFound = errors.New("idempotency key not found")

	// ErrKeyExpired 幂等性键已过期
	ErrKeyExpired = errors.New("idempotency key expired")

	// ErrProcessing 请求正在处理中
	ErrProcessing = errors.New("request is still processing")

	// ErrExecutionFailed 执行失败
	ErrExecutionFailed = errors.New("execution failed")

	// ErrInvalidStrategy 无效的幂等性策略
	ErrInvalidStrategy = errors.New("invalid idempotency strategy")

	// ErrDistributedLock 获取分布式锁失败
	ErrDistributedLock = errors.New("failed to acquire distributed lock")
)
