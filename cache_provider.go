package idempotency

import (
	"context"
	"time"
)

// CacheProvider 缓存提供者接口
// 定义幂等性框架所需的缓存操作，解耦对具体缓存实现的依赖。
//
// 架构说明：
//   - 此接口由 go-idempotency 框架定义，使用者需提供实现
//   - 适用于 Redis、内存缓存、分布式缓存等多种后端
//   - 方法签名与常见缓存库兼容，便于适配
type CacheProvider interface {
	// Get 获取缓存值
	// 返回值：
	//   - value: 缓存的值
	//   - found: 是否找到（false 表示键不存在）
	//   - err: 错误信息
	Get(ctx context.Context, key string) (interface{}, bool, error)

	// Set 设置缓存值，带过期时间
	Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error

	// Delete 删除缓存键
	Delete(ctx context.Context, key string) error

	// Exists 检查键是否存在
	Exists(ctx context.Context, key string) (bool, error)
}
