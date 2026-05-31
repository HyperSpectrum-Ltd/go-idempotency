# go-idempotency

[![Go Reference](https://pkg.go.dev/badge/github.com/HyperSpectrum-Ltd/go-idempotency.svg)](https://pkg.go.dev/github.com/HyperSpectrum-Ltd/go-idempotency)
[![Go Report Card](https://goreportcard.com/badge/github.com/HyperSpectrum-Ltd/go-idempotency)](https://goreportcard.com/report/github.com/HyperSpectrum-Ltd/go-idempotency)

一个通用的 Go 语言 API 幂等性框架，提供多种策略来确保接口调用的幂等性。

## 特性

- 🚀 **多种幂等性策略**：缓存优先、仅缓存、仅数据库、双写模式
- 🔒 **分布式锁支持**：内置分布式锁接口，防止并发重复执行
- 📊 **指标收集**：可插拔的指标收集器，支持命中/未命中/错误统计
- 🔍 **OpenTelemetry 集成**：内置分布式追踪支持
- 🛠️ **灵活的键构建器**：流式 API 构建幂等性键
- ⚡ **高性能**：本地缓存 + 并发控制
- 🧪 **易于测试**：提供内存实现用于单元测试

## 安装

```bash
go get github.com/HyperSpectrum-Ltd/go-idempotency
```

## 快速开始

### 基本使用

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    idempotency "github.com/HyperSpectrum-Ltd/go-idempotency"
    "go.uber.org/zap"
)

// 实现 CacheProvider 接口（以 Redis 为例）
type RedisCacheProvider struct {
    // Redis 客户端
}

func (r *RedisCacheProvider) Get(ctx context.Context, key string) (interface{}, bool, error) {
    // 从 Redis 获取
    // ...
}

func (r *RedisCacheProvider) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
    // 设置到 Redis
    // ...
}

func (r *RedisCacheProvider) Delete(ctx context.Context, key string) error {
    // 从 Redis 删除
    // ...
}

func (r *RedisCacheProvider) Exists(ctx context.Context, key string) (bool, error) {
    // 检查 Redis 中是否存在
    // ...
}

func main() {
    // 创建缓存提供者
    cache := &RedisCacheProvider{}

    // 创建幂等性服务
    service := idempotency.NewUnifiedIdempotencyService(
        cache,                                    // 缓存提供者
        nil,                                      // 数据库仓储（可选）
        nil,                                      // 分布式锁（可选）
        idempotency.DefaultIdempotencyConfig(),   // 配置
        zap.NewNop(),                             // 日志器
        nil,                                      // 指标收集器（可选）
    )

    // 使用幂等性执行
    ctx := context.Background()
    result, err := service.ExecuteWithIdempotency(
        ctx,
        "order:create:12345",                    // 幂等性键
        24*time.Hour,                            // TTL
        func() (interface{}, error) {
            // 业务逻辑
            return createOrder("12345")
        },
    )
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Result:", result)
}
```

### 使用键构建器

```go
import idempotency "github.com/HyperSpectrum-Ltd/go-idempotency"

// 使用流式 API 构建键
key := idempotency.NewUnifiedKeyBuilder().
    Domain("billing").
    Resource("order").
    Action("create").
    UserID(12345).
    UniqueID("order-uuid").
    Build()
// 结果: billing:order:create:user:12345:order-uuid

// 使用选项模式构建键
key = idempotency.BuildKeyWithOptions(
    idempotency.WithDomain("billing"),
    idempotency.WithResource("order"),
    idempotency.WithAction("create"),
    idempotency.WithUserID(12345),
)

// 使用注册表
registry := idempotency.NewKeyBuilderRegistry()
registry.Register("order-create", idempotency.NewUnifiedKeyBuilder().
    Domain("billing").
    Resource("order").
    Action("create"),
)
key = registry.Build("order-create")
```

### 使用执行器

```go
// 创建执行器
executor := idempotency.NewExecutorBuilder().
    WithService(service).
    WithConfig(idempotency.DefaultExecutorConfig()).
    WithLogger(logger).
    Build()

// 使用幂等性执行
result, err := executor.ExecuteWithIdempotency(
    ctx,
    "payment:process:67890",
    24*time.Hour,
    func(ctx context.Context) (interface{}, error) {
        return processPayment("67890")
    },
)

// 使用分布式锁执行
result, err := executor.ExecuteWithLock(
    ctx,
    "lock:payment:67890",
    30*time.Second,
    func(ctx context.Context) (interface{}, error) {
        return processPayment("67890")
    },
)

// 批量执行
results := executor.ExecuteBatch(ctx, []idempotency.BatchItem{
    {Key: "order:1", TTL: time.Hour, Fn: func(ctx context.Context) (interface{}, error) { return processOrder("1") }},
    {Key: "order:2", TTL: time.Hour, Fn: func(ctx context.Context) (interface{}, error) { return processOrder("2") }},
})
```

## 幂等性策略

### StrategyCacheFirst（推荐）

缓存优先策略，兼顾性能和持久化：

1. 先查缓存，命中则直接返回
2. 缓存未命中，查数据库
3. 数据库命中，回填缓存并返回
4. 都未命中，执行业务逻辑并保存

```go
config := idempotency.NewIdempotencyConfigBuilder().
    WithStrategy(idempotency.StrategyCacheFirst).
    Build()
```

### StrategyCacheOnly

仅使用缓存，性能最高，适合短期幂等：

```go
config := idempotency.NewIdempotencyConfigBuilder().
    WithStrategy(idempotency.StrategyCacheOnly).
    Build()
```

### StrategyDatabaseOnly

仅使用数据库，持久化存储，适合长期幂等：

```go
config := idempotency.NewIdempotencyConfigBuilder().
    WithStrategy(idempotency.StrategyDatabaseOnly).
    Build()
```

### StrategyWriteBoth

双写模式，同时写入缓存和数据库，最高可靠性：

```go
config := idempotency.NewIdempotencyConfigBuilder().
    WithStrategy(idempotency.StrategyWriteBoth).
    Build()
```

## 接口实现

### CacheProvider

```go
type CacheProvider interface {
    Get(ctx context.Context, key string) (interface{}, bool, error)
    Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error
    Delete(ctx context.Context, key string) error
    Exists(ctx context.Context, key string) (bool, error)
}
```

### IdempotencyRepository

```go
type IdempotencyRepository interface {
    Save(ctx context.Context, record *IdempotencyRecord) error
    FindByKey(ctx context.Context, key string) (*IdempotencyRecord, error)
    UpdateResult(ctx context.Context, key string, result []byte, status int) error
    Delete(ctx context.Context, key string) error
    DeleteExpired(ctx context.Context) error
    AcquireLock(ctx context.Context, key string, expiresAt time.Time) (string, bool, error)
    ReleaseLock(ctx context.Context, key string, lockValue string) error
}
```

### DistributedLockProvider

```go
type DistributedLockProvider interface {
    Lock(ctx context.Context, key string, ttl time.Duration) (string, bool, error)
    Unlock(ctx context.Context, key string, lockValue string) error
    WithLock(ctx context.Context, key string, ttl time.Duration, fn func() error) error
}
```

### MetricsCollector

```go
type MetricsCollector interface {
    RecordHit(strategy IdempotencyStrategy, source string)
    RecordMiss(strategy IdempotencyStrategy)
    RecordError(strategy IdempotencyStrategy, err error)
    RecordLatency(strategy IdempotencyStrategy, duration time.Duration)
}
```

## 配置选项

```go
config := idempotency.NewIdempotencyConfigBuilder().
    WithStrategy(idempotency.StrategyCacheFirst).     // 策略
    WithDefaultTTL(24*time.Hour).                      // 默认 TTL
    WithCacheTTL(1*time.Hour).                         // 缓存 TTL
    WithKeyPrefix("idempotency").                      // 键前缀
    WithMetrics(true).                                 // 启用指标
    WithDistributedLock(true).                         // 启用分布式锁
    WithLockTTL(30*time.Second).                       // 锁 TTL
    WithRetry(3, 100*time.Millisecond).                // 重试参数
    Build()
```

## 测试

框架提供了内存实现用于单元测试：

```go
import (
    idempotency "github.com/HyperSpectrum-Ltd/go-idempotency"
)

func TestIdempotency(t *testing.T) {
    // 使用内存仓储
    repo := idempotency.NewInMemoryIdempotencyRepository()
    
    // 使用内存缓存（需要自己实现或使用 mock）
    cache := &mockCacheProvider{}
    
    service := idempotency.NewUnifiedIdempotencyService(
        cache,
        repo,
        nil,
        idempotency.DefaultIdempotencyConfig(),
        nil,
        nil,
    )
    
    ctx := context.Background()
    key := "test:order:create:12345"
    
    // 第一次执行
    result1, err := service.ExecuteWithIdempotency(ctx, key, time.Hour, func() (interface{}, error) {
        return "order-created", nil
    })
    assert.NoError(t, err)
    assert.Equal(t, "order-created", result1)
    
    // 第二次执行（应该返回缓存结果）
    result2, err := service.ExecuteWithIdempotency(ctx, key, time.Hour, func() (interface{}, error) {
        return "order-created-again", nil
    })
    assert.NoError(t, err)
    assert.Equal(t, "order-created", result2) // 返回第一次的结果
}
```

## 架构

```
┌─────────────────────────────────────────────────────────┐
│                    调用方代码                            │
├─────────────────────────────────────────────────────────┤
│  UnifiedExecutor（执行器层）                             │
│  • 并发控制（信号量）                                    │
│  • 超时控制                                              │
│  • 重试机制                                              │
│  • 分布式锁                                              │
├─────────────────────────────────────────────────────────┤
│  UnifiedIdempotencyService（服务层）                     │
│  • 策略路由                                              │
│  • 缓存/数据库协调                                       │
│  • 指标收集                                              │
│  • 追踪集成                                              │
├─────────────────────────────────────────────────────────┤
│  接口层（可插拔）                                        │
│  • CacheProvider（缓存）                                 │
│  • IdempotencyRepository（数据库）                       │
│  • DistributedLockProvider（分布式锁）                   │
│  • MetricsCollector（指标）                              │
│  • Serializer（序列化）                                  │
└─────────────────────────────────────────────────────────┘
```

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！
