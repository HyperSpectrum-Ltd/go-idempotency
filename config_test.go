package idempotency

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultIdempotencyConfig(t *testing.T) {
	c := DefaultIdempotencyConfig()
	require.NotNil(t, c)

	assert.Equal(t, StrategyCacheFirst, c.Strategy)
	assert.Equal(t, 24*time.Hour, c.DefaultTTL)
	assert.Equal(t, 24*time.Hour, c.CacheTTL)
	assert.True(t, c.EnableMetrics)
	assert.Equal(t, "idempotency", c.KeyPrefix)
	assert.Equal(t, 3, c.MaxRetryCount)
	assert.Equal(t, 100*time.Millisecond, c.RetryDelay)
	assert.Equal(t, 30*time.Second, c.LockTTL)
	assert.True(t, c.EnableDistributedLock)
}

func TestIdempotencyConfigBuilder_Chain(t *testing.T) {
	c := NewIdempotencyConfigBuilder().
		WithStrategy(StrategyDatabaseOnly).
		WithDefaultTTL(time.Hour).
		WithCacheTTL(30*time.Minute).
		WithKeyPrefix("custom").
		WithMetrics(false).
		WithDistributedLock(false).
		WithLockTTL(10*time.Second).
		WithRetry(5, 200*time.Millisecond).
		Build()

	require.NotNil(t, c)
	assert.Equal(t, StrategyDatabaseOnly, c.Strategy)
	assert.Equal(t, time.Hour, c.DefaultTTL)
	assert.Equal(t, 30*time.Minute, c.CacheTTL)
	assert.Equal(t, "custom", c.KeyPrefix)
	assert.False(t, c.EnableMetrics)
	assert.False(t, c.EnableDistributedLock)
	assert.Equal(t, 10*time.Second, c.LockTTL)
	assert.Equal(t, 5, c.MaxRetryCount)
	assert.Equal(t, 200*time.Millisecond, c.RetryDelay)
}

func TestExecutionOptionsBuilder_Fields(t *testing.T) {
	opts := NewExecutionOptionsBuilder().
		WithKey("my-key").
		WithTTL(time.Minute).
		WithStrategy(StrategyWriteBoth).
		SkipCache().
		SkipDatabase().
		ForceExecute().
		OnHit(func(r *ExecutionResult) {}).
		OnMiss(func(r *ExecutionResult) {}).
		OnError(func(err error) {}).
		Build()

	require.NotNil(t, opts)
	assert.Equal(t, "my-key", opts.Key)
	assert.Equal(t, time.Minute, opts.TTL)
	assert.Equal(t, StrategyWriteBoth, opts.Strategy)
	assert.True(t, opts.SkipCache)
	assert.True(t, opts.SkipDatabase)
	assert.True(t, opts.ForceExecute)
	assert.NotNil(t, opts.OnHit)
	assert.NotNil(t, opts.OnMiss)
	assert.NotNil(t, opts.OnError)
}

func TestExecutionOptionsBuilder_DefaultTTL(t *testing.T) {
	opts := NewExecutionOptionsBuilder().Build()
	assert.Equal(t, 24*time.Hour, opts.TTL)
	assert.Empty(t, opts.Key)
	assert.Empty(t, opts.Strategy)
	assert.False(t, opts.ForceExecute)
}
