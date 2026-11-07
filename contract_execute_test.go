package idempotency

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExecuteWithOptions_IsCanonical ExecuteWithIdempotencyAndContext 必须与
// canonical 的 ExecuteWithOptions 行为一致（包括缓存命中）。
func TestExecuteWithOptions_IsCanonical(t *testing.T) {
	e := newTestExecutor(StrategyCacheOnly)
	ctx := context.Background()

	first, err := e.ExecuteWithOptions(ctx, &ExecutionOptions{Key: "canonical", TTL: time.Minute}, func(context.Context) (interface{}, error) {
		return "value", nil
	})
	require.NoError(t, err)
	assert.False(t, first.IsHit)

	// 兼容入口必须读到同一份幂等结果。
	second, err := e.ExecuteWithIdempotencyAndContext(ctx, "canonical", time.Minute, func(context.Context) (interface{}, error) {
		return "other", nil
	}, nil)
	require.NoError(t, err)
	assert.True(t, second.IsHit)
	assert.Equal(t, "value", second.Result)
}

// TestServiceConvenience_DelegatesToOptions service 的两个 ExecuteWithIdempotency*
// 是 ExecuteWithOptions 的薄封装。
func TestServiceConvenience_DelegatesToOptions(t *testing.T) {
	e := newTestExecutor(StrategyCacheOnly)
	svc := e.service
	ctx := context.Background()

	result, err := svc.ExecuteWithIdempotency(ctx, "svc-key", time.Minute, func() (interface{}, error) {
		return "svc-value", nil
	})
	require.NoError(t, err)
	assert.Equal(t, "svc-value", result)

	detailed, err := svc.ExecuteWithOptions(ctx, &ExecutionOptions{Key: "svc-key", TTL: time.Minute}, func() (interface{}, error) {
		return "unexpected", nil
	})
	require.NoError(t, err)
	assert.True(t, detailed.IsHit)
	assert.Equal(t, "svc-value", detailed.Result)

	viaAndResult, err := svc.ExecuteWithIdempotencyAndResult(ctx, "svc-key", time.Minute, func() (interface{}, error) {
		return "unexpected", nil
	})
	require.NoError(t, err)
	assert.True(t, viaAndResult.IsHit)
	assert.Equal(t, "svc-value", viaAndResult.Result)
}

// TestRuntimeConfigShared IdempotencyConfig 与 ExecutorConfig 共用同一
// RuntimeConfig 类型，删除重复字段。
func TestRuntimeConfigShared(t *testing.T) {
	runtimeType := reflect.TypeOf(RuntimeConfig{})
	for _, typ := range []reflect.Type{
		reflect.TypeOf(IdempotencyConfig{}),
		reflect.TypeOf(ExecutorConfig{}),
	} {
		field, ok := typ.FieldByName("RuntimeConfig")
		require.True(t, ok, "%s must embed RuntimeConfig", typ.Name())
		assert.Equal(t, runtimeType, field.Type)
		assert.True(t, field.Anonymous)
	}

	// 提升字段保持可访问，源码兼容。
	idc := DefaultIdempotencyConfig()
	execCfg := DefaultExecutorConfig()
	assert.Equal(t, idc.DefaultTTL, execCfg.DefaultTTL)
	idc.RetryDelay = 7 * time.Millisecond
	assert.Equal(t, 7*time.Millisecond, idc.RetryDelay)
}
