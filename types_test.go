package idempotency

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStrategyConstants(t *testing.T) {
	assert.Equal(t, IdempotencyStrategy("cache_only"), StrategyCacheOnly)
	assert.Equal(t, IdempotencyStrategy("database_only"), StrategyDatabaseOnly)
	assert.Equal(t, IdempotencyStrategy("cache_first"), StrategyCacheFirst)
	assert.Equal(t, IdempotencyStrategy("write_both"), StrategyWriteBoth)
}

func TestRecordStatusConstants(t *testing.T) {
	assert.Equal(t, 0, RecordStatusProcessing)
	assert.Equal(t, 1, RecordStatusCompleted)
	assert.Equal(t, 2, RecordStatusFailed)
}

func TestExecutionResult_JSONRoundTrip(t *testing.T) {
	r := &ExecutionResult{
		Key:           "k1",
		Result:        "value",
		IsHit:         true,
		IsNew:         false,
		Strategy:      StrategyCacheFirst,
		ExpiresAt:     time.Unix(1700000000, 0).UTC(),
		ExecutionTime: 42 * time.Millisecond,
		Source:        "cache",
	}

	data, err := json.Marshal(r)
	require.NoError(t, err)

	var got ExecutionResult
	require.NoError(t, json.Unmarshal(data, &got))

	assert.Equal(t, "k1", got.Key)
	assert.Equal(t, "value", got.Result)
	assert.True(t, got.IsHit)
	assert.False(t, got.IsNew)
	assert.Equal(t, StrategyCacheFirst, got.Strategy)
	assert.Equal(t, "cache", got.Source)
	assert.True(t, got.ExpiresAt.Equal(time.Unix(1700000000, 0).UTC()))
}

func TestExecutionResult_EmptyResult_Omittable(t *testing.T) {
	r := &ExecutionResult{
		Key:      "k2",
		IsNew:    true,
		Strategy: StrategyDatabaseOnly,
	}
	data, err := json.Marshal(r)
	require.NoError(t, err)

	var m map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &m))

	_, hasResult := m["result"]
	assert.False(t, hasResult, "result should be omitempty when nil")
	assert.Equal(t, "k2", m["key"])
}

func TestIdempotencyRecord_JSONRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	completed := now
	rec := &IdempotencyRecord{
		ID:          10,
		Key:         "rk",
		Result:      []byte(`{"x":1}`),
		Status:      RecordStatusCompleted,
		CreatedAt:   now,
		UpdatedAt:   now,
		ExpiresAt:   now.Add(time.Hour),
		CompletedAt: &completed,
	}

	data, err := json.Marshal(rec)
	require.NoError(t, err)

	var got IdempotencyRecord
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, rec.ID, got.ID)
	assert.Equal(t, rec.Key, got.Key)
	assert.Equal(t, rec.Result, got.Result)
	assert.Equal(t, rec.Status, got.Status)
	require.NotNil(t, got.CompletedAt)
	assert.True(t, got.CompletedAt.Equal(completed))
}

func TestIdempotencyRecord_CompletedAtOmittable(t *testing.T) {
	rec := &IdempotencyRecord{Key: "k", CompletedAt: nil}
	data, err := json.Marshal(rec)
	require.NoError(t, err)

	var m map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &m))
	_, has := m["completed_at"]
	assert.False(t, has, "completed_at should be omitempty when nil")
}
