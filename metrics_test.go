package idempotency

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultMetricsCollector_RecordHit(t *testing.T) {
	m := NewDefaultMetricsCollector()
	m.RecordHit(StrategyCacheFirst, "cache")
	m.RecordHit(StrategyCacheFirst, "cache")
	m.RecordHit(StrategyCacheFirst, "database")
	m.RecordHit(StrategyDatabaseOnly, "database")

	stats := m.GetStats()
	hits := stats["hits"].(map[string]int64)
	assert.Equal(t, int64(2), hits["cache_first:cache"])
	assert.Equal(t, int64(1), hits["cache_first:database"])
	assert.Equal(t, int64(1), hits["database_only:database"])
}

func TestDefaultMetricsCollector_RecordMiss(t *testing.T) {
	m := NewDefaultMetricsCollector()
	m.RecordMiss(StrategyCacheFirst)
	m.RecordMiss(StrategyCacheFirst)
	m.RecordMiss(StrategyDatabaseOnly)

	stats := m.GetStats()
	misses := stats["misses"].(map[string]int64)
	assert.Equal(t, int64(2), misses["cache_first"])
	assert.Equal(t, int64(1), misses["database_only"])
}

func TestDefaultMetricsCollector_RecordError(t *testing.T) {
	m := NewDefaultMetricsCollector()
	m.RecordError(StrategyCacheFirst, errors.New("boom"))
	m.RecordError(StrategyCacheFirst, fmt.Errorf("wrap: %w", errors.New("x")))

	stats := m.GetStats()
	errs := stats["errors"].(map[string]int64)
	assert.Equal(t, int64(2), errs["cache_first"])
}

func TestDefaultMetricsCollector_RecordLatency(t *testing.T) {
	m := NewDefaultMetricsCollector()
	m.RecordLatency(StrategyCacheFirst, 10*time.Millisecond)
	m.RecordLatency(StrategyCacheFirst, 5*time.Millisecond)

	stats := m.GetStats()
	latency := stats["latency"].(map[string]time.Duration)
	assert.Equal(t, 15*time.Millisecond, latency["cache_first"])
}

func TestDefaultMetricsCollector_Empty(t *testing.T) {
	m := NewDefaultMetricsCollector()
	stats := m.GetStats()
	assert.Equal(t, 0, len(stats["hits"].(map[string]int64)))
	assert.Equal(t, 0, len(stats["misses"].(map[string]int64)))
	assert.Equal(t, 0, len(stats["errors"].(map[string]int64)))
	assert.Equal(t, 0, len(stats["latency"].(map[string]time.Duration)))
}

// Bug 10: GetStats 应返回深拷贝，并发写不竞争返回的 map
func TestDefaultMetricsCollector_GetStats_DeepCopy(t *testing.T) {
	m := NewDefaultMetricsCollector()
	m.RecordHit(StrategyCacheFirst, "cache")

	stats := m.GetStats()
	hits := stats["hits"].(map[string]int64)

	// 并发：一个 goroutine 持续写入，另一个读取返回的快照
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			m.RecordHit(StrategyCacheFirst, "cache")
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			// 读快照，不应 panic 也不应 race
			_ = hits["cache_first:cache"]
		}
	}()
	wg.Wait()

	// 快照是 GetStats 调用时的视图，不为负
	assert.GreaterOrEqual(t, hits["cache_first:cache"], int64(1))
}

func TestDefaultMetricsCollector_Concurrent(t *testing.T) {
	m := NewDefaultMetricsCollector()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				m.RecordHit(StrategyCacheFirst, "cache")
				m.RecordMiss(StrategyCacheFirst)
				m.RecordError(StrategyCacheFirst, errors.New("e"))
				m.RecordLatency(StrategyCacheFirst, time.Millisecond)
				_ = m.GetStats()
			}
		}(i)
	}
	wg.Wait()

	stats := m.GetStats()
	hits := stats["hits"].(map[string]int64)
	misses := stats["misses"].(map[string]int64)
	errs := stats["errors"].(map[string]int64)
	assert.Equal(t, int64(20*100), hits["cache_first:cache"])
	assert.Equal(t, int64(20*100), misses["cache_first"])
	assert.Equal(t, int64(20*100), errs["cache_first"])
}

func TestNoOpMetricsCollector_NoPanic(t *testing.T) {
	var m NoOpMetricsCollector
	assert.NotPanics(t, func() {
		m.RecordHit(StrategyCacheFirst, "cache")
		m.RecordMiss(StrategyCacheFirst)
		m.RecordError(StrategyCacheFirst, errors.New("e"))
		m.RecordLatency(StrategyCacheFirst, time.Millisecond)
	})
}

// 确保 NoOpMetricsCollector 实现 MetricsCollector 接口
func TestNoOpMetricsCollector_ImplementsInterface(t *testing.T) {
	var _ MetricsCollector = (*NoOpMetricsCollector)(nil)
	var _ MetricsCollector = (*DefaultMetricsCollector)(nil)
}
