package idempotency

import (
	"sync"
	"time"
)

// MetricsCollector 指标收集器接口
type MetricsCollector interface {
	RecordHit(strategy IdempotencyStrategy, source string)
	RecordMiss(strategy IdempotencyStrategy)
	RecordError(strategy IdempotencyStrategy, err error)
	RecordLatency(strategy IdempotencyStrategy, duration time.Duration)
}

// DefaultMetricsCollector 默认指标收集器
type DefaultMetricsCollector struct {
	hits    map[string]int64
	misses  map[string]int64
	errors  map[string]int64
	latency map[string]time.Duration
	mu      sync.RWMutex
}

// NewDefaultMetricsCollector 创建默认指标收集器
func NewDefaultMetricsCollector() *DefaultMetricsCollector {
	return &DefaultMetricsCollector{
		hits:    make(map[string]int64),
		misses:  make(map[string]int64),
		errors:  make(map[string]int64),
		latency: make(map[string]time.Duration),
	}
}

// RecordHit 记录命中
func (m *DefaultMetricsCollector) RecordHit(strategy IdempotencyStrategy, source string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := string(strategy) + ":" + source
	m.hits[key]++
}

// RecordMiss 记录未命中
func (m *DefaultMetricsCollector) RecordMiss(strategy IdempotencyStrategy) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.misses[string(strategy)]++
}

// RecordError 记录错误
func (m *DefaultMetricsCollector) RecordError(strategy IdempotencyStrategy, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errors[string(strategy)]++
}

// RecordLatency 记录延迟
func (m *DefaultMetricsCollector) RecordLatency(strategy IdempotencyStrategy, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latency[string(strategy)] += duration
}

// GetStats 获取统计（返回深拷贝，避免锁释放后调用方与写入者竞争）
func (m *DefaultMetricsCollector) GetStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	hitsCopy := make(map[string]int64, len(m.hits))
	for k, v := range m.hits {
		hitsCopy[k] = v
	}
	missesCopy := make(map[string]int64, len(m.misses))
	for k, v := range m.misses {
		missesCopy[k] = v
	}
	errorsCopy := make(map[string]int64, len(m.errors))
	for k, v := range m.errors {
		errorsCopy[k] = v
	}
	latencyCopy := make(map[string]time.Duration, len(m.latency))
	for k, v := range m.latency {
		latencyCopy[k] = v
	}

	return map[string]interface{}{
		"hits":    hitsCopy,
		"misses":  missesCopy,
		"errors":  errorsCopy,
		"latency": latencyCopy,
	}
}

// NoOpMetricsCollector 空操作指标收集器
type NoOpMetricsCollector struct{}

// RecordHit 记录命中
func (m *NoOpMetricsCollector) RecordHit(strategy IdempotencyStrategy, source string) {}

// RecordMiss 记录未命中
func (m *NoOpMetricsCollector) RecordMiss(strategy IdempotencyStrategy) {}

// RecordError 记录错误
func (m *NoOpMetricsCollector) RecordError(strategy IdempotencyStrategy, err error) {}

// RecordLatency 记录延迟
func (m *NoOpMetricsCollector) RecordLatency(strategy IdempotencyStrategy, duration time.Duration) {}
