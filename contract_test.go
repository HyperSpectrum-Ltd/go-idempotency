package idempotency

import (
	"context"
	"sync"
	"testing"
	"time"

	dkport "github.com/HyperSpectrum-Ltd/go-domainkit/port"
)

type fakeLockProvider struct {
	mu     sync.Mutex
	held   map[string]string
	withNu int
}

func (f *fakeLockProvider) Lock(_ context.Context, key string, _ time.Duration) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.held == nil {
		f.held = make(map[string]string)
	}
	if _, ok := f.held[key]; ok {
		return "", false, nil
	}
	value := "token-" + key
	f.held[key] = value
	return value, true, nil
}

func (f *fakeLockProvider) Unlock(_ context.Context, key string, lockValue string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.held[key] == lockValue {
		delete(f.held, key)
	}
	return nil
}

func (f *fakeLockProvider) WithLock(_ context.Context, key string, _ time.Duration, fn func() error) error {
	f.mu.Lock()
	f.withNu++
	f.mu.Unlock()
	return fn()
}

// TestDistributedLockProvider_UsableAsDomainkitLock 证明 domainkit 的
// port.WithLock 可直接驱动本模块的锁提供者，无需额外适配器。
func TestIdempotencyConfig_MapsToDomainkitBackoff(t *testing.T) {
	cfg := &IdempotencyConfig{MaxRetryCount: 3, RetryDelay: 100 * time.Millisecond}
	bp := cfg.BackoffPolicy()

	if bp.MaxAttempts != 4 {
		t.Fatalf("MaxAttempts = %d, want 4", bp.MaxAttempts)
	}
	if bp.InitialDelay != 100*time.Millisecond || bp.MaxDelay != 100*time.Millisecond || bp.Multiplier != 1 {
		t.Fatalf("fixed-delay mapping mismatch: %+v", bp)
	}
	// 固定延迟：每次重试等待相同时间。
	for retry := 1; retry <= 3; retry++ {
		if got := bp.Delay(retry); got != 100*time.Millisecond {
			t.Fatalf("Delay(%d) = %v", retry, got)
		}
	}
}

func TestDistributedLockProvider_UsableAsDomainkitLock(t *testing.T) {
	var lock dkport.DistributedLock = &fakeLockProvider{}
	ctx := context.Background()

	if err := dkport.WithLock(ctx, lock, "job", time.Minute, func() error { return nil }); err != nil {
		t.Fatalf("WithLock: %v", err)
	}

	// 锁已释放，可再次获取。
	_, acquired, err := lock.Lock(ctx, "job", time.Minute)
	if err != nil || !acquired {
		t.Fatalf("re-acquire: acquired=%v err=%v", acquired, err)
	}
}
