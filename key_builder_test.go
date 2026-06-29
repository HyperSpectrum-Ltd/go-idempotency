package idempotency

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnifiedKeyBuilder_AllFields(t *testing.T) {
	b := NewUnifiedKeyBuilder().
		Prefix("idempotency").
		Domain("billing").
		Resource("order").
		Action("create").
		TenantID(7).
		UserID(42).
		UniqueID("u-1").
		RequestID("r-9").
		Extra("tag1", "tag2").
		Suffix("end")

	// SkipTimestamp to make the output deterministic
	b.SkipTimestamp()
	key := b.Build()

	assert.Contains(t, key, "idempotency")
	assert.Contains(t, key, "billing")
	assert.Contains(t, key, "order")
	assert.Contains(t, key, "create")
	assert.Contains(t, key, "tenant:7")
	assert.Contains(t, key, "user:42")
	assert.Contains(t, key, "u-1")
	assert.Contains(t, key, "req:r-9")
	assert.Contains(t, key, "tag1")
	assert.Contains(t, key, "tag2")
	assert.True(t, strings.HasSuffix(key, ":end"))
}

func TestUnifiedKeyBuilder_SkipPrefix(t *testing.T) {
	// 设置前缀但通过 SkipPrefix 跳过，并跳过时间戳以获得确定性输出
	key := NewUnifiedKeyBuilder().
		Prefix("pre").
		Domain("d").
		SkipPrefix().
		SkipTimestamp().
		Build()
	assert.Equal(t, "d", key)

	// 对照：不调用 SkipPrefix 时前缀应被包含
	keyWithPrefix := NewUnifiedKeyBuilder().
		Prefix("pre").
		Domain("d").
		SkipTimestamp().
		Build()
	assert.Equal(t, "pre:d", keyWithPrefix)
}

func TestUnifiedKeyBuilder_Empty(t *testing.T) {
	b := NewUnifiedKeyBuilder()
	b.SkipTimestamp()
	assert.Equal(t, "", b.Build())
}

func TestUnifiedKeyBuilder_Delimiter(t *testing.T) {
	b := NewUnifiedKeyBuilder().
		Domain("d").
		Resource("r").
		Delimiter("-").
		SkipTimestamp()
	assert.Equal(t, "d-r", b.Build())
}

func TestUnifiedKeyBuilder_BuildShort(t *testing.T) {
	b := NewUnifiedKeyBuilder().
		Domain("billing").
		Resource("order").
		UserID(42)
	short := b.BuildShort()
	// 8 bytes hex = 16 chars
	assert.Len(t, short, 16)
}

func TestUnifiedKeyBuilder_BuildWithHash(t *testing.T) {
	b := NewUnifiedKeyBuilder().
		Domain("d").
		Resource("r").
		SkipTimestamp()
	key := b.BuildWithHash()
	// should contain the original key + delimiter + 8 hex chars (4 bytes)
	assert.Contains(t, key, "d:r")
	parts := strings.Split(key, ":")
	last := parts[len(parts)-1]
	assert.Len(t, last, 8)
}

func TestUnifiedKeyBuilder_BuildWithPrefix(t *testing.T) {
	b := NewUnifiedKeyBuilder().
		Domain("d").
		SkipTimestamp()
	assert.Equal(t, "pre:d", b.BuildWithPrefix("pre"))
}

func TestUnifiedKeyBuilder_BuildForDomain(t *testing.T) {
	b := NewUnifiedKeyBuilder().Resource("r").SkipTimestamp()
	assert.Equal(t, "dom:r", b.BuildForDomain("dom"))
}

func TestUnifiedKeyBuilder_Reset(t *testing.T) {
	b := NewUnifiedKeyBuilder().
		Domain("d").
		Resource("r").
		Action("a").
		UserID(1).
		TenantID(2).
		UniqueID("u").
		Prefix("p").
		Suffix("s").
		Extra("e").
		Delimiter("-")
	b.Reset()

	assert.Equal(t, "", b.domain)
	assert.Equal(t, "", b.resource)
	assert.Equal(t, "", b.action)
	assert.Equal(t, int64(0), b.userID)
	assert.Equal(t, int64(0), b.tenantID)
	assert.Equal(t, ":", b.delimiter)
	assert.False(t, b.skipPrefix)
	assert.Nil(t, b.extra)

	b2 := b.Domain("x").SkipTimestamp()
	assert.Equal(t, "x", b2.Build())
}

func TestUnifiedKeyBuilder_Clone(t *testing.T) {
	b := NewUnifiedKeyBuilder().
		Domain("d").
		Resource("r").
		UserID(1).
		Extra("e1", "e2").
		SkipTimestamp()

	clone := b.Clone()
	// Mutate original
	b.Domain("changed").Extra("e3")

	// Clone unaffected
	assert.Equal(t, "d", clone.domain)
	assert.Equal(t, "r", clone.resource)
	assert.Equal(t, []string{"e1", "e2"}, clone.extra)
}

func TestUnifiedKeyBuilder_Validate(t *testing.T) {
	b := NewUnifiedKeyBuilder().Domain("d").SkipTimestamp()
	require.NoError(t, b.Validate())

	empty := NewUnifiedKeyBuilder()
	empty.SkipTimestamp()
	require.Error(t, empty.Validate())
}

func TestParseKey(t *testing.T) {
	m := ParseKey("domain:resource:action:tenant:7:user:42")
	assert.Equal(t, "domain", m["domain"])
	assert.Equal(t, "resource", m["resource"])
	assert.Equal(t, "action", m["action"])
	assert.Equal(t, "tenant:7:user:42", m["identifier"])
}

func TestParseKey_Short(t *testing.T) {
	m := ParseKey("only-one")
	assert.Equal(t, "only-one", m["domain"])
	_, ok := m["resource"]
	assert.False(t, ok)
}

func TestExtractUserIDFromKey(t *testing.T) {
	assert.Equal(t, int64(42), ExtractUserIDFromKey("billing:order:user:42:action"))
	assert.Equal(t, int64(0), ExtractUserIDFromKey("no:user:here"))
	assert.Equal(t, int64(0), ExtractUserIDFromKey("user"))
}

func TestExtractTenantIDFromKey(t *testing.T) {
	assert.Equal(t, int64(7), ExtractTenantIDFromKey("billing:tenant:7:order"))
	assert.Equal(t, int64(0), ExtractTenantIDFromKey("no:tenant:here"))
	assert.Equal(t, int64(0), ExtractTenantIDFromKey("tenant"))
}

func TestBuildUserActionKey(t *testing.T) {
	key := BuildUserActionKey("billing", "order", "create", 42)
	assert.Contains(t, key, "billing")
	assert.Contains(t, key, "order")
	assert.Contains(t, key, "create")
	assert.Contains(t, key, "user:42")
}

func TestBuildTenantActionKey(t *testing.T) {
	key := BuildTenantActionKey("billing", "order", "create", 7)
	assert.Contains(t, key, "tenant:7")
}

func TestBuildCompositeKey(t *testing.T) {
	assert.Equal(t, "a:b:c", BuildCompositeKey("a", "b", "c"))
}

func TestFormatKey(t *testing.T) {
	assert.Equal(t, "key:42", FormatKey("key:%d", 42))
}

func TestJoinKey_SplitKey(t *testing.T) {
	joined := JoinKey("a", "b", "c")
	assert.Equal(t, "a:b:c", joined)
	parts := SplitKey(joined)
	assert.Equal(t, []string{"a", "b", "c"}, parts)
}

func TestValidateKey(t *testing.T) {
	require.NoError(t, ValidateKey("any-key"))
	require.Error(t, ValidateKey(""))
}

func TestBuildKeyWithOptions(t *testing.T) {
	key := BuildKeyWithOptions(
		WithDomain("d"),
		WithResource("r"),
		WithAction("a"),
		WithUserID(1),
		WithTenantID(2),
		WithUniqueID("u"),
	)
	assert.Contains(t, key, "d")
	assert.Contains(t, key, "r")
	assert.Contains(t, key, "a")
	assert.Contains(t, key, "user:1")
	assert.Contains(t, key, "tenant:2")
	assert.Contains(t, key, "u")
}

// Bug 6: KeyBuilderRegistry 并发安全测试
func TestKeyBuilderRegistry_Concurrent(t *testing.T) {
	r := NewKeyBuilderRegistry()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				name := "builder"
				b := NewUnifiedKeyBuilder().Domain("d").SkipTimestamp()
				r.Register(name, b)
				if got, ok := r.Get(name); ok {
					_ = got.Build()
				}
				r.GetOrRegister(name, b)
				_ = r.Build(name)
				_ = r.All()
			}
		}(i)
	}
	wg.Wait()
}

func TestKeyBuilderRegistry_Basic(t *testing.T) {
	r := NewKeyBuilderRegistry()
	b := NewUnifiedKeyBuilder().Domain("d").Resource("r").SkipTimestamp()
	r.Register("k", b)

	got, ok := r.Get("k")
	require.True(t, ok)
	require.NotNil(t, got)
	assert.Equal(t, "d:r", got.Build())

	// Get returns a clone; mutating it must not affect registry
	got.Domain("mutated")
	again, _ := r.Get("k")
	assert.Equal(t, "d:r", again.Build())
}

func TestKeyBuilderRegistry_GetMissing(t *testing.T) {
	r := NewKeyBuilderRegistry()
	got, ok := r.Get("nope")
	assert.False(t, ok)
	assert.Nil(t, got)
}

func TestKeyBuilderRegistry_BuildMissing(t *testing.T) {
	r := NewKeyBuilderRegistry()
	assert.Equal(t, "", r.Build("nope"))
}

func TestKeyBuilderRegistry_GetOrRegister(t *testing.T) {
	r := NewKeyBuilderRegistry()
	b1 := NewUnifiedKeyBuilder().Domain("d").SkipTimestamp()
	got1 := r.GetOrRegister("k", b1)
	assert.Equal(t, "d", got1.Build())

	// second call returns existing (clone)
	b2 := NewUnifiedKeyBuilder().Domain("other").SkipTimestamp()
	got2 := r.GetOrRegister("k", b2)
	assert.Equal(t, "d", got2.Build(), "should return existing, not new")
}

func TestKeyBuilderRegistry_All_ReturnsClones(t *testing.T) {
	r := NewKeyBuilderRegistry()
	r.Register("a", NewUnifiedKeyBuilder().Domain("da").SkipTimestamp())
	r.Register("b", NewUnifiedKeyBuilder().Domain("db").SkipTimestamp())

	all := r.All()
	assert.Len(t, all, 2)
	// Mutate returned clones; registry should be unaffected
	all["a"].Domain("mutated")
	again, _ := r.Get("a")
	assert.Equal(t, "da", again.Build())
}
