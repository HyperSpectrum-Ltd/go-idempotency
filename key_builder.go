package idempotency

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// UnifiedKeyBuilder 统一键构建器（增强版流式 API）
type UnifiedKeyBuilder struct {
	domain     string
	resource   string
	action     string
	userID     int64
	tenantID   int64
	uniqueID   string
	requestID  string
	timestamp  int64
	extra      []string
	prefix     string
	suffix     string
	delimiter  string
	skipPrefix bool
}

// NewUnifiedKeyBuilder 创建统一键构建器
func NewUnifiedKeyBuilder() *UnifiedKeyBuilder {
	return &UnifiedKeyBuilder{
		timestamp: time.Now().Unix(),
		delimiter: ":",
	}
}

// Domain 设置领域
func (b *UnifiedKeyBuilder) Domain(domain string) *UnifiedKeyBuilder {
	b.domain = domain
	return b
}

// Resource 设置资源
func (b *UnifiedKeyBuilder) Resource(resource string) *UnifiedKeyBuilder {
	b.resource = resource
	return b
}

// Action 设置操作
func (b *UnifiedKeyBuilder) Action(action string) *UnifiedKeyBuilder {
	b.action = action
	return b
}

// UserID 设置用户 ID
func (b *UnifiedKeyBuilder) UserID(userID int64) *UnifiedKeyBuilder {
	b.userID = userID
	return b
}

// TenantID 设置租户 ID
func (b *UnifiedKeyBuilder) TenantID(tenantID int64) *UnifiedKeyBuilder {
	b.tenantID = tenantID
	return b
}

// UniqueID 设置唯一标识
func (b *UnifiedKeyBuilder) UniqueID(uniqueID string) *UnifiedKeyBuilder {
	b.uniqueID = uniqueID
	return b
}

// SkipTimestamp 跳过时间戳（用于幂等性键）
func (b *UnifiedKeyBuilder) SkipTimestamp() *UnifiedKeyBuilder {
	b.timestamp = 0
	return b
}

// RequestID 设置请求 ID
func (b *UnifiedKeyBuilder) RequestID(requestID string) *UnifiedKeyBuilder {
	b.requestID = requestID
	return b
}

// Timestamp 设置时间戳
func (b *UnifiedKeyBuilder) Timestamp(timestamp int64) *UnifiedKeyBuilder {
	b.timestamp = timestamp
	return b
}

// Prefix 设置前缀
func (b *UnifiedKeyBuilder) Prefix(prefix string) *UnifiedKeyBuilder {
	b.prefix = prefix
	return b
}

// Suffix 设置后缀
func (b *UnifiedKeyBuilder) Suffix(suffix string) *UnifiedKeyBuilder {
	b.suffix = suffix
	return b
}

// Extra 添加额外部分
func (b *UnifiedKeyBuilder) Extra(parts ...string) *UnifiedKeyBuilder {
	b.extra = append(b.extra, parts...)
	return b
}

// Delimiter 设置分隔符
func (b *UnifiedKeyBuilder) Delimiter(delimiter string) *UnifiedKeyBuilder {
	b.delimiter = delimiter
	return b
}

// SkipPrefix 跳过前缀
func (b *UnifiedKeyBuilder) SkipPrefix() *UnifiedKeyBuilder {
	b.skipPrefix = true
	return b
}

// Build 构建键
func (b *UnifiedKeyBuilder) Build() string {
	var parts []string

	// 添加前缀
	if !b.skipPrefix && b.prefix != "" {
		parts = append(parts, b.prefix)
	}

	// 添加领域
	if b.domain != "" {
		parts = append(parts, b.domain)
	}

	// 添加资源
	if b.resource != "" {
		parts = append(parts, b.resource)
	}

	// 添加操作
	if b.action != "" {
		parts = append(parts, b.action)
	}

	// 添加租户 ID
	if b.tenantID != 0 {
		parts = append(parts, "tenant:"+strconv.FormatInt(b.tenantID, 10))
	}

	// 添加用户 ID
	if b.userID != 0 {
		parts = append(parts, "user:"+strconv.FormatInt(b.userID, 10))
	}

	// 添加唯一标识
	if b.uniqueID != "" {
		parts = append(parts, b.uniqueID)
	}

	// 添加请求 ID
	if b.requestID != "" {
		parts = append(parts, "req:"+b.requestID)
	}

	// 添加时间戳
	if b.timestamp != 0 {
		parts = append(parts, strconv.FormatInt(b.timestamp, 10))
	}

	// 添加额外部分
	if len(b.extra) > 0 {
		parts = append(parts, b.extra...)
	}

	// 添加后缀
	if b.suffix != "" {
		parts = append(parts, b.suffix)
	}

	return strings.Join(parts, b.delimiter)
}

// BuildShort 构建短键（使用哈希）
func (b *UnifiedKeyBuilder) BuildShort() string {
	key := b.Build()
	hash := sha256.Sum256([]byte(key))
	return hex.EncodeToString(hash[:8])
}

// BuildWithHash 构建带哈希的键
func (b *UnifiedKeyBuilder) BuildWithHash() string {
	key := b.Build()
	hash := sha256.Sum256([]byte(key))
	return key + b.delimiter + hex.EncodeToString(hash[:4])
}

// BuildWithPrefix 使用指定前缀构建
func (b *UnifiedKeyBuilder) BuildWithPrefix(prefix string) string {
	return prefix + b.delimiter + b.Build()
}

// BuildForDomain 为特定领域构建键
func (b *UnifiedKeyBuilder) BuildForDomain(domain string) string {
	b.domain = domain
	return b.Build()
}

// Reset 重置构建器
func (b *UnifiedKeyBuilder) Reset() *UnifiedKeyBuilder {
	b.domain = ""
	b.resource = ""
	b.action = ""
	b.userID = 0
	b.tenantID = 0
	b.uniqueID = ""
	b.requestID = ""
	b.timestamp = time.Now().Unix()
	b.extra = nil
	b.prefix = ""
	b.suffix = ""
	b.delimiter = ":"
	b.skipPrefix = false
	return b
}

// Clone 克隆构建器
func (b *UnifiedKeyBuilder) Clone() *UnifiedKeyBuilder {
	return &UnifiedKeyBuilder{
		domain:     b.domain,
		resource:   b.resource,
		action:     b.action,
		userID:     b.userID,
		tenantID:   b.tenantID,
		uniqueID:   b.uniqueID,
		requestID:  b.requestID,
		timestamp:  b.timestamp,
		extra:      append([]string{}, b.extra...),
		prefix:     b.prefix,
		suffix:     b.suffix,
		delimiter:  b.delimiter,
		skipPrefix: b.skipPrefix,
	}
}

// Validate 验证键
func (b *UnifiedKeyBuilder) Validate() error {
	key := b.Build()
	return ValidateKey(key)
}

// ===== 通用工具函数 =====

// BuildUserActionKey 构建用户操作键
func BuildUserActionKey(domain, resource, action string, userID int64) string {
	return NewUnifiedKeyBuilder().
		Domain(domain).
		Resource(resource).
		Action(action).
		UserID(userID).
		Build()
}

// BuildTenantActionKey 构建租户操作键
func BuildTenantActionKey(domain, resource, action string, tenantID int64) string {
	return NewUnifiedKeyBuilder().
		Domain(domain).
		Resource(resource).
		Action(action).
		TenantID(tenantID).
		Build()
}

// BuildCompositeKey 构建复合键
func BuildCompositeKey(parts ...string) string {
	return strings.Join(parts, ":")
}

// ParseKey 解析键
func ParseKey(key string) map[string]string {
	result := make(map[string]string)
	parts := strings.Split(key, ":")

	if len(parts) > 0 {
		result["domain"] = parts[0]
	}
	if len(parts) > 1 {
		result["resource"] = parts[1]
	}
	if len(parts) > 2 {
		result["action"] = parts[2]
	}
	if len(parts) > 3 {
		result["identifier"] = strings.Join(parts[3:], ":")
	}

	return result
}

// ExtractUserIDFromKey 从键中提取用户 ID
func ExtractUserIDFromKey(key string) int64 {
	parts := strings.Split(key, ":")
	for i, part := range parts {
		if part == "user" && i+1 < len(parts) {
			userID, _ := strconv.ParseInt(parts[i+1], 10, 64)
			return userID
		}
	}
	return 0
}

// ExtractTenantIDFromKey 从键中提取租户 ID
func ExtractTenantIDFromKey(key string) int64 {
	parts := strings.Split(key, ":")
	for i, part := range parts {
		if part == "tenant" && i+1 < len(parts) {
			tenantID, _ := strconv.ParseInt(parts[i+1], 10, 64)
			return tenantID
		}
	}
	return 0
}

// KeyBuilderOption 键构建器选项
type KeyBuilderOption func(*UnifiedKeyBuilder)

// WithDomain 设置领域的选项
func WithDomain(domain string) KeyBuilderOption {
	return func(b *UnifiedKeyBuilder) {
		b.domain = domain
	}
}

// WithResource 设置资源的选项
func WithResource(resource string) KeyBuilderOption {
	return func(b *UnifiedKeyBuilder) {
		b.resource = resource
	}
}

// WithAction 设置操作的选项
func WithAction(action string) KeyBuilderOption {
	return func(b *UnifiedKeyBuilder) {
		b.action = action
	}
}

// WithUserID 设置用户 ID 的选项
func WithUserID(userID int64) KeyBuilderOption {
	return func(b *UnifiedKeyBuilder) {
		b.userID = userID
	}
}

// WithTenantID 设置租户 ID 的选项
func WithTenantID(tenantID int64) KeyBuilderOption {
	return func(b *UnifiedKeyBuilder) {
		b.tenantID = tenantID
	}
}

// WithUniqueID 设置唯一标识的选项
func WithUniqueID(uniqueID string) KeyBuilderOption {
	return func(b *UnifiedKeyBuilder) {
		b.uniqueID = uniqueID
	}
}

// BuildKeyWithOptions 使用选项构建键
func BuildKeyWithOptions(opts ...KeyBuilderOption) string {
	builder := NewUnifiedKeyBuilder()
	for _, opt := range opts {
		opt(builder)
	}
	return builder.Build()
}

// KeyBuilderRegistry 键构建器注册表
type KeyBuilderRegistry struct {
	mu       sync.RWMutex
	builders map[string]*UnifiedKeyBuilder
}

// NewKeyBuilderRegistry 创建键构建器注册表
func NewKeyBuilderRegistry() *KeyBuilderRegistry {
	return &KeyBuilderRegistry{
		builders: make(map[string]*UnifiedKeyBuilder),
	}
}

// Register 注册构建器
func (r *KeyBuilderRegistry) Register(name string, builder *UnifiedKeyBuilder) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.builders[name] = builder
}

// Get 获取构建器（返回克隆，避免外部修改影响注册表内部状态）
func (r *KeyBuilderRegistry) Get(name string) (*UnifiedKeyBuilder, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	builder, ok := r.builders[name]
	if !ok {
		return nil, false
	}
	return builder.Clone(), true
}

// GetOrRegister 获取或注册构建器
func (r *KeyBuilderRegistry) GetOrRegister(name string, builder *UnifiedKeyBuilder) *UnifiedKeyBuilder {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.builders[name]; ok {
		return existing.Clone()
	}
	r.builders[name] = builder
	return builder.Clone()
}

// Build 构建指定名称的键
func (r *KeyBuilderRegistry) Build(name string) string {
	r.mu.RLock()
	builder, ok := r.builders[name]
	r.mu.RUnlock()
	if !ok {
		return ""
	}
	return builder.Build()
}

// All 获取所有构建器（返回克隆）
func (r *KeyBuilderRegistry) All() map[string]*UnifiedKeyBuilder {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make(map[string]*UnifiedKeyBuilder, len(r.builders))
	for k, v := range r.builders {
		result[k] = v.Clone()
	}
	return result
}

// FormatKey 格式化键
func FormatKey(template string, args ...interface{}) string {
	return fmt.Sprintf(template, args...)
}

// JoinKey 连接键部分
func JoinKey(parts ...string) string {
	return strings.Join(parts, ":")
}

// SplitKey 分割键
func SplitKey(key string) []string {
	return strings.Split(key, ":")
}

// ValidateKey 验证幂等键格式
func ValidateKey(key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	return nil
}
