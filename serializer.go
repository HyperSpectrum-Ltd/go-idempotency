package idempotency

import "encoding/json"

// Serializer 序列化接口
// 定义幂等性结果的序列化和反序列化操作
type Serializer interface {
	// Marshal 将对象序列化为字节数组
	Marshal(v interface{}) ([]byte, error)

	// Unmarshal 将字节数组反序列化为对象
	Unmarshal(data []byte, v interface{}) error
}

// JSONSerializer JSON 序列化实现
type JSONSerializer struct{}

// NewJSONSerializer 创建 JSON 序列化器
func NewJSONSerializer() *JSONSerializer {
	return &JSONSerializer{}
}

// Marshal 实现序列化
func (s *JSONSerializer) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// Unmarshal 实现反序列化
func (s *JSONSerializer) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
