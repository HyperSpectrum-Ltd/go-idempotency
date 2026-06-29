package idempotency

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONSerializer_String(t *testing.T) {
	s := NewJSONSerializer()
	data, err := s.Marshal("hello")
	require.NoError(t, err)
	assert.Equal(t, `"hello"`, string(data))

	var got string
	require.NoError(t, s.Unmarshal(data, &got))
	assert.Equal(t, "hello", got)
}

func TestJSONSerializer_Int(t *testing.T) {
	s := NewJSONSerializer()
	data, err := s.Marshal(42)
	require.NoError(t, err)

	var got int
	require.NoError(t, s.Unmarshal(data, &got))
	assert.Equal(t, 42, got)
}

func TestJSONSerializer_Struct(t *testing.T) {
	type person struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	s := NewJSONSerializer()

	original := person{Name: "alice", Age: 30}
	data, err := s.Marshal(original)
	require.NoError(t, err)

	var got person
	require.NoError(t, s.Unmarshal(data, &got))
	assert.Equal(t, original, got)
}

func TestJSONSerializer_Slice(t *testing.T) {
	s := NewJSONSerializer()
	original := []int{1, 2, 3}
	data, err := s.Marshal(original)
	require.NoError(t, err)

	var got []int
	require.NoError(t, s.Unmarshal(data, &got))
	assert.Equal(t, original, got)
}

func TestJSONSerializer_Map(t *testing.T) {
	s := NewJSONSerializer()
	original := map[string]int{"a": 1, "b": 2}
	data, err := s.Marshal(original)
	require.NoError(t, err)

	var got map[string]int
	require.NoError(t, s.Unmarshal(data, &got))
	assert.Equal(t, original, got)
}

func TestJSONSerializer_InvalidJSON(t *testing.T) {
	s := NewJSONSerializer()
	var got map[string]int
	err := s.Unmarshal([]byte(`{invalid`), &got)
	require.Error(t, err)
}

func TestJSONSerializer_NilInput(t *testing.T) {
	s := NewJSONSerializer()
	data, err := s.Marshal(nil)
	require.NoError(t, err)
	assert.Equal(t, "null", string(data))
}

func TestJSONSerializer_EmptyBytes(t *testing.T) {
	s := NewJSONSerializer()
	var got map[string]int
	err := s.Unmarshal([]byte{}, &got)
	require.Error(t, err)
}

func TestJSONSerializer_NilTarget(t *testing.T) {
	s := NewJSONSerializer()
	// json.Unmarshal into nil target should error
	err := s.Unmarshal([]byte(`{"a":1}`), nil)
	require.Error(t, err)
}
