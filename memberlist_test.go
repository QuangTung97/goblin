package goblin

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMarshalUnmarshalBroadcast(t *testing.T) {
	b := broadcast{name: "name-1", addr: "address-1"}
	result := marshalBroadcast(b)
	assert.Equal(t, "name-1@address-1", string(result))
	b1, ok := unmarshalBroadcast(result)
	assert.Equal(t, true, ok)
	assert.Equal(t, b, b1)
}
