package goblin

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMarshalUnmarshalBroadcast(t *testing.T) {
	b := broadcast{name: "name-1", addr: "address-1"}
	result := marshalBroadcast(b)
	assert.Equal(t, "name-1@address-1", string(result))
	b1, ok := unmarshalBroadcast(result)
	assert.Equal(t, true, ok)
	assert.Equal(t, b, b1)
}

func TestComputeLeftNodesState(t *testing.T) {
	n := newNodeMap(30 * time.Second)
	n.nodeJoin("name-1", "address-1")
	n.nodeJoin("name-2", "address-2")
	n.nodeGracefulLeave("name-1", "address-1")
	n.nodeGracefulLeave("name-2", "address-2")

	result := computeLeftNodesState(n)
	s := string(result)
	if s != "name-1@address-1,name-2@address-2" && s != "name-2@address-2,name-1@address-1" {
		t.Error("computeLeftNodesState error", s)
	}
}

func TestComputeLeftNodesState_Empty(t *testing.T) {
	n := newNodeMap(30 * time.Second)
	result := computeLeftNodesState(n)
	assert.Equal(t, "", string(result))
}

func TestRemoteStateToBroadcast(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		result := remoteStateToBroadcast([]byte(""))
		assert.Equal(t, []broadcast(nil), result)
	})

	t.Run("normal", func(t *testing.T) {
		result := remoteStateToBroadcast([]byte("name-1@address-1,name-2@address-2"))
		assert.Equal(t, []broadcast{
			{
				name: "name-1",
				addr: "address-1",
			},
			{
				name: "name-2",
				addr: "address-2",
			},
		}, result)
	})
}
