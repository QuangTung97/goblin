package goblin

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestNodes_GetNotJoined(t *testing.T) {
	n := newNodeMap()
	n.nodeJoin("name-1", "address-1")
	n.nodeJoin("name-2", "address-2")
	n.nodeJoin("name-4", "address-4")
	n.nodeJoin("name-5", "address-5")

	seq, result := n.getNotJoinedAddresses([]string{
		"address-1", "address-2",
		"address-3", "address-4",
		"address-5", "address-6",
	})
	assert.Equal(t, []string{
		"address-3", "address-6",
	}, result)
	assert.Equal(t, uint64(4), seq)

	n.nodeLeave("name-4")
	seq, result = n.getNotJoinedAddresses([]string{
		"address-1", "address-2",
		"address-3", "address-4",
		"address-5", "address-6",
	})
	assert.Equal(t, []string{
		"address-3", "address-4", "address-6",
	}, result)
	assert.Equal(t, uint64(5), seq)
}

func TestNodes_WatchNodes(t *testing.T) {
	n := newNodeMap()
	seq, nodes := n.getNodes()
	assert.Equal(t, uint64(0), seq)
	assert.Equal(t, map[string]Node{}, nodes)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		seq, nodes = n.watchNodes(0)
	}()

	n.nodeJoin("name-1", "address-1")

	wg.Wait()

	assert.Equal(t, map[string]Node{
		"name-1": {
			Addr: "address-1",
		},
	}, nodes)

	n.nodeJoin("name-2", "address-2")

	assert.Equal(t, map[string]Node{
		"name-1": {
			Addr: "address-1",
		},
	}, nodes)
}
