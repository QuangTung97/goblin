package goblin

import (
	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestNodes_GetNotJoined(t *testing.T) {
	n := newNodeMap()
	n.updateNode("name-1", "address-1", memberlist.StateAlive)
	n.updateNode("name-2", "address-2", memberlist.StateSuspect)
	n.updateNode("name-3", "address-3", memberlist.StateDead)
	n.updateNode("name-4", "address-4", memberlist.StateLeft)
	n.updateNode("name-5", "address-5", memberlist.StateAlive)

	result := getNotJoinedAddresses(n.nodes, []string{
		"address-1", "address-2",
		"address-3", "address-4",
		"address-5", "address-6",
	})
	assert.Equal(t, []string{
		"address-3", "address-6",
	}, result)
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

	n.updateNode("name-1", "address-1", memberlist.StateSuspect)

	wg.Wait()

	assert.Equal(t, map[string]Node{
		"name-1": {
			Addr:  "address-1",
			State: memberlist.StateSuspect,
		},
	}, nodes)

	n.updateNode("name-2", "address-2", memberlist.StateAlive)

	assert.Equal(t, map[string]Node{
		"name-1": {
			Addr:  "address-1",
			State: memberlist.StateSuspect,
		},
	}, nodes)
}
