package goblin

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestNodes_GetNotJoined(t *testing.T) {
	n := newNodeMap(30 * time.Second)
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
	n := newNodeMap(30 * time.Second)
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

func TestNodes_GracefulLeave_CheckContinue(t *testing.T) {
	n := newNodeMap(30 * time.Second)
	n.nodeJoin("name-1", "address-1")
	n.nodeJoin("name-2", "address-2")
	n.nodeJoin("name-3", "address-3")
	n.nodeJoin("name-4", "address-4")

	continued := n.nodeGracefulLeave("name-3", "address-3")
	assert.Equal(t, true, continued)

	continued = n.nodeGracefulLeave("name-3", "address-3")
	assert.Equal(t, false, continued)
}

func mustParse(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

func TestNodes_GracefulLeave_GetNotJoinedAddresses(t *testing.T) {
	n := newNodeMap(30 * time.Second)
	n.nodeJoin("name-1", "address-1")
	n.nodeJoin("name-2", "address-2")
	n.nodeJoin("name-3", "address-3")
	n.nodeJoin("name-4", "address-4")

	now1 := mustParse("2021-06-18T09:00:00+07:00")
	n.getNow = func() time.Time { return now1 }

	n.nodeGracefulLeave("name-3", "address-3")
	assert.Equal(t, map[string]leftNode{
		"name-3": {
			addr:       "address-3",
			lastUpdate: now1,
		},
	}, n.leftNodes)

	seq, result := n.getNotJoinedAddresses([]string{"address-2", "address-3"})
	assert.Equal(t, uint64(4), seq)
	assert.Equal(t, []string(nil), result)

	n.nodeLeave("name-3")

	now2 := mustParse("2021-06-18T09:00:30+07:00")
	n.getNow = func() time.Time { return now2 }

	seq, result = n.getNotJoinedAddresses([]string{"address-2", "address-3"})
	assert.Equal(t, uint64(5), seq)
	assert.Equal(t, []string(nil), result)
	assert.Equal(t, map[string]leftNode{
		"name-3": {
			addr:       "address-3",
			lastUpdate: now1,
		},
	}, n.leftNodes)
}

func TestNodes_GracefulLeave_GetNotJoinedAddresses_RemoveLeftNode(t *testing.T) {
	n := newNodeMap(30 * time.Second)
	n.nodeJoin("name-1", "address-1")
	n.nodeJoin("name-2", "address-2")
	n.nodeJoin("name-3", "address-3")
	n.nodeJoin("name-4", "address-4")

	now1 := mustParse("2021-06-18T09:00:00+07:00")
	n.getNow = func() time.Time { return now1 }

	n.nodeGracefulLeave("name-3", "address-3")

	now2 := mustParse("2021-06-18T09:00:30+07:00")
	n.getNow = func() time.Time { return now2 }

	seq, result := n.getNotJoinedAddresses([]string{"address-2"})
	assert.Equal(t, uint64(4), seq)
	assert.Equal(t, []string(nil), result)
	assert.Equal(t, map[string]leftNode{
		"name-3": {
			addr:       "address-3",
			lastUpdate: now1,
		},
	}, n.leftNodes)

	n.nodeLeave("name-3")

	seq, result = n.getNotJoinedAddresses([]string{"address-2"})
	assert.Equal(t, uint64(5), seq)
	assert.Equal(t, []string(nil), result)
	assert.Equal(t, map[string]leftNode{}, n.leftNodes)
}

func TestNodes_GracefulLeave_GetNotJoinedAddresses_Same_Address(t *testing.T) {
	n := newNodeMap(30 * time.Second)
	n.nodeJoin("name-1", "address-1")
	n.nodeJoin("name-2", "address-2")
	n.nodeJoin("name-3", "address-3")
	n.nodeJoin("name-4", "address-4")

	now1 := mustParse("2021-06-18T09:00:00+07:00")
	n.getNow = func() time.Time { return now1 }

	n.nodeGracefulLeave("name-3", "address-3")
	n.nodeLeave("name-3")

	n.nodeJoin("name-5", "address-3")
	n.nodeGracefulLeave("name-5", "address-3")
	n.nodeLeave("name-5")

	n.nodeJoin("name-6", "address-3")

	now2 := mustParse("2021-06-18T09:00:30+07:00")
	n.getNow = func() time.Time { return now2 }

	seq, result := n.getNotJoinedAddresses([]string{"address-2", "address-3"})
	assert.Equal(t, uint64(8), seq)
	assert.Equal(t, []string(nil), result)
	assert.Equal(t, map[string]leftNode{}, n.leftNodes)
}

func TestNodes_GracefulLeave_After_Node_Leave(t *testing.T) {
	n := newNodeMap(30 * time.Second)
	n.nodeJoin("name-1", "address-1")
	n.nodeJoin("name-2", "address-2")
	n.nodeJoin("name-3", "address-3")
	n.nodeJoin("name-4", "address-4")

	now1 := mustParse("2021-06-18T09:00:00+07:00")
	n.getNow = func() time.Time { return now1 }

	n.nodeLeave("name-3")
	n.nodeGracefulLeave("name-3", "address-3")

	seq, result := n.getNotJoinedAddresses([]string{"address-2", "address-3"})
	assert.Equal(t, uint64(5), seq)
	assert.Equal(t, []string(nil), result)
	assert.Equal(t, map[string]leftNode{
		"name-3": {
			addr:       "address-3",
			lastUpdate: now1,
		},
	}, n.leftNodes)
}
