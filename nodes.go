package goblin

import (
	"fmt"
	"sync"
	"time"
)

// Node ...
type Node struct {
	Addr string
}

type leftNode struct {
	lastUpdate time.Time
}

type nodeMap struct {
	mu        sync.Mutex
	cond      *sync.Cond
	seq       uint64
	nodes     map[string]Node
	leftNodes map[string]leftNode
}

func newNodeMap() *nodeMap {
	result := &nodeMap{
		nodes:     map[string]Node{},
		leftNodes: map[string]leftNode{},
		seq:       0,
	}
	result.cond = sync.NewCond(&result.mu)
	return result
}

func (n *nodeMap) nodeJoin(name string, addr string) {
	fmt.Println("NodeJoin", name, addr)
	n.nodeJoinLock(name, addr)
	n.cond.Broadcast()
}

// leave because of Dead of Left
func (n *nodeMap) nodeLeave(name string) {
	fmt.Println("NodeLeave", name)
	n.nodeLeaveLock(name)
	n.cond.Broadcast()
}

func (n *nodeMap) nodeGracefulLeave(name string) bool {
	fmt.Println("NodeGracefulLeave", name)
	n.mu.Lock()
	defer n.mu.Unlock()

	_, existed := n.leftNodes[name]
	if existed {
		return false
	}
	n.leftNodes[name] = leftNode{
		lastUpdate: time.Now(),
	}
	return true
}

func (n *nodeMap) nodeJoinLock(name string, addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.nodes = cloneNodeMap(n.nodes)
	n.nodes[name] = Node{
		Addr: addr,
	}
	n.seq++
}

func (n *nodeMap) nodeLeaveLock(name string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.nodes = cloneNodeMap(n.nodes)
	delete(n.nodes, name)
	n.seq++
}

func (n *nodeMap) getNotJoinedAddresses(addrs []string) (uint64, []string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	addressSet := map[string]struct{}{}
	var result []string
	for _, node := range n.nodes {
		addressSet[node.Addr] = struct{}{}
	}

	for _, addr := range addrs {
		_, existed := addressSet[addr]
		if !existed {
			result = append(result, addr)
		}
	}

	return n.seq, result
}

func cloneNodeMap(nodes map[string]Node) map[string]Node {
	result := map[string]Node{}
	for k, v := range nodes {
		result[k] = v
	}
	return result
}

func (n *nodeMap) getNodes() (uint64, map[string]Node) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.seq, n.nodes
}

func (n *nodeMap) watchNodes(lastSeq uint64) (uint64, map[string]Node) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for n.seq <= lastSeq {
		n.cond.Wait()
	}

	return n.seq, n.nodes
}
