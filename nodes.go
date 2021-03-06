package goblin

import (
	"reflect"
	"sync"
	"time"
)

// Node ...
type Node struct {
	Addr string
}

type leftNode struct {
	addr       string
	lastUpdate time.Time
}

type nodeMap struct {
	leftNodeTime time.Duration

	mu        sync.Mutex
	cond      *sync.Cond
	nodes     map[string]Node
	leftNodes map[string]leftNode
	seq       uint64
	getNow    func() time.Time
}

func newNodeMap(leftNodeTime time.Duration) *nodeMap {
	result := &nodeMap{
		leftNodeTime: leftNodeTime,
		nodes:        map[string]Node{},
		leftNodes:    map[string]leftNode{},
		seq:          0,
		getNow:       func() time.Time { return time.Now() },
	}
	result.cond = sync.NewCond(&result.mu)
	return result
}

func (n *nodeMap) nodeJoin(name string, addr string) {
	n.nodeJoinLock(name, addr)
	n.cond.Broadcast()
}

// leave because of Dead of Left
func (n *nodeMap) nodeLeave(name string) {
	n.nodeLeaveLock(name)
	n.cond.Broadcast()
}

func (n *nodeMap) nodeGracefulLeave(name string, addr string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	_, existed := n.leftNodes[name]
	if existed {
		return false
	}
	n.leftNodes[name] = leftNode{
		addr:       addr,
		lastUpdate: n.getNow(),
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

func (n *nodeMap) watcherShouldLeave() {
	n.mu.Lock()
	n.seq++
	n.mu.Unlock()

	n.cond.Broadcast()
}

func (n *nodeMap) getNotJoinedAddresses(addrs []string) (uint64, []string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	addressSet := map[string]struct{}{}
	for _, node := range n.nodes {
		addressSet[node.Addr] = struct{}{}
	}

	leftAddressMap := map[string][]string{}
	for name, node := range n.leftNodes {
		leftAddressMap[node.addr] = append(leftAddressMap[node.addr], name)
	}

	var result []string
	for _, addr := range addrs {
		_, existed := addressSet[addr]
		if existed {
			continue
		}

		_, existed = leftAddressMap[addr]
		if existed {
			delete(leftAddressMap, addr)
			continue
		}

		result = append(result, addr)
	}

	// clean nodes that are left and not in input addrs list
	for _, names := range leftAddressMap {
		for _, name := range names {
			_, existed := n.nodes[name]
			if existed {
				continue
			}

			if !n.leftNodes[name].lastUpdate.Add(n.leftNodeTime).After(n.getNow()) {
				delete(n.leftNodes, name)
			}
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

func cloneLeftNodes(nodes map[string]leftNode) map[string]leftNode {
	result := map[string]leftNode{}
	for k, v := range nodes {
		result[k] = v
	}
	return result
}

func (n *nodeMap) getLeftNodes() map[string]leftNode {
	n.mu.Lock()
	defer n.mu.Unlock()
	return cloneLeftNodes(n.leftNodes)
}

func (n *nodeMap) watchNodes(lastSeq uint64) (uint64, map[string]Node) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for n.seq <= lastSeq {
		n.cond.Wait()
	}

	return n.seq, n.nodes
}

func nodeMapSame(a, b map[string]Node) bool {
	return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer()
}
