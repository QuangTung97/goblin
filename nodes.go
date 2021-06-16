package goblin

import (
	"github.com/hashicorp/memberlist"
	"sync"
)

// Node ...
type Node struct {
	Addr  string
	State memberlist.NodeStateType
}

type nodeMap struct {
	mu    sync.Mutex
	cond  *sync.Cond
	seq   uint64
	nodes map[string]Node
}

func newNodeMap() *nodeMap {
	result := &nodeMap{
		nodes: map[string]Node{},
		seq:   0,
	}
	result.cond = sync.NewCond(&result.mu)
	return result
}

func (n *nodeMap) updateNode(name string, addr string, state memberlist.NodeStateType) {
	n.updateNodeLock(name, addr, state)
	n.cond.Broadcast()
}

func (n *nodeMap) updateNodeLock(name string, addr string, state memberlist.NodeStateType) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.nodes = cloneNodeMap(n.nodes)
	n.nodes[name] = Node{
		Addr:  addr,
		State: state,
	}
	n.seq++
}

func getNotJoinedAddresses(nodes map[string]Node, addrs []string) []string {
	addressSet := map[string]struct{}{}
	var result []string
	for _, node := range nodes {
		if node.State != memberlist.StateDead {
			addressSet[node.Addr] = struct{}{}
		}
	}

	for _, addr := range addrs {
		_, existed := addressSet[addr]
		if !existed {
			result = append(result, addr)
		}
	}

	return result
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
