package goblin

import "github.com/hashicorp/memberlist"

type delegate struct {
}

var _ memberlist.Delegate = &delegate{}

func (d *delegate) NodeMeta(limit int) []byte {
	return nil
}

func (d *delegate) NotifyMsg([]byte) {
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return nil
}

func (d *delegate) LocalState(join bool) []byte {
	return nil
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {
}

type eventDelegate struct {
	nodes *nodeMap
}

func (d *eventDelegate) NotifyJoin(n *memberlist.Node) {
	d.nodes.updateNode(n.Name, nodeToAddr(n), n.State)
}

func (d *eventDelegate) NotifyLeave(n *memberlist.Node) {
	d.nodes.updateNode(n.Name, nodeToAddr(n), n.State)
}

func (d *eventDelegate) NotifyUpdate(n *memberlist.Node) {
	d.nodes.updateNode(n.Name, nodeToAddr(n), n.State)
}

var _ memberlist.EventDelegate = &eventDelegate{}
