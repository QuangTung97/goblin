package goblin

import (
	"fmt"
	"github.com/hashicorp/memberlist"
)

type delegate struct {
	nodes      *nodeMap
	broadcasts *memberlist.TransmitLimitedQueue
}

var _ memberlist.Delegate = &delegate{}

func newDelegate(nodes *nodeMap) *delegate {
	return &delegate{
		nodes: nodes,
	}
}

func (d *delegate) NodeMeta(limit int) []byte {
	return nil
}

func (d *delegate) NotifyMsg(msg []byte) {
	name := string(msg)
	fmt.Println(name)

	continued := d.nodes.nodeGracefulLeave(name)
	if continued {
		d.broadcasts.QueueBroadcast(broadcast{
			name: name,
		})
	}
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return d.broadcasts.GetBroadcasts(overhead, limit)
}

func (d *delegate) LocalState(join bool) []byte {
	// TODO local state
	return nil
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	// TODO merge
}

type eventDelegate struct {
	nodes *nodeMap
}

var _ memberlist.EventDelegate = &eventDelegate{}

func newEventDelegate(nodes *nodeMap) *eventDelegate {
	return &eventDelegate{
		nodes: nodes,
	}
}

func nodeToAddr(n *memberlist.Node) string {
	return fmt.Sprintf("%v:%d", n.Addr, n.Port)
}

func (d *eventDelegate) NotifyJoin(n *memberlist.Node) {
	d.nodes.nodeJoin(n.Name, nodeToAddr(n))
}

func (d *eventDelegate) NotifyLeave(n *memberlist.Node) {
	d.nodes.nodeLeave(n.Name)
}

func (d *eventDelegate) NotifyUpdate(n *memberlist.Node) {
}

var _ memberlist.EventDelegate = &eventDelegate{}

type broadcast struct {
	name string
}

var _ memberlist.Broadcast = broadcast{}

func (b broadcast) Invalidates(other memberlist.Broadcast) bool {
	return false
}

func (b broadcast) Message() []byte {
	return []byte(b.name)
}

func (b broadcast) Finished() {
}
