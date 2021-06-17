package goblin

import (
	"fmt"
	"github.com/hashicorp/memberlist"
	"strings"
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
	b, ok := unmarshalBroadcast(msg)
	if !ok {
		return
	}

	continued := d.nodes.nodeGracefulLeave(b.name, b.addr)
	fmt.Println("NodeGracefulLeave", b.name, continued)
	if continued {
		d.broadcasts.QueueBroadcast(b)
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

func (d *eventDelegate) NotifyUpdate(*memberlist.Node) {
}

var _ memberlist.EventDelegate = &eventDelegate{}

type broadcast struct {
	name string
	addr string
}

var _ memberlist.Broadcast = broadcast{}

func (b broadcast) Invalidates(other memberlist.Broadcast) bool {
	return false
}

func (b broadcast) Message() []byte {
	return marshalBroadcast(b)
}

func (b broadcast) Finished() {
}

func marshalBroadcast(b broadcast) []byte {
	return []byte(b.name + "@" + b.addr)
}

func unmarshalBroadcast(msg []byte) (broadcast, bool) {
	values := strings.Split(string(msg), "@")
	if len(values) < 2 {
		return broadcast{}, false
	}

	name := values[0]
	addr := values[1]
	return broadcast{
		name: name,
		addr: addr,
	}, true
}
