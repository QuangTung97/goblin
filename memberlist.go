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
	if continued {
		d.broadcasts.QueueBroadcast(b)
	}
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return d.broadcasts.GetBroadcasts(overhead, limit)
}

func computeLeftNodesState(nodes *nodeMap) []byte {
	leftNodes := nodes.getLeftNodes()

	result := make([]string, 0, len(leftNodes))
	for name, node := range leftNodes {
		b := marshalBroadcast(broadcast{
			name: name,
			addr: node.addr,
		})
		result = append(result, string(b))
	}
	return []byte(strings.Join(result, ","))
}

func (d *delegate) LocalState(bool) []byte {
	return computeLeftNodesState(d.nodes)
}

func remoteStateToBroadcast(s []byte) []broadcast {
	if len(s) == 0 {
		return nil
	}
	list := strings.Split(string(s), ",")
	result := make([]broadcast, 0, len(list))
	for _, data := range list {
		b, ok := unmarshalBroadcast([]byte(data))
		if !ok {
			return nil
		}
		result = append(result, b)
	}
	return result
}

func (d *delegate) MergeRemoteState(buf []byte, _ bool) {
	list := remoteStateToBroadcast(buf)
	for _, b := range list {
		continued := d.nodes.nodeGracefulLeave(b.name, b.addr)
		if continued {
			d.broadcasts.QueueBroadcast(b)
		}
	}
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

var _ memberlist.NamedBroadcast = broadcast{}

func (b broadcast) Invalidates(other memberlist.Broadcast) bool {
	nb, ok := other.(memberlist.NamedBroadcast)
	if !ok {
		return false
	}
	return b.Name() == nb.Name()
}

func (b broadcast) Message() []byte {
	return marshalBroadcast(b)
}

func (b broadcast) Finished() {
}

func (b broadcast) Name() string {
	return b.name
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
