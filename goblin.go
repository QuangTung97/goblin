package goblin

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/hashicorp/memberlist"
	"log"
	"time"
)

// Config ...
type Config struct {
	MemberlistBindPort uint16

	IsDynamicIPs bool
	StaticAddrs  []string
	ServiceName  string
}

// PoolServer a service discovery server for client connection pool
type PoolServer struct {
	config  Config
	m       *memberlist.Memberlist
	nodeMap *nodeMap
}

func nodeToAddr(n *memberlist.Node) string {
	return fmt.Sprintf("%v:%d", n.Addr, n.Port)
}

// NewPoolServer creates a PoolServer
func NewPoolServer(config Config) (*PoolServer, error) {
	nodes := newNodeMap()

	mconf := memberlist.DefaultLANConfig()
	mconf.Name = uuid.New().String()
	mconf.BindPort = int(config.MemberlistBindPort)
	mconf.Delegate = &delegate{}
	mconf.Events = &eventDelegate{
		nodes: nodes,
	}

	m, err := memberlist.Create(mconf)
	if err != nil {
		return nil, err
	}

	nodes.updateNode(mconf.Name, nodeToAddr(m.LocalNode()), memberlist.StateAlive)

	s := &PoolServer{
		config:  config,
		m:       m,
		nodeMap: nodes,
	}
	go s.joinIfNetworkPartition()

	return s, nil
}

// GetNodes ...
func (s *PoolServer) GetNodes() (uint64, map[string]Node) {
	return s.nodeMap.getNodes()
}

// WatchNodes ...
func (s *PoolServer) WatchNodes(lastSeq uint64) (uint64, map[string]Node) {
	return s.nodeMap.watchNodes(lastSeq)
}

func (s *PoolServer) joinIfNetworkPartition() {
	seq, nodes := s.nodeMap.getNodes()

	for {
		addrs := getNotJoinedAddresses(nodes, s.config.StaticAddrs)
		if len(addrs) == 0 {
			seq, nodes = s.nodeMap.watchNodes(seq)
			continue
		}

		_, err := s.m.Join(addrs)
		if err != nil {
			log.Println(err)
			time.Sleep(20 * time.Second)
			seq, nodes = s.nodeMap.getNodes()
			continue
		}

		seq, nodes = s.nodeMap.watchNodes(seq)
	}
}
