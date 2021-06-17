package goblin

import (
	"context"
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
	config Config
	name   string

	m          *memberlist.Memberlist
	broadcasts *memberlist.TransmitLimitedQueue

	nodeMap *nodeMap
	ctx     context.Context
	cancel  func()
}

// NewPoolServer creates a PoolServer
func NewPoolServer(config Config) (*PoolServer, error) {
	nodes := newNodeMap(30 * time.Second)

	name := uuid.New().String()

	mconf := memberlist.DefaultLANConfig()
	mconf.Name = name
	mconf.BindPort = int(config.MemberlistBindPort)

	d := newDelegate(nodes)
	mconf.Delegate = d
	mconf.Events = newEventDelegate(nodes)

	m, err := memberlist.Create(mconf)
	if err != nil {
		return nil, err
	}

	broadcasts := &memberlist.TransmitLimitedQueue{
		RetransmitMult: mconf.RetransmitMult,
		NumNodes: func() int {
			return m.NumMembers()
		},
	}

	d.broadcasts = broadcasts

	ctx, cancel := context.WithCancel(context.Background())
	s := &PoolServer{
		config:     config,
		name:       name,
		m:          m,
		broadcasts: broadcasts,
		nodeMap:    nodes,
		ctx:        ctx,
		cancel:     cancel,
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

// Shutdown ...
func (s *PoolServer) Shutdown() error {
	addr := nodeToAddr(s.m.LocalNode())
	s.nodeMap.nodeGracefulLeave(s.name, addr)
	s.broadcasts.QueueBroadcast(broadcast{
		name: s.name,
		addr: addr,
	})

	s.cancel()
	err := s.m.Leave(0)
	if err != nil {
		return err
	}

	return s.m.Shutdown()
}

func (s *PoolServer) joinIfNetworkPartition() {
	seq, _ := s.nodeMap.getNodes()

	for {
		if s.ctx.Err() != nil {
			return
		}

		var addrs []string
		seq, addrs = s.nodeMap.getNotJoinedAddresses(s.config.StaticAddrs)
		if len(addrs) == 0 {
			seq, _ = s.nodeMap.watchNodes(seq)
			continue
		}

		_, err := s.m.Join(addrs)
		if err != nil {
			log.Println("[ERROR] Join error:", err)
			time.Sleep(30 * time.Second)
			continue
		}

		seq, _ = s.nodeMap.watchNodes(seq)
	}
}
