package goblin

import (
	"context"
	"errors"
	"fmt"
	"github.com/QuangTung97/goblin/goblinpb"
	"github.com/google/uuid"
	"github.com/hashicorp/memberlist"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"strconv"
	"strings"
	"time"
)

// ServerConfig ...
type ServerConfig struct {
	GRPCPort uint16

	IsDynamicIPs bool
	StaticAddrs  []string
	ServiceAddr  string
	DialOptions  []grpc.DialOption // for rpc get a node address using ServiceAddr
}

// PoolServer a service discovery server for client connection pool
type PoolServer struct {
	config  ServerConfig
	options serverOptions
	name    string

	getJoinAddrs func() []string

	m          *memberlist.Memberlist
	broadcasts *memberlist.TransmitLimitedQueue

	nodeMap *nodeMap
	ctx     context.Context
	cancel  func()
}

func getStaticJoinAddrs(config ServerConfig, portDiff uint16) func() []string {
	joinAddrs := make([]string, 0, len(config.StaticAddrs))
	for _, addr := range config.StaticAddrs {
		ip, port, err := getStaticIPAndPort(addr)
		if err != nil {
			panic(err)
		}
		joinAddrs = append(joinAddrs, fmt.Sprintf("%s:%d", ip, port+portDiff))
	}

	return func() []string {
		return joinAddrs
	}
}

func getDynamicJoinAddrs(config ServerConfig, logger *zap.Logger) func() []string {
	return func() []string {
		conn, err := grpc.Dial(config.ServiceAddr, config.DialOptions...)
		if err != nil {
			logger.Error("getDynamicJoinAddrs Dial", zap.Error(err))
			return nil
		}
		defer func() {
			_ = conn.Close()
		}()

		client := goblinpb.NewGoblinServiceClient(conn)
		resp, err := client.GetNode(context.Background(), &goblinpb.GetNodeRequest{})
		if err != nil {
			logger.Warn("client.GetNode", zap.Error(err))
			return nil
		}
		return []string{resp.Addr}
	}
}

// NewPoolServer creates a PoolServer
func NewPoolServer(config ServerConfig, opts ...ServerOption) (*PoolServer, error) {
	err := validateServerConfig(config)
	if err != nil {
		return nil, err
	}

	options := computeServerOptions(opts...)

	nodes := newNodeMap(options.leftNodeExpireTime)
	name := uuid.New().String()

	mconf := memberlist.DefaultLANConfig()
	mconf.Name = name
	mconf.BindPort = int(config.GRPCPort + options.portDiff)

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

	getJoinAddrs := getStaticJoinAddrs(config, options.portDiff)
	if config.IsDynamicIPs {
		getJoinAddrs = getDynamicJoinAddrs(config, options.logger)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &PoolServer{
		config:  config,
		options: options,
		name:    name,

		getJoinAddrs: getJoinAddrs,

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

// GetName returns the name of current node
func (s *PoolServer) GetName() string {
	return s.name
}

// GetMemberlistAddress returns the address for list of memberlist
func (s *PoolServer) GetMemberlistAddress() string {
	return s.m.LocalNode().Address()
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
		seq, addrs = s.nodeMap.getNotJoinedAddresses(s.getJoinAddrs())
		if len(addrs) == 0 {
			seq, _ = s.nodeMap.watchNodes(seq)
			continue
		}

		_, err := s.m.Join(addrs)
		if err != nil {
			s.options.logger.Error("Join error", zap.Error(err))
			time.Sleep(s.options.joinRetryTime)
			continue
		}

		seq, _ = s.nodeMap.watchNodes(seq)
	}
}

func getStaticIPAndPort(addr string) (string, uint16, error) {
	list := strings.Split(addr, ":")
	if len(list) < 2 {
		return "", 0, errors.New("invalid static address")
	}

	port, err := strconv.Atoi(list[1])
	if err != nil {
		return "", 0, errors.New("invalid static address")
	}

	return list[0], uint16(port), nil
}

func validateServerConfig(conf ServerConfig) error {
	if conf.GRPCPort == 0 {
		return errors.New("empty GRPCPort in ServerConfig")
	}

	if conf.IsDynamicIPs {
		if len(conf.ServiceAddr) == 0 {
			return errors.New("empty ServiceAddr when IsDynamicIPs is true")
		}
	} else {
		if len(conf.StaticAddrs) == 0 {
			return errors.New("empty StaticAddrs when IsDynamicIPs is false")
		}

		for _, addr := range conf.StaticAddrs {
			_, _, err := getStaticIPAndPort(addr)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
