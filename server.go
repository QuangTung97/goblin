package goblin

import (
	"context"
	"github.com/QuangTung97/goblin/goblinpb"
	"google.golang.org/grpc"
)

// server is an implementation of GoblinService
type server struct {
	goblinpb.UnimplementedGoblinServiceServer
	pool *PoolServer
}

// Watch watch the changes of membership
func (s *server) Watch(_ *goblinpb.WatchRequest, stream goblinpb.GoblinService_WatchServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	go func() {
		<-s.pool.ctx.Done()
		cancel()
	}()

	ch := make(chan map[string]Node, 8)
	go func() {
		seq, nodes := s.pool.GetNodes()
		ch <- nodes

		for {
			var lastNodes map[string]Node
			seq, nodes = s.pool.WatchNodes(seq)
			if ctx.Err() != nil {
				return
			}
			if nodeMapSame(lastNodes, nodes) {
				continue
			}

			lastNodes = nodes
			ch <- nodes
		}
	}()

	for {
		select {
		case nodes, ok := <-ch:
			if !ok {
				return nil
			}
			err := sendChanges(stream, nodes)
			if err != nil {
				return err
			}

		case <-ctx.Done():
			s.pool.nodeMap.watcherShouldLeave()
			return nil
		}
	}
}

func sendChanges(stream goblinpb.GoblinService_WatchServer, nodes map[string]Node) error {
	output := make([]*goblinpb.Node, 0, len(nodes))
	for name, n := range nodes {
		output = append(output, &goblinpb.Node{
			Name: name,
			Addr: n.Addr,
		})
	}

	err := stream.Send(&goblinpb.NodeList{
		Nodes: output,
	})
	if err != nil {
		return err
	}
	return nil
}

// GetNode for dynamic ips in Kubernetes environment
func (s *server) GetNode(context.Context, *goblinpb.GetNodeRequest) (*goblinpb.GetNodeResponse, error) {
	return &goblinpb.GetNodeResponse{
		Name: s.pool.GetName(),
		Addr: s.pool.GetMemberlistAddress(),
	}, nil
}

// Register to grpc server
func (s *PoolServer) Register(grpcServer *grpc.Server) {
	goblinpb.RegisterGoblinServiceServer(grpcServer, &server{
		pool: s,
	})
}
