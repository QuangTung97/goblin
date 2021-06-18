package goblin

import (
	"context"
	"errors"
	"fmt"
	"github.com/QuangTung97/goblin/goblinpb"
	"google.golang.org/grpc"
	"io"
	"log"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"
)

// ErrNoConn when pool has not (or not yet) any connections
var ErrNoConn = errors.New("no connection available")

// ClientConfig for config client pooling
type ClientConfig struct {
	Addresses []string
	Options   []grpc.DialOption
}

type clientConn struct {
	conn     *grpc.ClientConn
	nodeName string
	refCount uint64   // reference count, for closing connection if no one refer to
	_padding [32]byte // to avoid false sharing (64 byte cache line)
}

type clientConns struct {
	conns []*clientConn
}

// PoolClient for client pooling
type PoolClient struct {
	conns  unsafe.Pointer // pointer to clientConns, use unsafe.Pointer for Read-Copy-Update
	seq    uint64
	config ClientConfig
}

// NewPoolClient ...
func NewPoolClient(config ClientConfig) *PoolClient {
	client := makePoolClient(config)
	go client.watchNodes()
	return client
}

func makePoolClient(config ClientConfig) *PoolClient {
	return &PoolClient{
		config: config,
	}
}

func (c *PoolClient) watchNodesSingleLoop(addr string) {
	conn, err := grpc.Dial(addr, c.config.Options...)
	if err != nil {
		log.Println("[ERROR] dial for watch nodes error:", err)
		time.Sleep(60 * time.Second)
		return
	}
	defer func() {
		_ = conn.Close()
	}()

	client := goblinpb.NewGoblinServiceClient(conn)
	stream, err := client.Watch(context.Background(), &goblinpb.WatchRequest{})
	if err != nil {
		log.Println("[ERROR] watch nodes error:", err)
		time.Sleep(60 * time.Second)
		return
	}

	for {
		nodeList, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Println("[ERROR] receive nodes error:", err)
			time.Sleep(60 * time.Second)
			return
		}

		c.handleNewNodeList(nodeList.Nodes)
	}
}

func (c *PoolClient) watchNodes() {
	index := 0
	for ; ; index = (index + 1) % len(c.config.Addresses) {
		addr := c.config.Addresses[index]
		c.watchNodesSingleLoop(addr)
	}
}

func (c *PoolClient) handleNewNodeList(nodes []*goblinpb.Node) {
	newClientConns := computeNewClientConns(c.getClientConns(), nodes, 2000, func(addr string) *grpc.ClientConn {
		conn, err := grpc.Dial(addr, c.config.Options...)
		if err != nil {
			panic(err)
		}
		return conn
	})
	c.setClientConns(newClientConns)
}

func doRequestConn(conn *clientConn, fn func(conn *grpc.ClientConn) error) error {
	defer func() {
		needClose := conn.release()
		if needClose {
			_ = conn.conn.Close()
		}
	}()
	return fn(conn.conn)
}

// GetConn get a connection from pool, DO *NOT* use conn outside the lifetime of current function
func (c *PoolClient) GetConn(fn func(conn *grpc.ClientConn) error) error {
	for {
		conn, ok := c.getNextConn()
		if !ok {
			return ErrNoConn
		}

		ok = conn.acquire()
		if !ok {
			continue
		}
		return doRequestConn(conn, fn)
	}
}

func (c *PoolClient) getClientConns() *clientConns {
	return (*clientConns)(atomic.LoadPointer(&c.conns))
}

func (c *PoolClient) setClientConns(conns *clientConns) {
	atomic.StorePointer(&c.conns, unsafe.Pointer(conns))
}

func (c *clientConn) acquire() (ok bool) {
	for {
		count := atomic.LoadUint64(&c.refCount)
		if count == 0 {
			return false
		}
		swapped := atomic.CompareAndSwapUint64(&c.refCount, count, count+1)
		if swapped {
			return true
		}
	}
}

func (c *clientConn) release() (needClose bool) {
	newVal := atomic.AddUint64(&c.refCount, ^uint64(0))
	return newVal == 0
}

func (c *PoolClient) getNextConn() (*clientConn, bool) {
	newVal := atomic.AddUint64(&c.seq, 1)

	tmp := c.getClientConns()
	if tmp == nil {
		return nil, false
	}

	conns := tmp.conns
	if len(conns) == 0 {
		return nil, false
	}

	index := (newVal - 1) % uint64(len(conns))
	return conns[index], true
}

func getGRPCAddrFromMemberlist(addr string, portDiff int) string {
	list := strings.Split(addr, ":")
	if len(list) < 2 {
		panic("invalid address")
	}
	port, err := strconv.Atoi(list[1])
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%s:%d", list[0], port-portDiff)
}

func computeNewClientConns(
	old *clientConns, nodes []*goblinpb.Node, portDiff int,
	dial func(addr string) *grpc.ClientConn,
) *clientConns {
	oldNameSet := map[string]struct{}{}
	for _, conn := range old.conns {
		oldNameSet[conn.nodeName] = struct{}{}
	}

	newNameSet := map[string]struct{}{}
	for _, node := range nodes {
		newNameSet[node.Name] = struct{}{}
	}

	result := &clientConns{}
	result.conns = make([]*clientConn, 0, len(old.conns))
	for _, conn := range old.conns {
		_, existed := newNameSet[conn.nodeName]
		if !existed {
			needClose := conn.release()
			if needClose {
				_ = conn.conn.Close()
			}
			continue
		}

		result.conns = append(result.conns, conn)
	}

	for _, node := range nodes {
		_, existed := oldNameSet[node.Name]
		if existed {
			continue
		}

		result.conns = append(result.conns, &clientConn{
			conn:     dial(getGRPCAddrFromMemberlist(node.Addr, portDiff)),
			nodeName: node.Name,
			refCount: 1,
		})
	}

	return result
}
