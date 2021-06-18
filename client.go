package goblin

import (
	"google.golang.org/grpc"
	"sync/atomic"
	"unsafe"
)

// ClientConfig for config client pooling
type ClientConfig struct {
	Addresses []string
	Options   []grpc.CallOption
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
	return &PoolClient{
		config: config,
	}
}

// GetConn get a connection from pool, DO *NOT* use conn outside the lifetime of current goroutine
func (c *PoolClient) GetConn(fn func(conn *grpc.ClientConn)) {
}

func (c *PoolClient) getClientConns() *clientConns {
	return (*clientConns)(atomic.LoadPointer(&c.conns))
}

func (c *PoolClient) setClientConns(conns *clientConns) {
	atomic.StorePointer(&c.conns, unsafe.Pointer(conns))
}

func cloneClientConns(conns *clientConns) *clientConns {
	result := &clientConns{}

	result.conns = make([]*clientConn, 0, len(conns.conns))
	for _, c := range conns.conns {
		result.conns = append(result.conns, c)
	}

	return result
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
