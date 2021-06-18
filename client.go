package goblin

import (
	"errors"
	"google.golang.org/grpc"
	"sync/atomic"
	"unsafe"
)

// ErrNoConn when pool has not (or not yet) any connections
var ErrNoConn = errors.New("no connection available")

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
	return makePoolClient(config)
}

func makePoolClient(config ClientConfig) *PoolClient {
	return &PoolClient{
		config: config,
	}
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

// GetConn get a connection from pool, DO *NOT* use conn outside the lifetime of current goroutine
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
