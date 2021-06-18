package goblin

import (
	"github.com/QuangTung97/goblin/goblinpb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"testing"
	"unsafe"
)

func TestValidateSizeOfClientConn(t *testing.T) {
	assert.Equal(t, uintptr(64), unsafe.Sizeof(clientConn{}))
	assert.Equal(t, uintptr(8), unsafe.Alignof(clientConn{}))
}

func TestClientConnAcquireRelease(t *testing.T) {
	c := &clientConn{
		refCount: 2,
	}

	ok := c.acquire()
	assert.Equal(t, true, ok)
	assert.Equal(t, uint64(3), c.refCount)

	needClose := c.release()
	assert.Equal(t, false, needClose)
	assert.Equal(t, uint64(2), c.refCount)

	needClose = c.release()
	assert.Equal(t, false, needClose)
	assert.Equal(t, uint64(1), c.refCount)

	needClose = c.release()
	assert.Equal(t, true, needClose)
	assert.Equal(t, uint64(0), c.refCount)

	ok = c.acquire()
	assert.Equal(t, false, ok)
	assert.Equal(t, uint64(0), c.refCount)
}

func TestPoolClient_GetNextConn(t *testing.T) {
	conns := &clientConns{
		conns: []*clientConn{
			{
				nodeName: "node-1",
				refCount: 10,
			},
			{
				nodeName: "node-2",
				refCount: 20,
			},
		},
	}
	pool := makePoolClient(ClientConfig{})
	pool.setClientConns(conns)

	result, ok := pool.getNextConn()
	assert.Equal(t, true, ok)
	assert.Equal(t, &clientConn{
		nodeName: "node-1",
		refCount: 10,
	}, result)
	assert.Equal(t, uint64(1), pool.seq)

	result, ok = pool.getNextConn()
	assert.Equal(t, true, ok)
	assert.Equal(t, &clientConn{
		nodeName: "node-2",
		refCount: 20,
	}, result)
	assert.Equal(t, uint64(2), pool.seq)

	result, ok = pool.getNextConn()
	assert.Equal(t, true, ok)
	assert.Equal(t, &clientConn{
		nodeName: "node-1",
		refCount: 10,
	}, result)
	assert.Equal(t, uint64(3), pool.seq)
}

func TestPoolClient_GetNextConn_No_Conns(t *testing.T) {
	pool := makePoolClient(ClientConfig{})
	result, ok := pool.getNextConn()
	assert.Equal(t, false, ok)
	assert.Equal(t, (*clientConn)(nil), result)
}

func TestPoolClient_GetNextConn_Conns_Empty(t *testing.T) {
	conns := &clientConns{}
	pool := makePoolClient(ClientConfig{})
	pool.setClientConns(conns)

	result, ok := pool.getNextConn()
	assert.Equal(t, false, ok)
	assert.Equal(t, (*clientConn)(nil), result)
}

func TestGetGRPCAddrFromMemberlistAddr(t *testing.T) {
	t.Run("not-contains-colon", func(t *testing.T) {
		assert.PanicsWithValue(t, "invalid address", func() {
			getGRPCAddrFromMemberlist("some-host-1", 200)
		})
	})

	t.Run("not-number", func(t *testing.T) {
		assert.Panics(t, func() {
			getGRPCAddrFromMemberlist("some-host-1:string", 200)
		})
	})

	t.Run("normal", func(t *testing.T) {
		result := getGRPCAddrFromMemberlist("some-host-1:5800", 200)
		assert.Equal(t, "some-host-1:5600", result)
	})
}

func TestComputeNewClientConns(t *testing.T) {
	conn1 := &clientConn{
		nodeName: "name-1",
		refCount: 10,
	}
	conn2 := &clientConn{
		nodeName: "name-2",
		refCount: 20,
	}

	old := &clientConns{
		conns: []*clientConn{
			conn1,
			conn2,
		},
	}
	nodes := []*goblinpb.Node{
		{
			Name: "name-1",
			Addr: "some-host-1:5800",
		},
		{
			Name: "name-3",
			Addr: "some-host-3:5800",
		},
		{
			Name: "name-4",
			Addr: "some-host-4:5800",
		},
	}

	var dialAddr string
	var dialCount int
	result := computeNewClientConns(old, nodes, 200, func(addr string) *grpc.ClientConn {
		dialCount++
		dialAddr = addr
		return nil
	})

	assert.NotNil(t, result)
	if result == old {
		t.Error("must not be same")
	}
	assert.Equal(t, 2, dialCount)
	assert.Equal(t, "some-host-4:5600", dialAddr)
	assert.Equal(t, []*clientConn{
		conn1,
		{
			nodeName: "name-3",
			refCount: 1,
		},
		{
			nodeName: "name-4",
			refCount: 1,
		},
	}, result.conns)

	assert.Equal(t, uint64(19), conn2.refCount)
}

func TestComputeNewClientConns_Old_Conns_Nil(t *testing.T) {
	nodes := []*goblinpb.Node{
		{
			Name: "name-1",
			Addr: "some-host-1:5800",
		},
		{
			Name: "name-3",
			Addr: "some-host-3:5800",
		},
		{
			Name: "name-4",
			Addr: "some-host-4:5800",
		},
	}

	var dialAddr string
	var dialCount int
	result := computeNewClientConns(nil, nodes, 200, func(addr string) *grpc.ClientConn {
		dialCount++
		dialAddr = addr
		return nil
	})

	assert.Equal(t, 3, dialCount)
	assert.Equal(t, "some-host-4:5600", dialAddr)
	assert.Equal(t, []*clientConn{
		{
			nodeName: "name-1",
			refCount: 1,
		},
		{
			nodeName: "name-3",
			refCount: 1,
		},
		{
			nodeName: "name-4",
			refCount: 1,
		},
	}, result.conns)
}
