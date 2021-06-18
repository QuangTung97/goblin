package goblin

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"unsafe"
)

func TestValidateSizeOfClientConn(t *testing.T) {
	assert.Equal(t, uintptr(64), unsafe.Sizeof(clientConn{}))
	assert.Equal(t, uintptr(8), unsafe.Alignof(clientConn{}))
}

func TestCloneClientConns(t *testing.T) {
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
	result := cloneClientConns(conns)
	if result == conns {
		t.Error("should not be equal")
	}
	assert.Equal(t, conns, result)
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
