package goblin

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetStaticIPAndPort(t *testing.T) {
	t.Run("no-port", func(t *testing.T) {
		ip, port, err := getStaticIPAndPort("address-1")
		assert.Equal(t, errors.New("invalid static address"), err)
		assert.Equal(t, "", ip)
		assert.Equal(t, uint16(0), port)

	})

	t.Run("invalid-port", func(t *testing.T) {
		ip, port, err := getStaticIPAndPort("address-1:sample")
		assert.Equal(t, errors.New("invalid static address"), err)
		assert.Equal(t, "", ip)
		assert.Equal(t, uint16(0), port)
	})

	t.Run("normal", func(t *testing.T) {
		ip, port, err := getStaticIPAndPort("address-1:5000")
		assert.Equal(t, nil, err)
		assert.Equal(t, "address-1", ip)
		assert.Equal(t, uint16(5000), port)
	})
}

func TestValidateServerConfig(t *testing.T) {
	table := []struct {
		name string
		conf ServerConfig
		err  error
	}{
		{
			name: "empty-port",
			err:  errors.New("empty GRPCPort in ServerConfig"),
		},
		{
			name: "invalid-static-addr",
			conf: ServerConfig{
				GRPCPort: 4001,
				StaticAddrs: []string{
					"address-1",
				},
			},
			err: errors.New("invalid static address"),
		},
		{
			name: "service-addr-empty-when-dynamic",
			conf: ServerConfig{
				GRPCPort:     4001,
				IsDynamicIPs: true,
			},
			err: errors.New("empty ServiceAddr when IsDynamicIPs is true"),
		},
		{
			name: "normal-static",
			conf: ServerConfig{
				GRPCPort:     4001,
				IsDynamicIPs: false,
				StaticAddrs: []string{
					"address-1:4001",
				},
			},
			err: nil,
		},
		{
			name: "normal-dynamic",
			conf: ServerConfig{
				GRPCPort:     4001,
				IsDynamicIPs: true,
				ServiceAddr:  "service-name:4001",
			},
			err: nil,
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			err := validateServerConfig(e.conf)
			assert.Equal(t, e.err, err)
		})
	}
}

func TestGetStaticJoinAddrs(t *testing.T) {
	addrs := getStaticJoinAddrs(ServerConfig{
		StaticAddrs: []string{
			"address-1:8001",
			"address-1:8002",
		},
	}, 2000)()
	assert.Equal(t, []string{
		"address-1:10001",
		"address-1:10002",
	}, addrs)
}
