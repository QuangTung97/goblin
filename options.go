package goblin

import (
	"go.uber.org/zap"
	"time"
)

type serverOptions struct {
	portDiff           uint16
	leftNodeExpireTime time.Duration
	joinRetryTime      time.Duration
	logger             *zap.Logger
}

func defaultServerOptions() serverOptions {
	return serverOptions{
		portDiff:           2000,
		leftNodeExpireTime: 30 * time.Second,
		joinRetryTime:      30 * time.Second,
		logger:             zap.NewNop(),
	}
}

// ServerOption ...
type ServerOption func(opts *serverOptions)

func computeOptions(opts ...ServerOption) serverOptions {
	result := defaultServerOptions()
	for _, o := range opts {
		o(&result)
	}
	return result
}

// WithServerLogger configures the logger for PoolServer
func WithServerLogger(logger *zap.Logger) ServerOption {
	return func(opts *serverOptions) {
		opts.logger = logger
	}
}
