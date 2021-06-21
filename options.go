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

func computeServerOptions(opts ...ServerOption) serverOptions {
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

//================================================================

type clientOptions struct {
	portDiff   uint16
	watchRetry time.Duration
	logger     *zap.Logger
}

func defaultClientOptions() clientOptions {
	return clientOptions{
		portDiff:   2000,
		watchRetry: 60 * time.Second,
		logger:     zap.NewNop(),
	}
}

// ClientOption ...
type ClientOption func(opts *clientOptions)

func computeClientOptions(opts ...ClientOption) clientOptions {
	result := defaultClientOptions()
	for _, o := range opts {
		o(&result)
	}
	return result
}

// WithClientLogger configures the logger for PoolClient
func WithClientLogger(logger *zap.Logger) ClientOption {
	return func(opts *clientOptions) {
		opts.logger = logger
	}
}

// WithWatchRetryDuration configures watch nodes retry duration
func WithWatchRetryDuration(d time.Duration) ClientOption {
	return func(opts *clientOptions) {
		opts.watchRetry = d
	}
}
