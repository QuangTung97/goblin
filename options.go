package goblin

import (
	"github.com/hashicorp/memberlist"
	"go.uber.org/zap"
	"time"
)

type serverOptions struct {
	portDiff           uint16
	leftNodeExpireTime time.Duration
	joinRetryTime      time.Duration
	logger             *zap.Logger
	memberlistConf     func(conf *memberlist.Config)
}

func defaultServerOptions() serverOptions {
	return serverOptions{
		portDiff:           2000,
		leftNodeExpireTime: 30 * time.Second,
		joinRetryTime:      30 * time.Second,
		logger:             zap.NewNop(),
		memberlistConf:     func(conf *memberlist.Config) {},
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

// WithServerMemberlistConfig for customizing memberlist (default use LAN Config)
func WithServerMemberlistConfig(fn func(config *memberlist.Config)) ServerOption {
	return func(opts *serverOptions) {
		opts.memberlistConf = fn
	}
}

// WithServerPortDiff configures the port difference between gRPC and memberlist ports
func WithServerPortDiff(diff uint16) ServerOption {
	return func(opts *serverOptions) {
		opts.portDiff = diff
	}
}

// WithJoinRetryDuration configures the retry duration for join group
func WithJoinRetryDuration(d time.Duration) ServerOption {
	return func(opts *serverOptions) {
		opts.joinRetryTime = d
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

// WithClientPortDiff configures the port difference between gRPC and memberlist ports
func WithClientPortDiff(diff uint16) ClientOption {
	return func(opts *clientOptions) {
		opts.portDiff = diff
	}
}
