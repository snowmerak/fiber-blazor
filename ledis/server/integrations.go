package server

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/redis/go-redis/v9"
	"github.com/redis/rueidis"
	"github.com/snowmerak/fiber-blazor/ledis"
)

// NewGoRedisClient creates a new go-redis client connected to the given Ledis instance.
func NewGoRedisClient(db *ledis.DistributedMap) *redis.Client {
	clientConn, serverConn := net.Pipe()
	handler := NewHandler(db)

	_ = db.WorkerPool.Submit(func() {
		handler.Handle(serverConn)
	})

	return redis.NewClient(&redis.Options{
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return clientConn, nil
		},
	})
}

// NewRueidisClient creates a new ruleidis client connected to the given Ledis instance.
func NewRueidisClient(db *ledis.DistributedMap) (rueidis.Client, error) {
	clientConn, serverConn := net.Pipe()
	handler := NewHandler(db)

	_ = db.WorkerPool.Submit(func() {
		handler.Handle(serverConn)
	})

	return rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{"127.0.0.1:6379"}, // Dummy address
		DialFn: func(s string, d *net.Dialer, c *tls.Config) (net.Conn, error) {
			return clientConn, nil
		},
		DisableCache: true,
	})
}
