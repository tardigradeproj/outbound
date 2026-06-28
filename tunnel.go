// Copyright IBM Corp. 2014, 2025
// SPDX-License-Identifier: MPL-2.0

package outbound

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
)

// DialFunc is how an upstream is reached.
type DialFunc func(ctx context.Context) (net.Conn, error)

// Upstream is the registration record for a local service.
type Upstream struct {
	Id   uint8
	Name string
	Dial DialFunc
}

// TCPUpstream returns a DialFunc that dials a plain TCP address.
func TCPUpstream(addr string) DialFunc {
	return func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	}
}

// Tunnel wraps a outbound session and routes streams to local upstreams.
type Tunnel struct {
	session   *Session
	mu        sync.RWMutex
	upstreams map[uint8]Upstream
}

// NewTunnel creates a Tunnel around an existing outbound session.
func NewTunnel(session *Session) *Tunnel {
	return &Tunnel{
		session:   session,
		upstreams: make(map[uint8]Upstream),
	}
}

// Register adds a local upstream reachable by remote dialers.
// ID must be unique within this tunnel.
func (t *Tunnel) Register(u Upstream) {
	t.mu.Lock()
	t.upstreams[u.Id] = u
	t.mu.Unlock()
}

// Unregister removes a local upstream by ID.
func (t *Tunnel) Unregister(id uint8) {
	t.mu.Lock()
	delete(t.upstreams, id)
	t.mu.Unlock()
}

// Dial opens a stream to an upstream on the remote side identified by its ID.
// Returns a net.Conn that is a transparent pipe to that upstream.
func (t *Tunnel) Dial(ctx context.Context, upstreamID uint8) (net.Conn, error) {
	stream, err := t.session.OpenStream(upstreamID)
	if err != nil {
		return nil, fmt.Errorf("outbound: open stream: %w", err)
	}
	return stream, nil
}

// Serve accepts incoming streams and routes them to local upstreams.
// Blocks until the session closes or ctx is cancelled.
func (t *Tunnel) Serve(ctx context.Context) error {
	for {
		stream, err := t.session.AcceptStreamWithContext(ctx)
		if err != nil {
			return err
		}
		go t.handleStream(ctx, stream)
	}
}

// Close closes the tunnel and the underlying session.
func (t *Tunnel) Close() error {
	return t.session.Close()
}

func (t *Tunnel) handleStream(ctx context.Context, stream *Stream) {
	defer func() { _ = stream.Close() }()

	t.mu.RLock()
	upstream, ok := t.upstreams[stream.UpstreamID()]
	t.mu.RUnlock()
	if !ok {
		return
	}

	conn, err := upstream.Dial(ctx)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()

	tunnelPipe(stream, conn)
}

// tunnelPipe copies bidirectionally between a and b.
// It returns when both directions are done.
func tunnelPipe(a, b net.Conn) {
	done := make(chan struct{}, 2)
	cp := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		// Close both ends so the other direction unblocks.
		_ = dst.Close()
		_ = src.Close()
		done <- struct{}{}
	}
	go cp(a, b)
	go cp(b, a)
	<-done
	<-done
}
