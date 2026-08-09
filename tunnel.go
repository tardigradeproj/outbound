// Copyright IBM Corp. 2014, 2025
// SPDX-License-Identifier: MPL-2.0

package outbound

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
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

// Registry holds a shared set of upstreams, safe for concurrent use.
// A single Registry can be shared across multiple Tunnels so upstreams
// only need to be registered once regardless of how many connections exist.
type Registry struct {
	mu        sync.RWMutex
	upstreams map[uint8]Upstream
}

// NewRegistry creates an empty Registry.
func NewRegistry() *Registry {
	return &Registry{upstreams: make(map[uint8]Upstream)}
}

// Register adds a local upstream reachable by remote dialers.
// ID must be unique within this registry.
func (r *Registry) Register(u Upstream) {
	r.mu.Lock()
	r.upstreams[u.Id] = u
	r.mu.Unlock()
}

// Unregister removes a local upstream by ID.
func (r *Registry) Unregister(id uint8) {
	r.mu.Lock()
	delete(r.upstreams, id)
	r.mu.Unlock()
}

func (r *Registry) lookup(id uint8) (Upstream, bool) {
	r.mu.RLock()
	u, ok := r.upstreams[id]
	r.mu.RUnlock()
	return u, ok
}

// Tunnel wraps an outbound session and routes streams to upstreams in a Registry.
type Tunnel struct {
	session  *Session
	registry *Registry
}

// NewTunnel creates a Tunnel around an existing outbound session.
// The registry is shared and must not be nil.
func NewTunnel(session *Session, registry *Registry) *Tunnel {
	return &Tunnel{session: session, registry: registry}
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

// IsClosed reports whether the underlying session has been closed.
func (t *Tunnel) IsClosed() bool {
	return t.session.IsClosed()
}

// CloseChan returns a channel that is closed when the underlying session closes.
func (t *Tunnel) CloseChan() <-chan struct{} {
	return t.session.CloseChan()
}

// NumStreams returns the number of currently open streams.
func (t *Tunnel) NumStreams() int {
	return t.session.NumStreams()
}

// Ping measures the round-trip time to the remote end.
func (t *Tunnel) Ping() (time.Duration, error) {
	return t.session.Ping()
}

// GoAway signals the remote end to stop opening new streams.
func (t *Tunnel) GoAway() error {
	return t.session.GoAway()
}

// LocalAddr returns the local address of the underlying connection.
func (t *Tunnel) LocalAddr() net.Addr {
	return t.session.LocalAddr()
}

// RemoteAddr returns the remote address of the underlying connection.
func (t *Tunnel) RemoteAddr() net.Addr {
	return t.session.RemoteAddr()
}

func (t *Tunnel) handleStream(ctx context.Context, stream *Stream) {
	defer func() { _ = stream.Close() }()

	upstream, ok := t.registry.lookup(stream.UpstreamID())
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

// copyBufferPool holds reusable scratch buffers for tunnelPipe, avoiding a
// fresh allocation per direction on every proxied connection.
var copyBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 32*1024)
		return &buf
	},
}

// onlyReader/onlyWriter strip any optional interfaces (like io.WriterTo or
// io.ReaderFrom) from the wrapped value. net.TCPConn implements both, and
// io.CopyBuffer silently was ignoring the buffer it's given in favor of those
// fast paths, which then allocate their own scratch buffer internally since
// the peer here is a *Stream, not a raw file descriptor eligible for true
// zero-copy using splice/sendfile system calls. Hiding the interfaces forces the pooled buffer below to
// actually be used.
type onlyReader struct{ io.Reader }
type onlyWriter struct{ io.Writer }

// tunnelPipe copies bidirectionally between a and b.
// It returns when both directions are done.
func tunnelPipe(a, b net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)
	pipe := func(dst, src net.Conn) {
		defer wg.Done()
		bufp := copyBufferPool.Get().(*[]byte)
		defer copyBufferPool.Put(bufp)
		_, _ = io.CopyBuffer(onlyWriter{dst}, onlyReader{src}, *bufp)
		// Half-close the write side of dst so it knows we are done
		// sending, but leave src open so the other direction can still
		// drain (e.g. the upstream response arriving after the request
		// is fully sent).
		closeWrite(dst)
	}
	go pipe(b, a)
	go pipe(a, b)
	wg.Wait()
	_ = a.Close()
	_ = b.Close()
}

// closeWrite signals EOF on the write side of c without fully closing it.
// Falls back to Close for connections that do not support half-close.
func closeWrite(c net.Conn) {
	type halfCloser interface {
		CloseWrite() error
	}
	if hc, ok := c.(halfCloser); ok {
		_ = hc.CloseWrite()
	} else {
		_ = c.Close()
	}
}
