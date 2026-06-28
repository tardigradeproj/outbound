package outbound

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
)

func TestTunnelUnregisterWhileConnectionActive(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	// Upstream echoes whatever it receives.
	go func() {
		for {
			cn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(cn net.Conn) {
				defer cn.Close()
				pr, pw := io.Pipe()
				go func() { io.Copy(pw, cn); pw.Close() }()
				io.Copy(cn, pr)
			}(cn)
		}
	}()

	clientSession, serverSession := testClientServer(t)
	workerTunnel := NewTunnel(clientSession)
	workerTunnel.Register(Upstream{Id: 7, Name: "echo", Dial: TCPUpstream(ln.Addr().String())})
	go workerTunnel.Serve(context.Background())

	cloudTunnel := NewTunnel(serverSession)

	// Establish the first connection and confirm it is live before unregistering.
	cnn1, err := cloudTunnel.Dial(context.Background(), 7)
	if err != nil {
		t.Fatalf("first dial failed: %v", err)
	}
	defer cnn1.Close()

	ping := []byte("ping")
	if _, err := cnn1.Write(ping); err != nil {
		t.Fatalf("first write failed: %v", err)
	}
	got := make([]byte, len(ping))
	if _, err := io.ReadFull(cnn1, got); err != nil {
		t.Fatalf("first read failed: %v", err)
	}
	if !bytes.Equal(got, ping) {
		t.Fatalf("got %q, want %q", got, ping)
	}

	// Unregister the upstream while cnn1 is still open.
	workerTunnel.Unregister(7)

	// The live connection must still work after Unregister.
	msg := []byte("still alive")
	if _, err := cnn1.Write(msg); err != nil {
		t.Fatalf("write after unregister failed: %v", err)
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(cnn1, buf); err != nil {
		t.Fatalf("read after unregister failed: %v", err)
	}
	if !bytes.Equal(buf, msg) {
		t.Fatalf("got %q, want %q", buf, msg)
	}

	// A new dial for the now-unregistered ID must behave like an unknown upstream:
	// Dial succeeds at the outbound level but Read returns an error.
	cnn2, err := cloudTunnel.Dial(context.Background(), 7)
	if err != nil {
		t.Fatalf("second dial failed: %v", err)
	}
	defer cnn2.Close()

	n, err := cnn2.Read(make([]byte, 16))
	if n != 0 || err == nil {
		t.Fatalf("expected EOF on new dial after unregister, got n=%d err=%v", n, err)
	}
}

func TestTunnelDialUnknownUpstreamID(t *testing.T) {
	clientSession, serverSession := testClientServer(t)

	workerTunnel := NewTunnel(clientSession)
	// Intentionally register nothing — upstream ID 99 is unknown.
	go workerTunnel.Serve(context.Background())

	cloudTunnel := NewTunnel(serverSession)
	cnn, err := cloudTunnel.Dial(context.Background(), 99)
	if err != nil {
		t.Fatalf("Dial should succeed at the outbound level: %v", err)
	}
	defer cnn.Close()

	// The worker closes the stream when it finds no matching upstream.
	// A Read on the cloud side must return an error or EOF — not hang.
	buf := make([]byte, 16)
	n, err := cnn.Read(buf)
	if n != 0 || err == nil {
		t.Fatalf("expected 0 bytes and an error, got n=%d err=%v", n, err)
	}
}

func TestTunnelConcurrentDials(t *testing.T) {
	const dialCount = 20

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	// Each accepted connection gets a unique index written back to the caller.
	// We accept dialCount connections, each in its own goroutine.
	go func() {
		for i := 0; i < dialCount; i++ {
			cn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(cn net.Conn, idx int) {
				defer cn.Close()
				msg := fmt.Sprintf("conn-%02d", idx)
				buf := make([]byte, len(msg))
				if _, err := io.ReadFull(cn, buf); err != nil {
					return
				}
				cn.Write([]byte(msg))
			}(cn, i)
		}
	}()

	clientSession, serverSession := testClientServer(t)
	workerTunnel := NewTunnel(clientSession)
	workerTunnel.Register(Upstream{
		Id:   1,
		Name: "concurrent",
		Dial: TCPUpstream(ln.Addr().String()),
	})
	go workerTunnel.Serve(context.Background())

	cloudTunnel := NewTunnel(serverSession)

	type result struct {
		idx int
		err error
	}
	results := make(chan result, dialCount)

	var wg sync.WaitGroup
	for i := 0; i < dialCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cnn, err := cloudTunnel.Dial(context.Background(), 1)
			if err != nil {
				results <- result{idx, fmt.Errorf("dial: %w", err)}
				return
			}
			defer cnn.Close()

			msg := fmt.Sprintf("conn-%02d", idx)
			if _, err := cnn.Write([]byte(msg)); err != nil {
				results <- result{idx, fmt.Errorf("write: %w", err)}
				return
			}
			buf := make([]byte, len(msg))
			if _, err := io.ReadFull(cnn, buf); err != nil {
				results <- result{idx, fmt.Errorf("read: %w", err)}
				return
			}
			// The upstream echoes back "conn-XX" where XX is its own accept index,
			// not necessarily the same as idx (connections race). Just verify the
			// response is well-formed.
			var n int
			if _, err := fmt.Sscanf(string(buf), "conn-%02d", &n); err != nil {
				results <- result{idx, fmt.Errorf("bad response %q: %w", string(buf), err)}
				return
			}
			results <- result{idx, nil}
		}(i)
	}

	wg.Wait()
	close(results)

	for r := range results {
		if r.err != nil {
			t.Errorf("goroutine %d: %v", r.idx, r.err)
		}
	}
}

func TestTunnelMultipleUpstreamsRouteByID(t *testing.T) {
	upstreams := []struct {
		id      uint8
		message string
	}{
		{id: 1, message: "upstream-one"},
		{id: 2, message: "upstream-two"},
		{id: 3, message: "upstream-three"},
	}

	clientSession, serverSession := testClientServer(t)
	workerTunnel := NewTunnel(clientSession)

	for _, u := range upstreams {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to listen for upstream %d: %v", u.id, err)
		}
		defer ln.Close()

		msg := u.message
		go func() {
			cn, err := ln.Accept()
			if err != nil {
				return
			}
			defer cn.Close()
			_, _ = cn.Write([]byte(msg))
		}()

		workerTunnel.Register(Upstream{
			Id:   u.id,
			Name: u.message,
			Dial: TCPUpstream(ln.Addr().String()),
		})
	}

	go workerTunnel.Serve(context.Background())

	cloudTunnel := NewTunnel(serverSession)

	for _, u := range upstreams {
		cnn, err := cloudTunnel.Dial(context.Background(), u.id)
		if err != nil {
			t.Fatalf("upstream %d: failed to dial: %v", u.id, err)
		}

		buf := make([]byte, len(u.message))
		_, err = cnn.Read(buf)
		cnn.Close()
		if err != nil {
			t.Fatalf("upstream %d: failed to read: %v", u.id, err)
		}
		if string(buf) != u.message {
			t.Fatalf("upstream %d: got %q, want %q", u.id, string(buf), u.message)
		}
	}
}

func TestTunnelLargePayloadStreaming(t *testing.T) {
	const size = 4 * 1024 * 1024 // 4 MB

	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	// Echo server: io.Pipe decouples the read goroutine from the write goroutine so
	// neither direction blocks the other under outbound flow control backpressure.
	go func() {
		cn, err := ln.Accept()
		if err != nil {
			return
		}
		defer cn.Close()
		pr, pw := io.Pipe()
		go func() {
			io.Copy(pw, cn)
			pw.Close()
		}()
		io.Copy(cn, pr)
	}()

	clientSession, serverSession := testClientServer(t)
	workerTunnel := NewTunnel(clientSession)
	workerTunnel.Register(Upstream{
		Id:   1,
		Name: "echo",
		Dial: TCPUpstream(ln.Addr().String()),
	})
	go workerTunnel.Serve(context.Background())

	cloudTunnel := NewTunnel(serverSession)
	cnn, err := cloudTunnel.Dial(context.Background(), 1)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cnn.Close()

	// Write the full payload in a goroutine so it runs concurrently with the read below.
	writeErr := make(chan error, 1)
	go func() {
		_, err := io.Copy(cnn, bytes.NewReader(payload))
		writeErr <- err
	}()

	// Read back exactly size bytes (the echo) and verify byte-for-byte.
	received := make([]byte, size)
	if _, err := io.ReadFull(cnn, received); err != nil {
		t.Fatalf("read error: %v", err)
	}

	if err := <-writeErr; err != nil {
		t.Fatalf("write error: %v", err)
	}

	for i := range payload {
		if received[i] != payload[i] {
			t.Fatalf("mismatch at byte %d: got %d, want %d", i, received[i], payload[i])
		}
	}
}

func TestTunnelSendingTrafficToBothEnds(t *testing.T) {
	listenAddr := "127.0.0.1:9995"
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	go func() {
		cn, err := ln.Accept()
		if err != nil {
			t.Fatalf("failed to accept connection: %v", err)
		}
		_, _ = cn.Write([]byte("I'm here"))
	}()
	clientSession, serverSession := testClientServer(t)
	workerTunnel := NewTunnel(clientSession)
	workerTunnel.Register(Upstream{
		Id:   12,
		Name: "test",
		Dial: TCPUpstream(listenAddr),
	})
	go workerTunnel.Serve(context.Background())

	cloudTunnel := NewTunnel(serverSession)
	cnn, err := cloudTunnel.Dial(context.Background(), 12)

	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	_, err = cnn.Write([]byte("hello world"))
	if err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	b := make([]byte, 8)
	_, err = cnn.Read(b)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}
	if string(b) != "I'm here" {
		t.Fatalf("got %s, want %s", string(b), "I'm here")
	}
}
