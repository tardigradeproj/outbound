// Copyright IBM Corp. 2014, 2025
// SPDX-License-Identifier: MPL-2.0

package outbound

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
)

// benchEchoUpstream starts a real TCP echo server on loopback and registers
// it in reg under id.
func benchEchoUpstream(b *testing.B, reg *Registry, id uint8) {
	b.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to listen: %v", err)
	}
	b.Cleanup(func() { _ = ln.Close() })

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

	reg.Register(Upstream{Id: id, Name: "bench-echo", Dial: TCPUpstream(ln.Addr().String())})
}

// benchTunnelPair wires up a worker Tunnel serving reg and a cloud Tunnel
// dialing into it, over a real loopback+TLS outbound session (via
// testClientServer). Only the cloud Tunnel is returned since callers only
// ever dial from it; the worker just needs to be kept serving in the
// background.
func benchTunnelPair(b *testing.B, reg *Registry) *Tunnel {
	b.Helper()

	clientSession, serverSession := testClientServer(b)
	worker := NewTunnel(clientSession, reg)
	cloud := NewTunnel(serverSession, NewRegistry())

	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)
	go worker.Serve(ctx)

	return cloud
}

// BenchmarkTunnelDial measures the full cost of establishing a usable
// proxied connection: OpenStream, the worker accepting it, a Registry
// lookup, a real TCP dial to the upstream, and the first byte echoed back.
// This is the Tunnel-level analogue of BenchmarkAccept, which only measures
// a bare Session stream open/close with no upstream involved.
func BenchmarkTunnelDial(b *testing.B) {
	reg := NewRegistry()
	benchEchoUpstream(b, reg, 1)
	cloud := benchTunnelPair(b, reg)

	probe := []byte{0}
	recv := make([]byte, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		cnn, err := cloud.Dial(context.Background(), 1)
		if err != nil {
			b.Fatalf("dial: %v", err)
		}
		if _, err := cnn.Write(probe); err != nil {
			b.Fatalf("write: %v", err)
		}
		if _, err := io.ReadFull(cnn, recv); err != nil && !errors.Is(err, io.EOF) {
			b.Fatalf("read: %v", err)
		}
		cnn.Close()
	}
}

func BenchmarkTunnelSendRecv32(b *testing.B)   { benchmarkTunnelSendRecv(b, 32) }
func BenchmarkTunnelSendRecv64(b *testing.B)   { benchmarkTunnelSendRecv(b, 64) }
func BenchmarkTunnelSendRecv128(b *testing.B)  { benchmarkTunnelSendRecv(b, 128) }
func BenchmarkTunnelSendRecv256(b *testing.B)  { benchmarkTunnelSendRecv(b, 256) }
func BenchmarkTunnelSendRecv512(b *testing.B)  { benchmarkTunnelSendRecv(b, 512) }
func BenchmarkTunnelSendRecv1024(b *testing.B) { benchmarkTunnelSendRecv(b, 1024) }
func BenchmarkTunnelSendRecv2048(b *testing.B) { benchmarkTunnelSendRecv(b, 2048) }
func BenchmarkTunnelSendRecv4096(b *testing.B) { benchmarkTunnelSendRecv(b, 4096) }

// benchmarkTunnelSendRecv measures single-stream, steady-state round-trip
// cost through an already-established Tunnel connection: write size bytes,
// read size bytes back once the upstream echoes them.
func benchmarkTunnelSendRecv(b *testing.B, size int) {
	reg := NewRegistry()
	benchEchoUpstream(b, reg, 1)
	cloud := benchTunnelPair(b, reg)

	cnn, err := cloud.Dial(context.Background(), 1)
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	defer cnn.Close()

	sendBuf := make([]byte, size)
	recvBuf := make([]byte, size)

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if _, err := cnn.Write(sendBuf); err != nil {
			b.Fatalf("write: %v", err)
		}
		if _, err := io.ReadFull(cnn, recvBuf); err != nil {
			b.Fatalf("read: %v", err)
		}
	}
}

func BenchmarkTunnelSendRecvParallel32(b *testing.B)   { benchmarkTunnelSendRecvParallel(b, 32) }
func BenchmarkTunnelSendRecvParallel64(b *testing.B)   { benchmarkTunnelSendRecvParallel(b, 64) }
func BenchmarkTunnelSendRecvParallel128(b *testing.B)  { benchmarkTunnelSendRecvParallel(b, 128) }
func BenchmarkTunnelSendRecvParallel256(b *testing.B)  { benchmarkTunnelSendRecvParallel(b, 256) }
func BenchmarkTunnelSendRecvParallel512(b *testing.B)  { benchmarkTunnelSendRecvParallel(b, 512) }
func BenchmarkTunnelSendRecvParallel1024(b *testing.B) { benchmarkTunnelSendRecvParallel(b, 1024) }
func BenchmarkTunnelSendRecvParallel2048(b *testing.B) { benchmarkTunnelSendRecvParallel(b, 2048) }
func BenchmarkTunnelSendRecvParallel4096(b *testing.B) { benchmarkTunnelSendRecvParallel(b, 4096) }

// benchmarkTunnelSendRecvParallel measures round-trip throughput with many
// concurrent streams sharing one Tunnel/session, each dialing its own
// upstream connection through the shared Registry. This is the
// Tunnel-level analogue of benchmarkSendRecvParallel: it exercises Registry
// lookup contention and per-stream worker-side goroutines under concurrency,
// which the single-stream benchmarks above cannot surface.
func benchmarkTunnelSendRecvParallel(b *testing.B, size int) {
	reg := NewRegistry()
	benchEchoUpstream(b, reg, 1)
	cloud := benchTunnelPair(b, reg)

	sendBuf := make([]byte, size)

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		cnn, err := cloud.Dial(context.Background(), 1)
		if err != nil {
			b.Fatalf("dial: %v", err)
		}
		defer cnn.Close()

		recvBuf := make([]byte, size)
		for pb.Next() {
			if _, err := cnn.Write(sendBuf); err != nil {
				b.Fatalf("write: %v", err)
			}
			if _, err := io.ReadFull(cnn, recvBuf); err != nil {
				b.Fatalf("read: %v", err)
			}
		}
	})
}
