package outbound_test

import (
	"context"
	"fmt"
	"net"

	"github.com/tardigradeproj/outbound"
)

// ExampleTunnel_Dial shows how a cloud-side caller dials a named upstream
// registered on the worker side and reads data from it.
func ExampleTunnel_Dial() {
	// Wire the worker and cloud sessions together over an in-memory pipe.
	workerConn, cloudConn := net.Pipe()

	workerSession, _ := outbound.Server(workerConn, nil)
	cloudSession, _ := outbound.Client(cloudConn, nil)

	// Worker side: register a local TCP service as upstream ID 1.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	go func() {
		cn, err := ln.Accept()
		if err != nil {
			return
		}
		defer cn.Close()
		cn.Write([]byte("hello"))
	}()

	workerTunnel := outbound.New(workerSession)
	workerTunnel.Register(outbound.Upstream{
		Id:   1,
		Name: "greeter",
		Dial: outbound.TCPUpstream(ln.Addr().String()),
	})
	go workerTunnel.Serve(context.Background())

	// Cloud side: open a connection to upstream 1 and read its response.
	cloudTunnel := outbound.New(cloudSession)
	conn, err := cloudTunnel.Dial(context.Background(), 1)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	buf := make([]byte, 5)
	if _, err := conn.Read(buf); err != nil {
		panic(err)
	}
	fmt.Println(string(buf))
	// Output: hello
}
