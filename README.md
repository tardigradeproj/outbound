# Outbound

Outbound lets you expose multiple NAT'd services on a single port. 
A Go fork of [Yamux](https://github.com/hashicorp/yamux), it layers stream-oriented multiplexing over a 
reliable, ordered transport such as TCP or a Unix domain socket.

Outbound features include:

* Bi-directional streams
  * Streams can be opened by either client or server
  * Useful for NAT traversal
  * Server-side push support
* Flow control
  * Avoid starvation
  * Back-pressure to prevent overwhelming a receiver
* Keep Alives
  * Enables persistent connections over a load balancer
* Efficient
  * Enables thousands of logical streams with low overhead
* Targets
  * Enables TCP communication with multiple services using a single port

## Documentation

For complete documentation, see the associated [Godoc](http://godoc.org/github.com/tardigradeproj/outbound).

## Specification

The full specification for Outbound is provided in the `spec.md` file.
It can be used as a guide to implementors of interoperable libraries.

## Usage

### Tunnel — expose local services over a single multiplexed connection

`Tunnel` wraps a session and routes incoming streams to registered local upstreams by ID.
This lets a worker behind NAT register services that a cloud host can dial by name.

```go
// Worker side (behind NAT): register a local HTTP service and serve streams.
func worker() {
    conn, err := net.Dial("tcp", "cloud-host:1234")
    if err != nil {
        panic(err)
    }

    session, err := outbound.Client(conn, nil)
    if err != nil {
        panic(err)
    }

    tunnel := outbound.New(session)
    tunnel.Register(outbound.Upstream{
        Id:   1,
        Name: "http",
        Dial: outbound.TCPUpstream("127.0.0.1:8080"),
    })

    // Blocks until the session closes or ctx is cancelled.
    tunnel.Serve(context.Background())
}

// Cloud side: dial the worker's upstream by ID and use it like a plain net.Conn.
func cloud() {
    conn, err := listener.Accept()
    if err != nil {
        panic(err)
    }

    session, err := outbound.Server(conn, nil)
    if err != nil {
        panic(err)
    }

    tunnel := outbound.New(session)

    // Opens a transparent pipe to the worker's upstream 1 (127.0.0.1:8080).
    c, err := tunnel.Dial(context.Background(), 1)
    if err != nil {
        panic(err)
    }

    buf := make([]byte, 4)
    c.Read(buf) // reads data sent by the upstream
}
```

