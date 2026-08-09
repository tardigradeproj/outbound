.PHONY: profile-tunnel-dial

profile-tunnel-dial:
	go test -bench=BenchmarkTunnelDial -benchmem -run=^$$ -cpuprofile=cpu.out -memprofile=mem.out .
	go tool pprof -ignore='benchEchoUpstream' -http=: mem.out
