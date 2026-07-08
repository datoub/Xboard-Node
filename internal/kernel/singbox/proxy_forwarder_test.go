package singbox

import (
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/model"
)

func TestPrepareProxyRuntimeNodeAllocatesDistinctInternalPort(t *testing.T) {
	listenPort := reserveTCPPort(t)
	node := &model.NodeSpec{ServerPort: listenPort, AcceptProxyProtocol: true}

	runtimeNode, specs, err := prepareProxyRuntimeNode(node, nil)
	if err != nil {
		t.Fatalf("prepareProxyRuntimeNode() error = %v", err)
	}
	if runtimeNode == node {
		t.Fatal("expected runtime node clone")
	}
	if runtimeNode.ProxyInternalPort == 0 {
		t.Fatal("expected allocated proxy internal port")
	}
	if runtimeNode.ProxyInternalPort == listenPort {
		t.Fatalf("internal port must not equal external listen port %d", listenPort)
	}
	if specs[listenPort] != runtimeNode.ProxyInternalPort {
		t.Fatalf("spec target = %d, want %d", specs[listenPort], runtimeNode.ProxyInternalPort)
	}
}

func TestPrepareProxyRuntimeNodeReusesExistingTargetPort(t *testing.T) {
	node := &model.NodeSpec{ServerPort: 32001, AcceptProxyProtocol: true}
	current := map[int]*proxyProtocolForwarder{
		32001: newProxyProtocolForwarder(32001, 43123),
	}

	runtimeNode, specs, err := prepareProxyRuntimeNode(node, current)
	if err != nil {
		t.Fatalf("prepareProxyRuntimeNode() error = %v", err)
	}
	if runtimeNode.ProxyInternalPort != 43123 {
		t.Fatalf("internal port = %d, want reused 43123", runtimeNode.ProxyInternalPort)
	}
	if specs[32001] != 43123 {
		t.Fatalf("spec target = %d, want 43123", specs[32001])
	}
}

func TestReserveLoopbackPortSkipsUDPUnavailablePort(t *testing.T) {
	blocked := reserveTCPPort(t)
	udp, err := net.ListenPacket("udp", net.JoinHostPort("127.0.0.1", strconv.Itoa(blocked)))
	if err != nil {
		t.Fatalf("reserve udp blocker: %v", err)
	}
	defer udp.Close()

	port, err := reserveLoopbackPort(map[int]struct{}{})
	if err != nil {
		t.Fatalf("reserveLoopbackPort() error = %v", err)
	}
	if port == blocked {
		t.Fatalf("reserveLoopbackPort() chose udp-blocked port %d", blocked)
	}
}

func TestStartProxyForwardersSupportsMultiplePorts(t *testing.T) {
	targetPort1, received1, closeTarget1 := startTCPTestTarget(t)
	defer closeTarget1()
	targetPort2, received2, closeTarget2 := startTCPTestTarget(t)
	defer closeTarget2()

	listenPort1 := reserveTCPPort(t)
	listenPort2 := reserveTCPPort(t)
	forwarders, err := startProxyForwarders(map[int]int{
		listenPort1: targetPort1,
		listenPort2: targetPort2,
	})
	if err != nil {
		t.Fatalf("startProxyForwarders() error = %v", err)
	}
	defer closeProxyForwarders(forwarders)

	writeTCP(t, listenPort1, "first")
	writeTCP(t, listenPort2, "second")

	assertReceived(t, received1, "first")
	assertReceived(t, received2, "second")
}

func TestSyncProxyForwardersReplacesChangedTargets(t *testing.T) {
	targetPort1, received1, closeTarget1 := startTCPTestTarget(t)
	defer closeTarget1()
	targetPort2, received2, closeTarget2 := startTCPTestTarget(t)
	defer closeTarget2()

	listenPort := reserveTCPPort(t)
	s := &SingBox{}
	if err := s.syncProxyForwarders(map[int]int{listenPort: targetPort1}); err != nil {
		t.Fatalf("initial syncProxyForwarders() error = %v", err)
	}
	if err := s.syncProxyForwarders(map[int]int{listenPort: targetPort2}); err != nil {
		t.Fatalf("replace syncProxyForwarders() error = %v", err)
	}
	defer closeProxyForwarders(s.proxyForwarders)

	writeTCP(t, listenPort, "replacement")

	select {
	case got := <-received1:
		t.Fatalf("old target received %q after replacement", got)
	case <-time.After(150 * time.Millisecond):
	}
	assertReceived(t, received2, "replacement")
}

func startTCPTestTarget(t *testing.T) (int, <-chan string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target: %v", err)
	}
	received := make(chan string, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		data, _ := io.ReadAll(conn)
		received <- string(data)
	}()
	return ln.Addr().(*net.TCPAddr).Port, received, func() {
		_ = ln.Close()
		<-done
	}
}

func reserveTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatalf("close reserved port: %v", err)
	}
	return port
}

func writeTCP(t *testing.T, port int, payload string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), time.Second)
	if err != nil {
		t.Fatalf("dial forwarder %d: %v", port, err)
	}
	if _, err := conn.Write([]byte(payload)); err != nil {
		_ = conn.Close()
		t.Fatalf("write forwarder %d: %v", port, err)
	}
	_ = conn.Close()
}

func assertReceived(t *testing.T, ch <-chan string, want string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("received = %q, want %q", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for %q", want)
	}
}
