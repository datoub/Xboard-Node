package singbox

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/sagernet/sing-box/adapter"
	singM "github.com/sagernet/sing/common/metadata"
	"golang.org/x/time/rate"
)

func TestSingBoxCapabilities(t *testing.T) {
	s := New(config.KernelConfig{Type: "sing-box"})
	caps := s.Capabilities()
	if !caps.PerUserSpeedLimit || !caps.DeviceLimit || !caps.AliveIPTracking || !caps.ForceCloseUser {
		t.Fatalf("unexpected sing-box capabilities: %+v", caps)
	}
	if caps.BuiltInTrafficStats || caps.ForceCloseConnection {
		t.Fatalf("unexpected sing-box capabilities: %+v", caps)
	}
	protocols := s.Protocols()
	found := false
	for _, protocol := range protocols {
		if protocol == "hysteria" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("Protocols() missing hysteria: %v", protocols)
	}
}

type testConn struct {
	closed bool
	reads  [][]byte
	writes [][]byte
}

func (c *testConn) Read(b []byte) (int, error) {
	if len(c.reads) == 0 {
		return 0, errors.New("eof")
	}
	chunk := c.reads[0]
	c.reads = c.reads[1:]
	n := copy(b, chunk)
	return n, nil
}

func (c *testConn) Write(b []byte) (int, error) {
	cp := append([]byte(nil), b...)
	c.writes = append(c.writes, cp)
	return len(b), nil
}

func (c *testConn) Close() error                     { c.closed = true; return nil }
func (c *testConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *testConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *testConn) SetDeadline(time.Time) error      { return nil }
func (c *testConn) SetReadDeadline(time.Time) error  { return nil }
func (c *testConn) SetWriteDeadline(time.Time) error { return nil }

func testInboundContext(uuid, ip string) adapter.InboundContext {
	return testInboundContextWithPort(uuid, ip, 0)
}

func testInboundContextWithPort(uuid, ip string, port uint16) adapter.InboundContext {
	return adapter.InboundContext{
		User:   uuid,
		Source: singM.Socksaddr{Addr: netip.MustParseAddr(ip), Port: port},
	}
}

func TestConnTrackerRoutedConnectionTracksTrafficAndAliveIPs(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	base := &testConn{reads: [][]byte{[]byte("hello")}}

	wrapped := tracker.RoutedConnection(context.Background(), base, testInboundContext("uuid-1", "1.1.1.1"), nil, nil)

	buf := make([]byte, 16)
	n, err := wrapped.Read(buf)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if n != 5 {
		t.Fatalf("Read() bytes = %d, want 5", n)
	}
	if _, err := wrapped.Write([]byte("bye")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	traffic, aliveIPs, connCount := tracker.GetUserTraffic()
	if got := traffic[1]; got != [2]int64{5, 3} {
		t.Fatalf("traffic[1] = %v, want [5 3]", got)
	}
	if !aliveIPs[1]["1.1.1.1"] {
		t.Fatal("expected alive IP to include source address")
	}
	if connCount != 1 {
		t.Fatalf("connCount = %d, want 1", connCount)
	}

	if err := wrapped.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	_, aliveIPs, connCount = tracker.GetUserTraffic()
	if aliveIPs[1] != nil {
		t.Fatalf("aliveIPs after close = %v, want nil", aliveIPs[1])
	}
	if connCount != 0 {
		t.Fatalf("connCount after close = %d, want 0", connCount)
	}
}

func TestConnTrackerUsesProxyProtocolSourceIP(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	sourcePort := uint16(45678)
	proxySourceMap.Store(strconv.Itoa(int(sourcePort)), "1.2.3.4")
	defer proxySourceMap.Delete(strconv.Itoa(int(sourcePort)))

	base := &testConn{reads: [][]byte{[]byte("hello")}}
	wrapped := tracker.RoutedConnection(context.Background(), base, testInboundContextWithPort("uuid-1", "127.0.0.1", sourcePort), nil, nil)
	buf := make([]byte, 16)
	if _, err := wrapped.Read(buf); err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	_, aliveIPs, _ := tracker.GetUserTraffic()
	if !aliveIPs[1]["1.2.3.4"] {
		t.Fatalf("aliveIPs = %v, want proxy protocol source IP", aliveIPs[1])
	}
	if aliveIPs[1]["127.0.0.1"] {
		t.Fatalf("aliveIPs = %v, should not expose internal loopback source", aliveIPs[1])
	}
}

func TestProxyProtocolSourceIPParsers(t *testing.T) {
	if got := proxyV1SourceIP("PROXY TCP4 1.2.3.4 5.6.7.8 12345 443\r\n"); got != "1.2.3.4" {
		t.Fatalf("proxyV1SourceIP() = %q, want 1.2.3.4", got)
	}

	header := append([]byte(nil), proxyV2Signature...)
	header = append(header, 0x21, 0x11, 0, 12)
	header = append(header, net.ParseIP("9.8.7.6").To4()...)
	header = append(header, net.ParseIP("5.6.7.8").To4()...)
	ports := make([]byte, 4)
	binary.BigEndian.PutUint16(ports[0:2], 12345)
	binary.BigEndian.PutUint16(ports[2:4], 443)
	header = append(header, ports...)
	if got := proxyV2SourceIP(header); got != "9.8.7.6" {
		t.Fatalf("proxyV2SourceIP() = %q, want 9.8.7.6", got)
	}
}

func TestConnTrackerRoutedConnectionRejectsWhenDeviceLimitExceeded(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	tracker.SetDeviceLimitFunc(func(uuid string) (int, bool) {
		if uuid != "uuid-1" {
			return 0, false
		}
		return 1, true
	})

	first := &testConn{}
	wrapped1 := tracker.RoutedConnection(context.Background(), first, testInboundContext("uuid-1", "1.1.1.1"), nil, nil)
	if wrapped1 == first {
		t.Fatal("expected first connection to be wrapped")
	}

	second := &testConn{}
	wrapped2 := tracker.RoutedConnection(context.Background(), second, testInboundContext("uuid-1", "2.2.2.2"), nil, nil)
	if wrapped2 != second {
		t.Fatal("expected rejected connection to be returned unwrapped")
	}
	if !second.closed {
		t.Fatal("expected rejected connection to be closed")
	}
}

func TestConnTrackerCheckDeviceGateMergesFreshGlobalDevices(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	us := tracker.users[1]
	us.addConn("1.1.1.1")
	tracker.UpdateGlobalDevices(map[int][]string{1: {"9.9.9.9"}})

	if tracker.checkDeviceGate(us, 1, "2.2.2.2", 2) {
		t.Fatal("expected lexicographically earlier candidate to remain allowed")
	}
	if !tracker.checkDeviceGate(us, 1, "99.99.99.99", 2) {
		t.Fatal("expected merged local+global device state to reject lexicographically later third device")
	}
	if tracker.checkDeviceGate(us, 1, "1.1.1.1", 2) {
		t.Fatal("existing local IP should still be allowed")
	}
	if tracker.checkDeviceGate(us, 1, "9.9.9.9", 2) {
		t.Fatal("existing global IP should still be allowed")
	}
}

func TestConnTrackerCloseByIDClosesTrackedConnection(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	base := &testConn{}
	wrapped := tracker.RoutedConnection(context.Background(), base, testInboundContext("uuid-1", "1.1.1.1"), nil, nil)
	tracked, ok := wrapped.(*trackedConn)
	if !ok {
		t.Fatalf("wrapped type = %T, want *trackedConn", wrapped)
	}
	if !tracker.CloseByID(tracked.connID) {
		t.Fatal("CloseByID() = false, want true")
	}
	if !base.closed {
		t.Fatal("expected underlying connection to be closed")
	}
}

func TestConnTrackerRateLimitHonorsContextCancellation(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	tracker.SetSpeedLimitFunc(func(uuid string) *rate.Limiter {
		if uuid != "uuid-1" {
			return nil
		}
		return rate.NewLimiter(rate.Limit(1), 1)
	})

	readCtx, cancelRead := context.WithCancel(context.Background())
	readBase := &testConn{reads: [][]byte{[]byte("a"), []byte("a")}}
	readTracked := tracker.RoutedConnection(readCtx, readBase, testInboundContext("uuid-1", "1.1.1.1"), nil, nil).(*trackedConn)
	buf := make([]byte, 8)
	if n, err := readTracked.Read(buf); err != nil || n != 1 {
		t.Fatalf("first Read() = (%d, %v), want (1, nil)", n, err)
	}
	cancelRead()
	if n, err := readTracked.Read(buf); !errors.Is(err, context.Canceled) || n != 1 {
		t.Fatalf("second Read() = (%d, %v), want (1, context canceled)", n, err)
	}

	writeCtx, cancelWrite := context.WithCancel(context.Background())
	writeBase := &testConn{}
	writeTracked := tracker.RoutedConnection(writeCtx, writeBase, testInboundContext("uuid-1", "1.1.1.2"), nil, nil).(*trackedConn)
	if n, err := writeTracked.Write([]byte("a")); err != nil || n != 1 {
		t.Fatalf("first Write() = (%d, %v), want (1, nil)", n, err)
	}
	cancelWrite()
	if n, err := writeTracked.Write([]byte("a")); !errors.Is(err, context.Canceled) || n != 0 {
		t.Fatalf("second Write() = (%d, %v), want (0, context canceled)", n, err)
	}
}
