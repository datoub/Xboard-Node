package singbox

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cedar2025/xboard-node/internal/model"
)

var proxyV2Signature = []byte{'\r', '\n', '\r', '\n', 0, '\r', '\n', 'Q', 'U', 'I', 'T', '\n'}

const proxyProbeTimeout = 3 * time.Second

var proxySourceMap sync.Map // upstream local port string -> source IP

func proxyProtocolInternalPort(port int) int {
	if port <= 0 {
		return port
	}
	return port + 10000
}

type proxyProtocolForwarder struct {
	listenAddr string
	targetAddr string
	targetPort int
	listener   net.Listener
	done       chan struct{}
	once       sync.Once
	wg         sync.WaitGroup
}

func newProxyProtocolForwarder(listenPort, targetPort int) *proxyProtocolForwarder {
	return &proxyProtocolForwarder{
		listenAddr: fmt.Sprintf(":%d", listenPort),
		targetAddr: fmt.Sprintf("127.0.0.1:%d", targetPort),
		targetPort: targetPort,
		done:       make(chan struct{}),
	}
}

func proxyForwarderSpecs(node *model.NodeSpec) map[int]int {
	if node == nil || !node.GetProxyProtocol() {
		return nil
	}
	port := node.ServerPort
	if port <= 0 {
		return nil
	}
	targetPort := node.ProxyInternalPort
	if targetPort <= 0 {
		targetPort = proxyProtocolInternalPort(port)
	}
	return map[int]int{port: targetPort}
}

func prepareProxyRuntimeNode(node *model.NodeSpec, current map[int]*proxyProtocolForwarder) (*model.NodeSpec, map[int]int, error) {
	if node == nil || !node.GetProxyProtocol() {
		return node, nil, nil
	}
	if node.ServerPort <= 0 {
		return node, nil, nil
	}
	targetPort := 0
	if forwarder := current[node.ServerPort]; forwarder != nil && forwarder.targetPort > 0 {
		targetPort = forwarder.targetPort
	}
	if targetPort <= 0 {
		exclude := map[int]struct{}{node.ServerPort: {}}
		for listenPort, forwarder := range current {
			exclude[listenPort] = struct{}{}
			if forwarder != nil && forwarder.targetPort > 0 {
				exclude[forwarder.targetPort] = struct{}{}
			}
		}
		port, err := reserveLoopbackPort(exclude)
		if err != nil {
			return nil, nil, err
		}
		targetPort = port
	}
	clone := *node
	clone.ProxyInternalPort = targetPort
	return &clone, map[int]int{node.ServerPort: targetPort}, nil
}

func reserveLoopbackPort(exclude map[int]struct{}) (int, error) {
	var lastErr error
	for attempts := 0; attempts < 32; attempts++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return 0, err
		}
		port := ln.Addr().(*net.TCPAddr).Port
		if err := ln.Close(); err != nil {
			return 0, err
		}
		if _, blocked := exclude[port]; !blocked {
			if canBindLoopbackUDP(port) {
				return port, nil
			}
			lastErr = fmt.Errorf("reserved loopback port %d is unavailable for udp", port)
			continue
		}
		lastErr = fmt.Errorf("reserved excluded loopback port %d", port)
	}
	if lastErr != nil {
		return 0, lastErr
	}
	return 0, fmt.Errorf("no loopback port available")
}

func canBindLoopbackUDP(port int) bool {
	conn, err := net.ListenPacket("udp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func startProxyForwarders(specs map[int]int) (map[int]*proxyProtocolForwarder, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	forwarders := make(map[int]*proxyProtocolForwarder, len(specs))
	for listenPort, targetPort := range specs {
		forwarder := newProxyProtocolForwarder(listenPort, targetPort)
		if err := forwarder.Start(); err != nil {
			closeProxyForwarders(forwarders)
			return forwarders, fmt.Errorf("listen %d -> %d: %w", listenPort, targetPort, err)
		}
		forwarders[listenPort] = forwarder
	}
	return forwarders, nil
}

func closeProxyForwarders(forwarders map[int]*proxyProtocolForwarder) {
	for _, forwarder := range forwarders {
		if forwarder != nil {
			forwarder.Close()
		}
	}
}

func (p *proxyProtocolForwarder) Start() error {
	ln, err := net.Listen("tcp", p.listenAddr)
	if err != nil {
		return err
	}
	p.listener = ln
	p.wg.Add(1)
	go p.acceptLoop()
	return nil
}

func (p *proxyProtocolForwarder) Close() {
	p.once.Do(func() {
		close(p.done)
		if p.listener != nil {
			_ = p.listener.Close()
		}
		p.wg.Wait()
	})
}

func (p *proxyProtocolForwarder) acceptLoop() {
	defer p.wg.Done()
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			select {
			case <-p.done:
				return
			default:
				continue
			}
		}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.handle(conn)
		}()
	}
}

func (p *proxyProtocolForwarder) handle(client net.Conn) {
	defer client.Close()
	upstream, err := net.DialTimeout("tcp", p.targetAddr, 8*time.Second)
	if err != nil {
		return
	}
	defer upstream.Close()

	prefix, sourceIP, err := readProxyPrefix(client)
	if err != nil {
		return
	}
	if sourceIP == "" {
		sourceIP = hostFromAddr(client.RemoteAddr())
	}
	upstreamPort := portFromAddr(upstream.LocalAddr())
	if upstreamPort != "" && sourceIP != "" {
		proxySourceMap.Store(upstreamPort, sourceIP)
		defer proxySourceMap.Delete(upstreamPort)
	}
	if len(prefix) > 0 {
		if _, err := upstream.Write(prefix); err != nil {
			return
		}
	}
	relay(client, upstream)
}

func readProxyPrefix(conn net.Conn) ([]byte, string, error) {
	_ = conn.SetReadDeadline(time.Now().Add(proxyProbeTimeout))
	reader := bufio.NewReader(conn)
	prefix, err := reader.Peek(16)
	if err != nil && len(prefix) == 0 {
		_ = conn.SetReadDeadline(time.Time{})
		return nil, "", err
	}

	if bytes.HasPrefix(prefix, proxyV2Signature) {
		if len(prefix) < 16 {
			_ = conn.SetReadDeadline(time.Time{})
			return nil, "", io.ErrUnexpectedEOF
		}
		length := int(binary.BigEndian.Uint16(prefix[14:16]))
		header := make([]byte, 16+length)
		if _, err := io.ReadFull(reader, header); err != nil {
			_ = conn.SetReadDeadline(time.Time{})
			return nil, "", err
		}
		_ = conn.SetReadDeadline(time.Time{})
		return drainBuffered(reader), proxyV2SourceIP(header), nil
	}

	if bytes.HasPrefix(prefix, []byte("PROXY ")) {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			_ = conn.SetReadDeadline(time.Time{})
			return nil, "", err
		}
		if !bytes.HasSuffix(line, []byte("\r\n")) {
			_ = conn.SetReadDeadline(time.Time{})
			return nil, "", fmt.Errorf("invalid proxy protocol v1 line")
		}
		_ = conn.SetReadDeadline(time.Time{})
		return drainBuffered(reader), proxyV1SourceIP(string(line)), nil
	}

	_ = conn.SetReadDeadline(time.Time{})
	return drainBuffered(reader), "", nil
}

func drainBuffered(reader *bufio.Reader) []byte {
	if reader.Buffered() == 0 {
		return nil
	}
	buf := make([]byte, reader.Buffered())
	_, _ = io.ReadFull(reader, buf)
	return buf
}

func relay(a, b net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(a, b)
		_ = closeWrite(a)
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(b, a)
		_ = closeWrite(b)
	}()
	wg.Wait()
}

func closeWrite(conn net.Conn) error {
	if tcp, ok := conn.(*net.TCPConn); ok {
		return tcp.CloseWrite()
	}
	return conn.Close()
}

func proxiedSourceIP(sourcePort uint16, fallback string) string {
	if sourcePort != 0 {
		if v, ok := proxySourceMap.Load(strconv.Itoa(int(sourcePort))); ok {
			if ip, ok := v.(string); ok && ip != "" {
				return ip
			}
		}
	}
	return fallback
}

func proxyV2SourceIP(header []byte) string {
	if len(header) < 16 {
		return ""
	}
	famProto := header[13]
	addr := header[16:]
	switch famProto & 0xF0 {
	case 0x10: // AF_INET
		if len(addr) >= 12 {
			return net.IP(addr[0:4]).String()
		}
	case 0x20: // AF_INET6
		if len(addr) >= 36 {
			return net.IP(addr[0:16]).String()
		}
	}
	return ""
}

func proxyV1SourceIP(line string) string {
	fields := strings.Fields(strings.TrimSpace(line))
	if len(fields) >= 3 && fields[0] == "PROXY" {
		return fields[2]
	}
	return ""
}

func hostFromAddr(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err == nil {
		return host
	}
	return addr.String()
}

func portFromAddr(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	_, port, err := net.SplitHostPort(addr.String())
	if err == nil {
		return port
	}
	return ""
}
