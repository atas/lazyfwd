package httpserver

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/atas/autotunnel/internal/config"
	"github.com/atas/autotunnel/internal/tunnel"
	"github.com/atas/autotunnel/internal/tunnelmgr"
)

// tlsMockTunnel implements tunnelmgr.TunnelHandle for TLS tests
type tlsMockTunnel struct {
	running     bool
	localPort   int
	startErr    error
	startCalled bool
	touchCalled bool
}

func (m *tlsMockTunnel) IsRunning() bool { return m.running }
func (m *tlsMockTunnel) Start(ctx context.Context) error {
	m.startCalled = true
	if m.startErr != nil {
		return m.startErr
	}
	m.running = true
	return nil
}
func (m *tlsMockTunnel) Stop()                       {}
func (m *tlsMockTunnel) LocalPort() int              { return m.localPort }
func (m *tlsMockTunnel) Scheme() string              { return "https" }
func (m *tlsMockTunnel) Touch()                      { m.touchCalled = true }
func (m *tlsMockTunnel) IdleDuration() time.Duration { return 0 }
func (m *tlsMockTunnel) State() tunnel.State         { return tunnel.StateRunning }
func (m *tlsMockTunnel) LastError() error            { return nil }
func (m *tlsMockTunnel) Hostname() string            { return "mock.localhost" }
func (m *tlsMockTunnel) MarkFailed(err error)        { m.running = false }

// tlsMockManager implements Manager interface for TLS tests
type tlsMockManager struct {
	mu         sync.Mutex
	tunnel     tunnelmgr.TunnelHandle
	err        error
	getCalls   []string
	getSchemes []string
}

func (m *tlsMockManager) GetOrCreateTunnel(hostname string, scheme string) (tunnelmgr.TunnelHandle, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getCalls = append(m.getCalls, hostname)
	m.getSchemes = append(m.getSchemes, scheme)
	return m.tunnel, m.err
}

func (m *tlsMockManager) GetCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.getCalls...)
}

func (m *tlsMockManager) GetSchemes() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.getSchemes...)
}

func TestExtractSNI_EmptyData(t *testing.T) {
	_, err := extractSNI([]byte{})
	if err == nil {
		t.Error("Expected error for empty data")
	}
}

func TestExtractSNI_TooShort(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{"1 byte", []byte{0x16}},
		{"2 bytes", []byte{0x16, 0x03}},
		{"3 bytes", []byte{0x16, 0x03, 0x01}},
		{"4 bytes", []byte{0x16, 0x03, 0x01, 0x00}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := extractSNI(tc.data)
			if err == nil {
				t.Errorf("Expected error for %s, got nil", tc.name)
			}
		})
	}
}

func TestExtractSNI_NotTLSHandshake(t *testing.T) {
	// First byte is not 0x16 (TLS handshake)
	data := []byte{0x17, 0x03, 0x01, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00}
	_, err := extractSNI(data)
	if err == nil {
		t.Error("Expected error for non-TLS handshake")
	}
}

func TestExtractSNI_NotClientHello(t *testing.T) {
	// TLS record with handshake type != 0x01 (not ClientHello)
	data := []byte{
		0x16, 0x03, 0x01, 0x00, 0x05, // TLS record header
		0x02, 0x00, 0x00, 0x01, 0x00, // Handshake type 0x02 (ServerHello)
	}
	_, err := extractSNI(data)
	if err == nil {
		t.Error("Expected error for non-ClientHello")
	}
}

func TestExtractSNI_ValidClientHelloWithSNI(t *testing.T) {
	// Generate a valid ClientHello with SNI
	testSNI := "test.example.com"
	clientHello := generateClientHello(testSNI)

	sni, err := extractSNI(clientHello)
	if err != nil {
		t.Fatalf("extractSNI failed: %v", err)
	}

	if sni != testSNI {
		t.Errorf("Expected SNI %q, got %q", testSNI, sni)
	}
}

func TestExtractSNI_MultipleHosts(t *testing.T) {
	testHosts := []string{
		"localhost",
		"api.example.com",
		"sub.domain.example.org",
		"very-long-subdomain.with-many-parts.example.com",
	}

	for _, host := range testHosts {
		t.Run(host, func(t *testing.T) {
			clientHello := generateClientHello(host)
			sni, err := extractSNI(clientHello)
			if err != nil {
				t.Fatalf("extractSNI failed for %q: %v", host, err)
			}
			if sni != host {
				t.Errorf("Expected %q, got %q", host, sni)
			}
		})
	}
}

func TestHandleTLSConnection_Success(t *testing.T) {
	// Create a TLS backend using net.Pipe for simplicity
	// We just need to verify the TLS passthrough extracts SNI and routes correctly
	mockTun := &tlsMockTunnel{
		running:   true,
		localPort: 12345, // Dummy port - we won't actually connect
	}
	mockMgr := &tlsMockManager{tunnel: mockTun}

	cfg := &config.Config{
		Verbose: false,
		HTTP: config.HTTPConfig{
			ListenAddr: ":8989",
		},
	}

	server := NewServer(cfg, mockMgr)

	// Create a connection pair
	client, serverConn := net.Pipe()
	defer client.Close()
	defer serverConn.Close()

	// Create a ClientHello for our test SNI
	testSNI := "test.localhost"
	clientHello := generateClientHello(testSNI)

	// Send ClientHello in a goroutine
	done := make(chan struct{})
	go func() {
		defer close(done)

		// Write ClientHello
		_, err := client.Write(clientHello)
		if err != nil {
			return
		}

		// Close after a short delay to allow handling
		time.Sleep(100 * time.Millisecond)
		client.Close()
	}()

	// Handle the connection
	pc := newPeekConn(serverConn)
	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		server.handleTLSConnection(pc)
	}()

	// Wait for both client and handler to complete
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Test timed out waiting for client")
	}

	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Test timed out waiting for handler")
	}

	// Verify manager was called with correct hostname
	calls := mockMgr.GetCalls()
	if len(calls) != 1 || calls[0] != testSNI {
		t.Errorf("Expected GetOrCreateTunnel called with %q, got %v", testSNI, calls)
	}

	// Verify scheme was https for TLS
	schemes := mockMgr.GetSchemes()
	if len(schemes) > 0 && schemes[0] != "https" {
		t.Errorf("Expected scheme 'https', got %q", schemes[0])
	}
}

func TestHandleTLSConnection_UnknownSNI(t *testing.T) {
	mockMgr := &tlsMockManager{
		err: errors.New("no route configured"),
	}

	cfg := &config.Config{
		Verbose: false,
		HTTP: config.HTTPConfig{
			ListenAddr: ":8989",
		},
	}

	server := NewServer(cfg, mockMgr)

	client, serverConn := net.Pipe()
	defer serverConn.Close()

	testSNI := "unknown.localhost"
	clientHello := generateClientHello(testSNI)

	// Write ClientHello and immediately close
	go func() {
		_, _ = client.Write(clientHello)
		time.Sleep(50 * time.Millisecond)
		client.Close()
	}()

	pc := newPeekConn(serverConn)
	server.handleTLSConnection(pc)

	// Should have tried to look up the unknown host
	if len(mockMgr.getCalls) != 1 || mockMgr.getCalls[0] != testSNI {
		t.Errorf("Expected lookup for %q, got %v", testSNI, mockMgr.getCalls)
	}
}

func TestHandleTLSConnection_TunnelStartError(t *testing.T) {
	mockTun := &tlsMockTunnel{
		running:  false,
		startErr: errors.New("failed to start tunnel"),
	}
	mockMgr := &tlsMockManager{tunnel: mockTun}

	cfg := &config.Config{
		Verbose: false,
		HTTP: config.HTTPConfig{
			ListenAddr: ":8989",
		},
	}

	server := NewServer(cfg, mockMgr)

	client, serverConn := net.Pipe()
	defer serverConn.Close()

	testSNI := "test.localhost"
	clientHello := generateClientHello(testSNI)

	go func() {
		_, _ = client.Write(clientHello)
		time.Sleep(50 * time.Millisecond)
		client.Close()
	}()

	pc := newPeekConn(serverConn)
	server.handleTLSConnection(pc)

	if !mockTun.startCalled {
		t.Error("Expected Start() to be called for non-running tunnel")
	}
}

func TestBidirectionalCopy(t *testing.T) {
	// Test the bidirectional copy logic
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	testData := []byte("test data from client to server")

	var wg sync.WaitGroup
	wg.Add(2)

	// Client side - write and then close write
	go func() {
		defer wg.Done()
		_, _ = client.Write(testData)
		// Simulate closing write direction
		if tc, ok := client.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		} else {
			client.Close()
		}
	}()

	// Server side - read all data
	var received []byte
	go func() {
		defer wg.Done()
		received, _ = io.ReadAll(server)
	}()

	wg.Wait()

	if string(received) != string(testData) {
		t.Errorf("Expected %q, got %q", testData, received)
	}
}

func TestForwardClientHello(t *testing.T) {
	// Test that ClientHello bytes are correctly forwarded
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	testSNI := "forward.test.com"
	clientHello := generateClientHello(testSNI)

	go func() {
		_, _ = client.Write(clientHello)
		client.Close()
	}()

	received := make([]byte, len(clientHello))
	n, err := io.ReadFull(server, received)
	if err != nil {
		t.Fatalf("Failed to read forwarded data: %v", err)
	}

	if n != len(clientHello) {
		t.Errorf("Expected %d bytes, got %d", len(clientHello), n)
	}

	// Verify we can extract the same SNI from forwarded data
	extractedSNI, err := extractSNI(received)
	if err != nil {
		t.Fatalf("Failed to extract SNI from forwarded data: %v", err)
	}

	if extractedSNI != testSNI {
		t.Errorf("Expected SNI %q, got %q", testSNI, extractedSNI)
	}
}

