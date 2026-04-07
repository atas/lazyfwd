package httpserver

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/atas/autotunnel/internal/config"
	"github.com/atas/autotunnel/internal/tunnel"
	"github.com/atas/autotunnel/internal/tunnelmgr"
)

// mockTunnel implements tunnelmgr.TunnelHandle for testing
type mockTunnel struct {
	running     bool
	localPort   int
	scheme      string
	startErr    error
	startCalled bool
	touchCalled bool
}

func (m *mockTunnel) IsRunning() bool              { return m.running }
func (m *mockTunnel) Start(ctx context.Context) error {
	m.startCalled = true
	if m.startErr != nil {
		return m.startErr
	}
	m.running = true
	return nil
}
func (m *mockTunnel) Stop()                        {}
func (m *mockTunnel) LocalPort() int               { return m.localPort }
func (m *mockTunnel) Scheme() string {
	if m.scheme == "" {
		return "http"
	}
	return m.scheme
}
func (m *mockTunnel) Touch()                       { m.touchCalled = true }
func (m *mockTunnel) IdleDuration() time.Duration  { return 0 }
func (m *mockTunnel) State() tunnel.State          { return tunnel.StateRunning }
func (m *mockTunnel) LastError() error             { return nil }
func (m *mockTunnel) Hostname() string             { return "mock.localhost" }
func (m *mockTunnel) MarkFailed(err error)         { m.running = false }

// retryMockTunnel is a mock tunnel that can simulate a dead port-forward.
// On the first request it points to a dead port; after MarkFailed + recreation
// it points to a working backend.
type retryMockTunnel struct {
	running       bool
	localPort     int
	scheme        string
	startCalled   bool
	touchCalled   bool
	markedFailed  bool
	failedErr     error
}

func (m *retryMockTunnel) IsRunning() bool              { return m.running }
func (m *retryMockTunnel) Start(ctx context.Context) error {
	m.startCalled = true
	m.running = true
	return nil
}
func (m *retryMockTunnel) Stop()                        {}
func (m *retryMockTunnel) LocalPort() int               { return m.localPort }
func (m *retryMockTunnel) Scheme() string {
	if m.scheme == "" {
		return "http"
	}
	return m.scheme
}
func (m *retryMockTunnel) Touch()                       { m.touchCalled = true }
func (m *retryMockTunnel) IdleDuration() time.Duration  { return 0 }
func (m *retryMockTunnel) State() tunnel.State          { return tunnel.StateRunning }
func (m *retryMockTunnel) LastError() error             { return m.failedErr }
func (m *retryMockTunnel) Hostname() string             { return "mock.localhost" }
func (m *retryMockTunnel) MarkFailed(err error) {
	m.markedFailed = true
	m.failedErr = err
	m.running = false
}

// mockManager implements Manager interface for testing
type mockManager struct {
	tunnel      tunnelmgr.TunnelHandle
	err         error
	getCalls    []string
	getSchemes  []string
}

func (m *mockManager) GetOrCreateTunnel(hostname string, scheme string) (tunnelmgr.TunnelHandle, error) {
	m.getCalls = append(m.getCalls, hostname)
	m.getSchemes = append(m.getSchemes, scheme)
	return m.tunnel, m.err
}

// retryMockManager returns different tunnels on successive calls,
// simulating a dead tunnel being replaced with a fresh one.
type retryMockManager struct {
	tunnels    []tunnelmgr.TunnelHandle
	callIndex  int
	getCalls   []string
}

func (m *retryMockManager) GetOrCreateTunnel(hostname string, scheme string) (tunnelmgr.TunnelHandle, error) {
	m.getCalls = append(m.getCalls, hostname)
	idx := m.callIndex
	if idx >= len(m.tunnels) {
		idx = len(m.tunnels) - 1
	}
	m.callIndex++
	return m.tunnels[idx], nil
}

func testHTTPConfig() *config.Config {
	return &config.Config{
		Verbose: false,
		HTTP: config.HTTPConfig{
			ListenAddr: ":8989",
		},
	}
}

func TestServer_ServeHTTP_ValidHost(t *testing.T) {
	// Create a backend server to proxy to
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("backend response"))
	}))
	defer backend.Close()

	// Extract port from backend URL
	backendPort := backend.Listener.Addr().(*net.TCPAddr).Port

	mockTun := &mockTunnel{
		running:   true,
		localPort: backendPort,
		scheme:    "http",
	}
	mockMgr := &mockManager{tunnel: mockTun}
	cfg := testHTTPConfig()

	server := NewServer(cfg, mockMgr)

	req := httptest.NewRequest("GET", "/test-path", nil)
	req.Host = "test.localhost:8989"

	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	// Verify manager was called with correct hostname (without port)
	if len(mockMgr.getCalls) != 1 || mockMgr.getCalls[0] != "test.localhost" {
		t.Errorf("Expected GetOrCreateTunnel called with 'test.localhost', got %v", mockMgr.getCalls)
	}

	// Verify scheme was passed correctly
	if mockMgr.getSchemes[0] != "http" {
		t.Errorf("Expected scheme 'http', got %q", mockMgr.getSchemes[0])
	}

	// Verify Touch was called
	if !mockTun.touchCalled {
		t.Error("Expected Touch() to be called")
	}
}

func TestServer_ServeHTTP_UnknownHost(t *testing.T) {
	mockMgr := &mockManager{
		err: errors.New("no route configured for hostname: unknown.localhost"),
	}
	cfg := testHTTPConfig()

	server := NewServer(cfg, mockMgr)

	req := httptest.NewRequest("GET", "/", nil)
	req.Host = "unknown.localhost:8989"

	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusBadGateway {
		t.Errorf("Expected status 502, got %d", w.Code)
	}

	// Verify error message is in response
	body := w.Body.String()
	if body == "" {
		t.Error("Expected error message in response body")
	}
}

func TestServer_ServeHTTP_HostWithPort(t *testing.T) {
	// Create a backend server
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	backendPort := backend.Listener.Addr().(*net.TCPAddr).Port

	mockTun := &mockTunnel{
		running:   true,
		localPort: backendPort,
	}
	mockMgr := &mockManager{tunnel: mockTun}
	cfg := testHTTPConfig()

	server := NewServer(cfg, mockMgr)

	// Test with explicit port in Host header
	req := httptest.NewRequest("GET", "/", nil)
	req.Host = "myapp.localhost:9999"

	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	// Should strip port from hostname when looking up route
	if len(mockMgr.getCalls) != 1 || mockMgr.getCalls[0] != "myapp.localhost" {
		t.Errorf("Expected 'myapp.localhost', got %v", mockMgr.getCalls)
	}
}

func TestServer_ServeHTTP_EmptyHost(t *testing.T) {
	mockMgr := &mockManager{
		err: errors.New("no route configured"),
	}
	cfg := testHTTPConfig()

	server := NewServer(cfg, mockMgr)

	req := httptest.NewRequest("GET", "/", nil)
	req.Host = ""

	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusBadGateway {
		t.Errorf("Expected status 502 for empty host, got %d", w.Code)
	}
}

func TestServer_ServeHTTP_TunnelStartError(t *testing.T) {
	mockTun := &mockTunnel{
		running:  false,
		startErr: errors.New("failed to connect to k8s"),
	}
	mockMgr := &mockManager{tunnel: mockTun}
	cfg := testHTTPConfig()

	server := NewServer(cfg, mockMgr)

	req := httptest.NewRequest("GET", "/", nil)
	req.Host = "test.localhost"

	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusBadGateway {
		t.Errorf("Expected status 502, got %d", w.Code)
	}

	if !mockTun.startCalled {
		t.Error("Expected Start() to be called for non-running tunnel")
	}
}

func TestServer_ServeHTTP_StartsTunnelIfNotRunning(t *testing.T) {
	// Create a backend server
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	backendPort := backend.Listener.Addr().(*net.TCPAddr).Port

	mockTun := &mockTunnel{
		running:   false, // Not running initially
		localPort: backendPort,
	}
	mockMgr := &mockManager{tunnel: mockTun}
	cfg := testHTTPConfig()

	server := NewServer(cfg, mockMgr)

	req := httptest.NewRequest("GET", "/", nil)
	req.Host = "test.localhost"

	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if !mockTun.startCalled {
		t.Error("Expected Start() to be called")
	}

	// After successful start, tunnel should be running
	if !mockTun.running {
		t.Error("Expected tunnel to be running after Start()")
	}

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 after starting tunnel, got %d", w.Code)
	}
}

func TestServer_ServeHTTP_XForwardedHeaders(t *testing.T) {
	var receivedProto string
	var receivedHost string
	var receivedFor string
	var receivedOrigHost string

	// Use TLS backend since we're testing with scheme="https"
	backend := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedProto = r.Header.Get("X-Forwarded-Proto")
		receivedHost = r.Header.Get("X-Forwarded-Host")
		receivedFor = r.Header.Get("X-Forwarded-For")
		receivedOrigHost = r.Host
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	backendPort := backend.Listener.Addr().(*net.TCPAddr).Port

	mockTun := &mockTunnel{
		running:   true,
		localPort: backendPort,
		scheme:    "https",
	}
	mockMgr := &mockManager{tunnel: mockTun}
	cfg := testHTTPConfig()

	server := NewServer(cfg, mockMgr)

	req := httptest.NewRequest("GET", "/api/v1/resource", nil)
	req.Host = "secure.localhost:8989"
	req.RemoteAddr = "192.168.1.100:54321"

	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d", w.Code)
	}

	if receivedProto != "https" {
		t.Errorf("X-Forwarded-Proto: expected 'https', got %q", receivedProto)
	}

	if receivedHost != "secure.localhost:8989" {
		t.Errorf("X-Forwarded-Host: expected 'secure.localhost:8989', got %q", receivedHost)
	}

	// X-Forwarded-For may be appended by intermediate proxies, just check it contains the expected IP
	if !strings.Contains(receivedFor, "192.168.1.100") {
		t.Errorf("X-Forwarded-For: expected to contain '192.168.1.100', got %q", receivedFor)
	}

	// Original Host header should be preserved
	if receivedOrigHost != "secure.localhost:8989" {
		t.Errorf("Host header: expected 'secure.localhost:8989', got %q", receivedOrigHost)
	}
}

func TestServer_ServeHTTP_HTTPSchemeDefault(t *testing.T) {
	var receivedProto string

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedProto = r.Header.Get("X-Forwarded-Proto")
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	backendPort := backend.Listener.Addr().(*net.TCPAddr).Port

	mockTun := &mockTunnel{
		running:   true,
		localPort: backendPort,
		scheme:    "", // Empty scheme should default to http
	}
	mockMgr := &mockManager{tunnel: mockTun}
	cfg := testHTTPConfig()

	server := NewServer(cfg, mockMgr)

	req := httptest.NewRequest("GET", "/", nil)
	req.Host = "test.localhost"

	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if receivedProto != "http" {
		t.Errorf("Expected default X-Forwarded-Proto 'http', got %q", receivedProto)
	}
}

func TestServer_ServeHTTP_RetryOnDeadTunnel(t *testing.T) {
	// Simulate a dead port-forward: first tunnel points to a port with nothing listening,
	// after MarkFailed the manager returns a fresh tunnel pointing to a working backend.

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("recovered"))
	}))
	defer backend.Close()
	backendPort := backend.Listener.Addr().(*net.TCPAddr).Port

	// Dead tunnel points to a port that's not listening
	deadTun := &retryMockTunnel{
		running:   true,
		localPort: 1, // port 1 should refuse connections
		scheme:    "http",
	}

	// Fresh tunnel points to working backend
	freshTun := &retryMockTunnel{
		running:   true,
		localPort: backendPort,
		scheme:    "http",
	}

	mockMgr := &retryMockManager{
		tunnels: []tunnelmgr.TunnelHandle{deadTun, freshTun},
	}

	cfg := testHTTPConfig()
	server := NewServer(cfg, mockMgr)

	req := httptest.NewRequest("GET", "/", nil)
	req.Host = "test.localhost:8989"

	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	// The dead tunnel should have been marked as failed
	if !deadTun.markedFailed {
		t.Error("Expected dead tunnel to be marked as failed")
	}

	// Manager should have been called twice (initial + retry)
	if len(mockMgr.getCalls) != 2 {
		t.Errorf("Expected 2 GetOrCreateTunnel calls (initial + retry), got %d", len(mockMgr.getCalls))
	}

	// The retry should have succeeded with 200
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 after retry, got %d", w.Code)
	}

	body := w.Body.String()
	if !strings.Contains(body, "recovered") {
		t.Errorf("Expected body to contain 'recovered', got %q", body)
	}
}

func TestServer_ServeHTTP_RetryFailsGracefully(t *testing.T) {
	// Both the initial and retry tunnels are dead -- should return 502

	deadTun1 := &retryMockTunnel{
		running:   true,
		localPort: 1,
		scheme:    "http",
	}

	deadTun2 := &retryMockTunnel{
		running:   true,
		localPort: 2,
		scheme:    "http",
	}

	mockMgr := &retryMockManager{
		tunnels: []tunnelmgr.TunnelHandle{deadTun1, deadTun2},
	}

	cfg := testHTTPConfig()
	server := NewServer(cfg, mockMgr)

	req := httptest.NewRequest("GET", "/", nil)
	req.Host = "test.localhost:8989"

	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if !deadTun1.markedFailed {
		t.Error("Expected first tunnel to be marked as failed")
	}

	if w.Code != http.StatusBadGateway {
		t.Errorf("Expected status 502 when both tunnels are dead, got %d", w.Code)
	}
}
