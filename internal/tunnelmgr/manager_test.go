package tunnelmgr

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/atas/autotunnel/internal/config"
	"github.com/atas/autotunnel/internal/tunnel"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// mockTunnel implements TunnelHandle for testing
type mockTunnel struct {
	mu           sync.RWMutex
	running      bool
	stopped      bool
	touched      bool
	localPort    int
	scheme       string
	idleDuration time.Duration
	state        tunnel.State
}

func newMockTunnel(running bool) *mockTunnel {
	return &mockTunnel{
		running:   running,
		localPort: 12345,
		scheme:    "http",
		state:     tunnel.StateRunning,
	}
}

func (m *mockTunnel) IsRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.running
}

func (m *mockTunnel) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.running = true
	m.state = tunnel.StateRunning
	return nil
}

func (m *mockTunnel) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.running = false
	m.stopped = true
	m.state = tunnel.StateIdle
}

func (m *mockTunnel) LocalPort() int {
	return m.localPort
}

func (m *mockTunnel) Scheme() string {
	return m.scheme
}

func (m *mockTunnel) Touch() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.touched = true
}

func (m *mockTunnel) IdleDuration() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.idleDuration
}

func (m *mockTunnel) LastError() error {
	return nil
}

func (m *mockTunnel) State() tunnel.State {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state
}

func (m *mockTunnel) Hostname() string {
	return "mock.localhost"
}

func (m *mockTunnel) MarkFailed(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.running = false
	m.state = tunnel.StateFailed
}

func (m *mockTunnel) wasTouched() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.touched
}

func (m *mockTunnel) wasStopped() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.stopped
}

// Helper to create a test config
func testConfig(routes map[string]config.K8sRouteConfig) *config.Config {
	return &config.Config{
		HTTP: config.HTTPConfig{
			ListenAddr:  ":8989",
			IdleTimeout: 60 * time.Minute,
			K8s: config.K8sConfig{
				Routes: routes,
			},
		},
	}
}

func TestGetOrCreateTunnel_UnknownHostname(t *testing.T) {
	cfg := testConfig(map[string]config.K8sRouteConfig{})
	m := NewManager(cfg)

	_, err := m.GetOrCreateTunnel("unknown.localhost", "http")

	if err == nil {
		t.Error("Expected error for unknown hostname")
	}
	if err.Error() != "no route configured for hostname: unknown.localhost" {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestGetOrCreateTunnel_ReturnsExistingRunning(t *testing.T) {
	routes := map[string]config.K8sRouteConfig{
		"test.localhost": {Context: "test", Namespace: "default", Service: "test", Port: 80},
	}
	cfg := testConfig(routes)
	m := NewManager(cfg)

	// Inject a running mock tunnel
	existingTunnel := newMockTunnel(true)
	m.tunnels["test.localhost"] = existingTunnel

	// Act
	result, err := m.GetOrCreateTunnel("test.localhost", "http")

	// Assert
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result != existingTunnel {
		t.Error("Expected to return the existing tunnel")
	}
	if !existingTunnel.wasTouched() {
		t.Error("Expected Touch() to be called on existing tunnel")
	}
}

func TestGetOrCreateTunnel_RemovesFailedTunnel(t *testing.T) {
	routes := map[string]config.K8sRouteConfig{
		"test.localhost": {Context: "test", Namespace: "default", Service: "test", Port: 80},
	}
	cfg := testConfig(routes)
	m := NewManager(cfg)

	// Inject a failed mock tunnel (should be replaced)
	failedTunnel := newMockTunnel(false)
	failedTunnel.state = tunnel.StateFailed // Failed state should trigger replacement
	m.tunnels["test.localhost"] = failedTunnel

	// Set up factory to return a new mock - factory receives nil k8s clients in this test
	factoryCalled := false
	newTunnel := newMockTunnel(false)
	m.tunnelFactory = func(hostname string, cfg config.K8sRouteConfig,
		clientset kubernetes.Interface, restConfig *rest.Config,
		listenAddr string, verbose bool) TunnelHandle {
		factoryCalled = true
		return newTunnel
	}

	// Pre-populate k8s client cache to avoid real k8s client creation
	m.ClientFactory().InjectClient("test", nil, nil)

	// Act
	result, err := m.GetOrCreateTunnel("test.localhost", "http")

	// Assert
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !factoryCalled {
		t.Error("Expected factory to be called for new tunnel")
	}
	if result != newTunnel {
		t.Error("Expected to return a new tunnel, not the stopped one")
	}
	if _, exists := m.tunnels["test.localhost"]; !exists {
		t.Error("Expected new tunnel to be stored in map")
	}
}

func TestActiveTunnels_CountsCorrectly(t *testing.T) {
	cfg := testConfig(map[string]config.K8sRouteConfig{})
	m := NewManager(cfg)

	// No tunnels
	if count := m.ActiveTunnels(); count != 0 {
		t.Errorf("Expected 0 active tunnels, got %d", count)
	}

	// Add running and stopped tunnels
	m.tunnels["running1.localhost"] = newMockTunnel(true)
	m.tunnels["running2.localhost"] = newMockTunnel(true)
	m.tunnels["stopped.localhost"] = newMockTunnel(false)

	if count := m.ActiveTunnels(); count != 2 {
		t.Errorf("Expected 2 active tunnels, got %d", count)
	}
}

func TestListTunnels_ReturnsInfo(t *testing.T) {
	cfg := testConfig(map[string]config.K8sRouteConfig{})
	m := NewManager(cfg)

	mock := newMockTunnel(true)
	mock.localPort = 54321
	mock.idleDuration = 5 * time.Minute
	m.tunnels["test.localhost"] = mock

	infos := m.ListTunnels()

	if len(infos) != 1 {
		t.Fatalf("Expected 1 tunnel info, got %d", len(infos))
	}
	if infos[0].Hostname != "test.localhost" {
		t.Errorf("Expected hostname 'test.localhost', got %q", infos[0].Hostname)
	}
	if infos[0].LocalPort != 54321 {
		t.Errorf("Expected port 54321, got %d", infos[0].LocalPort)
	}
	if infos[0].IdleDuration != 5*time.Minute {
		t.Errorf("Expected idle duration 5m, got %v", infos[0].IdleDuration)
	}
}

func TestCleanupIdleTunnels_StopsIdleTunnels(t *testing.T) {
	routes := map[string]config.K8sRouteConfig{
		"idle.localhost":   {Context: "test", Namespace: "default", Service: "idle", Port: 80},
		"active.localhost": {Context: "test", Namespace: "default", Service: "active", Port: 80},
	}
	cfg := testConfig(routes)
	cfg.HTTP.IdleTimeout = 30 * time.Minute
	m := NewManager(cfg)

	// Idle tunnel (exceeded timeout)
	idleTunnel := newMockTunnel(true)
	idleTunnel.idleDuration = 60 * time.Minute // > 30m timeout

	// Active tunnel (within timeout)
	activeTunnel := newMockTunnel(true)
	activeTunnel.idleDuration = 10 * time.Minute // < 30m timeout

	m.tunnels["idle.localhost"] = idleTunnel
	m.tunnels["active.localhost"] = activeTunnel

	// Act
	m.cleanupIdleTunnels()

	// Assert
	if !idleTunnel.wasStopped() {
		t.Error("Expected idle tunnel to be stopped")
	}
	if activeTunnel.wasStopped() {
		t.Error("Expected active tunnel to NOT be stopped")
	}
	if _, exists := m.tunnels["idle.localhost"]; exists {
		t.Error("Expected idle tunnel to be removed from map")
	}
	if _, exists := m.tunnels["active.localhost"]; !exists {
		t.Error("Expected active tunnel to remain in map")
	}
}

func TestShutdown_StopsAllTunnels(t *testing.T) {
	cfg := testConfig(map[string]config.K8sRouteConfig{})
	m := NewManager(cfg)

	tunnel1 := newMockTunnel(true)
	tunnel2 := newMockTunnel(true)
	tunnel3 := newMockTunnel(false) // not running

	m.tunnels["t1.localhost"] = tunnel1
	m.tunnels["t2.localhost"] = tunnel2
	m.tunnels["t3.localhost"] = tunnel3

	// Start manager (starts the cleanup loop)
	m.Start()

	// Shutdown
	m.Shutdown()

	// Assert running tunnels were stopped
	if !tunnel1.wasStopped() {
		t.Error("Expected tunnel1 to be stopped")
	}
	if !tunnel2.wasStopped() {
		t.Error("Expected tunnel2 to be stopped")
	}
	// tunnel3 was not running, so Stop() might not be called (depends on implementation)

	// Tunnels map should be cleared
	if len(m.tunnels) != 0 {
		t.Errorf("Expected tunnels map to be empty, got %d entries", len(m.tunnels))
	}
}

func TestGetOrCreateTunnel_DynamicRouteResolution(t *testing.T) {
	// Config with dynamic_host but no static routes
	cfg := &config.Config{
		HTTP: config.HTTPConfig{
			ListenAddr:  ":8989",
			IdleTimeout: 60 * time.Minute,
			K8s: config.K8sConfig{
				DynamicHost: "k8s.localhost",
				Routes:      map[string]config.K8sRouteConfig{}, // empty
			},
		},
	}
	m := NewManager(cfg)

	// Set up factory to capture what config was used
	var capturedConfig config.K8sRouteConfig
	m.tunnelFactory = func(hostname string, cfg config.K8sRouteConfig,
		clientset kubernetes.Interface, restConfig *rest.Config,
		listenAddr string, verbose bool) TunnelHandle {
		capturedConfig = cfg
		return newMockTunnel(false)
	}

	// Pre-populate k8s client cache
	m.ClientFactory().InjectClient("microk8s", nil, nil)

	// Act - use dynamic hostname format
	_, err := m.GetOrCreateTunnel("nginx-80.svc.default.ns.microk8s.cx.k8s.localhost", "http")

	// Assert
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if capturedConfig.Service != "nginx" {
		t.Errorf("Expected service 'nginx', got %q", capturedConfig.Service)
	}
	if capturedConfig.Port != 80 {
		t.Errorf("Expected port 80, got %d", capturedConfig.Port)
	}
	if capturedConfig.Namespace != "default" {
		t.Errorf("Expected namespace 'default', got %q", capturedConfig.Namespace)
	}
	if capturedConfig.Context != "microk8s" {
		t.Errorf("Expected context 'microk8s', got %q", capturedConfig.Context)
	}
}

func TestManager_ConcurrentGetOrCreateTunnel(t *testing.T) {
	routes := map[string]config.K8sRouteConfig{
		"test1.localhost": {Context: "test", Namespace: "default", Service: "test1", Port: 80},
		"test2.localhost": {Context: "test", Namespace: "default", Service: "test2", Port: 80},
	}
	cfg := testConfig(routes)
	m := NewManager(cfg)

	// Pre-populate k8s client cache
	m.ClientFactory().InjectClient("test", nil, nil)

	// Set up factory
	m.tunnelFactory = func(hostname string, cfg config.K8sRouteConfig,
		clientset kubernetes.Interface, restConfig *rest.Config,
		listenAddr string, verbose bool) TunnelHandle {
		return newMockTunnel(false)
	}

	var wg sync.WaitGroup
	const goroutines = 50

	// Concurrent access should be safe
	for range goroutines {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = m.GetOrCreateTunnel("test1.localhost", "http")
		}()
		go func() {
			defer wg.Done()
			_, _ = m.GetOrCreateTunnel("test2.localhost", "http")
		}()
	}

	wg.Wait()
}

func TestManager_CleanupIdleTunnels_NoTunnels(t *testing.T) {
	cfg := testConfig(map[string]config.K8sRouteConfig{})
	m := NewManager(cfg)

	// Should not panic with empty tunnels map
	m.cleanupIdleTunnels()

	if len(m.tunnels) != 0 {
		t.Errorf("Expected 0 tunnels, got %d", len(m.tunnels))
	}
}

func TestManager_CleanupIdleTCPTunnels(t *testing.T) {
	cfg := &config.Config{
		HTTP: config.HTTPConfig{
			IdleTimeout: 30 * time.Minute,
		},
		TCP: config.TCPConfig{
			IdleTimeout: 0, // Should fall back to HTTP idle timeout
			K8s: config.TCPK8sConfig{
				Routes: map[int]config.TCPRouteConfig{
					5432: {Context: "test", Namespace: "default", Service: "postgres", Port: 5432},
				},
			},
		},
	}
	m := NewManager(cfg)

	// Add idle TCP tunnel
	idleTunnel := newMockTunnel(true)
	idleTunnel.idleDuration = 60 * time.Minute // > 30m timeout (uses HTTP fallback)
	m.tcpTunnels[5432] = idleTunnel

	// Active TCP tunnel
	activeTunnel := newMockTunnel(true)
	activeTunnel.idleDuration = 10 * time.Minute
	m.tcpTunnels[3306] = activeTunnel

	// Act
	m.cleanupIdleTunnels()

	// Idle tunnel should be stopped
	if !idleTunnel.wasStopped() {
		t.Error("Expected idle TCP tunnel to be stopped")
	}
	if activeTunnel.wasStopped() {
		t.Error("Expected active TCP tunnel to NOT be stopped")
	}
}

func TestManager_ListTunnels_Empty(t *testing.T) {
	cfg := testConfig(map[string]config.K8sRouteConfig{})
	m := NewManager(cfg)

	infos := m.ListTunnels()

	if len(infos) != 0 {
		t.Errorf("Expected 0 tunnel infos, got %d", len(infos))
	}
}

func TestManager_ActiveTunnels_MixedStates(t *testing.T) {
	cfg := testConfig(map[string]config.K8sRouteConfig{})
	m := NewManager(cfg)

	// Add tunnels in various states
	runningTunnel := newMockTunnel(true)
	stoppedTunnel := newMockTunnel(false)

	m.tunnels["running.localhost"] = runningTunnel
	m.tunnels["stopped.localhost"] = stoppedTunnel

	count := m.ActiveTunnels()

	if count != 1 {
		t.Errorf("Expected 1 active tunnel, got %d", count)
	}
}

