package tunnel

import (
	"sync"
	"testing"
	"time"

	"github.com/atas/autotunnel/internal/config"
)

func TestTunnel_LocalPort(t *testing.T) {
	tunnel := &Tunnel{localPort: 12345}

	if got := tunnel.LocalPort(); got != 12345 {
		t.Errorf("LocalPort() = %d, want %d", got, 12345)
	}
}

func TestTunnel_Hostname(t *testing.T) {
	tunnel := &Tunnel{hostname: "grafana.localhost"}

	if got := tunnel.Hostname(); got != "grafana.localhost" {
		t.Errorf("Hostname() = %q, want %q", got, "grafana.localhost")
	}
}

func TestTunnel_Scheme(t *testing.T) {
	tests := []struct {
		name     string
		scheme   string
		expected string
	}{
		{"empty defaults to http", "", "http"},
		{"explicit http", "http", "http"},
		{"explicit https", "https", "https"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tunnel := &Tunnel{
				config: config.K8sRouteConfig{Scheme: tt.scheme},
			}
			if got := tunnel.Scheme(); got != tt.expected {
				t.Errorf("Scheme() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestTunnel_Touch_IdleDuration(t *testing.T) {
	tunnel := &Tunnel{lastAccess: time.Now().Add(-1 * time.Hour)}

	// Verify it's been idle for approximately 1 hour
	idle := tunnel.IdleDuration()
	if idle < 59*time.Minute || idle > 61*time.Minute {
		t.Errorf("IdleDuration() = %v, expected approximately 1 hour", idle)
	}

	// Touch should reset the idle duration
	tunnel.Touch()

	idle = tunnel.IdleDuration()
	if idle > 100*time.Millisecond {
		t.Errorf("After Touch(), IdleDuration() = %v, expected < 100ms", idle)
	}
}

func TestTunnel_LastError(t *testing.T) {
	tunnel := &Tunnel{}

	// Initially nil
	if err := tunnel.LastError(); err != nil {
		t.Errorf("LastError() = %v, want nil", err)
	}

	// Set an error
	testErr := &testError{msg: "test error"}
	tunnel.lastError = testErr

	if err := tunnel.LastError(); err != testErr {
		t.Errorf("LastError() = %v, want %v", err, testErr)
	}
}

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

func TestTunnel_ConcurrentTouchCalls(t *testing.T) {
	tunnel := NewTunnel("test.localhost", config.K8sRouteConfig{}, nil, nil, ":8989", false)

	var wg sync.WaitGroup
	const goroutines = 100

	// Concurrent Touch and IdleDuration calls
	for range goroutines {
		wg.Add(2)
		go func() {
			defer wg.Done()
			tunnel.Touch()
		}()
		go func() {
			defer wg.Done()
			_ = tunnel.IdleDuration()
		}()
	}

	wg.Wait()

	// After all touches, idle duration should be very small
	idle := tunnel.IdleDuration()
	if idle > 100*time.Millisecond {
		t.Errorf("After concurrent touches, IdleDuration() = %v, expected < 100ms", idle)
	}
}

func TestTunnel_ConcurrentLocalPortAccess(t *testing.T) {
	tunnel := &Tunnel{localPort: 8080}

	var wg sync.WaitGroup
	const goroutines = 100

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			port := tunnel.LocalPort()
			if port != 8080 {
				t.Errorf("LocalPort() = %d, want 8080", port)
			}
		}()
	}

	wg.Wait()
}

func TestTunnel_MarkFailed_FromRunning(t *testing.T) {
	stopChan := make(chan struct{})
	tun := &Tunnel{
		hostname: "test.localhost",
		state:    StateRunning,
		stopChan: stopChan,
	}

	testErr := &testError{msg: "connection reset by peer"}
	tun.MarkFailed(testErr)

	if tun.state != StateFailed {
		t.Errorf("state = %v, want %v", tun.state, StateFailed)
	}
	if tun.lastError != testErr {
		t.Errorf("lastError = %v, want %v", tun.lastError, testErr)
	}

	// stopChan should have been closed
	select {
	case <-stopChan:
		// expected
	default:
		t.Error("stopChan should have been closed")
	}
}

func TestTunnel_MarkFailed_FromIdle(t *testing.T) {
	tun := &Tunnel{
		hostname: "test.localhost",
		state:    StateIdle,
	}

	tun.MarkFailed(&testError{msg: "should not change state"})

	// Idle tunnels should not be marked failed (they're already stopped)
	if tun.state != StateIdle {
		t.Errorf("state = %v, want %v (should remain idle)", tun.state, StateIdle)
	}
}

func TestTunnel_MarkFailed_FromFailed(t *testing.T) {
	tun := &Tunnel{
		hostname: "test.localhost",
		state:    StateFailed,
	}

	tun.MarkFailed(&testError{msg: "second error"})

	// Already failed -- should remain failed but not panic
	if tun.state != StateFailed {
		t.Errorf("state = %v, want %v", tun.state, StateFailed)
	}
}

func TestTunnel_MarkFailed_NilStopChan(t *testing.T) {
	tun := &Tunnel{
		hostname: "test.localhost",
		state:    StateRunning,
		stopChan: nil,
	}

	// Should not panic with nil stopChan
	tun.MarkFailed(&testError{msg: "test"})

	if tun.state != StateFailed {
		t.Errorf("state = %v, want %v", tun.state, StateFailed)
	}
}
