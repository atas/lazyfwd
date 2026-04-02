package tunnel

import (
	"log"
	"time"
)

func (t *Tunnel) LocalPort() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.localPort
}

func (t *Tunnel) Touch() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastAccess = time.Now()
}

func (t *Tunnel) IdleDuration() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return time.Since(t.lastAccess)
}

func (t *Tunnel) Hostname() string {
	return t.hostname
}

func (t *Tunnel) Scheme() string {
	if t.config.Scheme == "" {
		return "http"
	}
	return t.config.Scheme
}

func (t *Tunnel) LastError() error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lastError
}

func (t *Tunnel) MarkFailed(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == StateRunning || t.state == StateStarting {
		log.Printf("[%s] Marking tunnel as failed: %v", t.hostname, err)
		t.lastError = err
		t.state = StateFailed
		if t.stopChan != nil {
			close(t.stopChan)
		}
	}
}
