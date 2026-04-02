package tunnelmgr

import (
	"context"
	"time"

	"github.com/atas/autotunnel/internal/tunnel"
)

type TunnelHandle interface {
	IsRunning() bool
	Start(ctx context.Context) error
	Stop()
	LocalPort() int
	Scheme() string
	Touch()
	IdleDuration() time.Duration
	State() tunnel.State
	LastError() error
	Hostname() string
	MarkFailed(err error)
}

type TunnelInfo struct {
	Hostname     string
	LocalPort    int
	State        string
	IdleDuration time.Duration
}

func (m *Manager) ActiveTunnels() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	for _, tunnel := range m.tunnels {
		if tunnel.IsRunning() {
			count++
		}
	}
	return count
}

func (m *Manager) ListTunnels() []TunnelInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	infos := make([]TunnelInfo, 0, len(m.tunnels))
	for hostname, tunnel := range m.tunnels {
		infos = append(infos, TunnelInfo{
			Hostname:     hostname,
			LocalPort:    tunnel.LocalPort(),
			State:        tunnel.State().String(),
			IdleDuration: tunnel.IdleDuration(),
		})
	}
	return infos
}
