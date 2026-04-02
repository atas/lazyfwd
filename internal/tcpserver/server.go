package tcpserver

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/atas/autotunnel/internal/config"
	"github.com/atas/autotunnel/internal/netutil"
)

type listenerType int

const (
	listenerTypeRoute listenerType = iota // direct port-forward
	listenerTypeJump                      // jump-host via exec+socat/nc
)

type Server struct {
	config  *config.Config
	manager Manager
	verbose bool

	mu        sync.RWMutex
	listeners map[int]*portListener

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type portListener struct {
	port         int
	listenerType listenerType
	listener     net.Listener
	stopChan     chan struct{}
}

func NewServer(cfg *config.Config, mgr Manager) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		config:    cfg,
		manager:   mgr,
		verbose:   cfg.Verbose,
		listeners: make(map[int]*portListener),
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (s *Server) Start() error {
	for port := range s.config.TCP.K8s.Routes {
		if err := s.startListener(port, listenerTypeRoute); err != nil {
			s.Shutdown()
			return err
		}
	}

	for port := range s.config.TCP.K8s.Jump {
		if err := s.startListener(port, listenerTypeJump); err != nil {
			s.Shutdown()
			return err
		}
	}

	return nil
}

func (s *Server) startListener(port int, lt listenerType) error {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on TCP port %d: %w", port, err)
	}

	pl := &portListener{
		port:         port,
		listenerType: lt,
		listener:     listener,
		stopChan:     make(chan struct{}),
	}

	s.mu.Lock()
	s.listeners[port] = pl
	s.mu.Unlock()

	s.wg.Add(1)
	go s.acceptLoop(pl)

	var destStr string
	if lt == listenerTypeJump {
		jumpCfg := s.config.TCP.K8s.Jump[port]
		destStr = fmt.Sprintf("-> %s:%d via %s/%s (jump)",
			jumpCfg.Target.Host, jumpCfg.Target.Port, jumpCfg.Namespace, jumpCfg.Via.TargetDisplay())
	} else {
		routeCfg := s.config.TCP.K8s.Routes[port]
		destStr = fmt.Sprintf("-> %s/%s:%d", routeCfg.Namespace, routeCfg.TargetDisplay(), routeCfg.Port)
	}
	log.Printf("TCP listener started on %s %s", addr, destStr)
	return nil
}

func (s *Server) acceptLoop(pl *portListener) {
	defer s.wg.Done()

	for {
		conn, err := pl.listener.Accept()
		if err != nil {
			select {
			case <-pl.stopChan:
				return
			case <-s.ctx.Done():
				return
			default:
				if s.verbose {
					log.Printf("[tcp:%d] Accept error: %v", pl.port, err)
				}
				continue
			}
		}

		if pl.listenerType == listenerTypeJump {
			go s.handleJumpConnection(pl.port, conn)
		} else {
			go s.handleConnection(pl.port, conn)
		}
	}
}

func (s *Server) handleConnection(localPort int, conn net.Conn) {
	defer conn.Close()

	// Get or create tunnel for this port
	tunnel, err := s.manager.GetOrCreateTCPTunnel(localPort)
	if err != nil {
		log.Printf("[tcp:%d] Failed to get tunnel: %v", localPort, err)
		return
	}

	// Ensure tunnel is started
	if !tunnel.IsRunning() {
		ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
		if err := tunnel.Start(ctx); err != nil {
			cancel()
			log.Printf("[tcp:%d] Failed to start tunnel: %v", localPort, err)
			return
		}
		cancel()
	}

	// Connect to tunnel's local port
	backendAddr := fmt.Sprintf("127.0.0.1:%d", tunnel.LocalPort())
	backend, err := net.DialTimeout("tcp", backendAddr, 10*time.Second)
	if err != nil {
		log.Printf("[tcp:%d] Failed to connect to backend %s: %v", localPort, backendAddr, err)
		tunnel.MarkFailed(fmt.Errorf("backend dial failed: %w", err))
		return
	}
	defer backend.Close()

	tunnel.Touch()

	if s.verbose {
		log.Printf("[tcp:%d] Connection established -> backend port %d", localPort, tunnel.LocalPort())
	}

	netutil.BidirectionalCopy(backend, conn)

	if s.verbose {
		log.Printf("[tcp:%d] Connection closed", localPort)
	}
}

func (s *Server) handleJumpConnection(localPort int, conn net.Conn) {
	defer conn.Close()

	s.mu.RLock()
	route, exists := s.config.TCP.K8s.Jump[localPort]
	kubeconfigs := s.config.TCP.K8s.ResolvedKubeconfigs
	s.mu.RUnlock()

	if !exists {
		log.Printf("[jump:%d] No route configured", localPort)
		return
	}

	clientset, restConfig, err := s.manager.GetClientForContext(kubeconfigs, route.Context)
	if err != nil {
		log.Printf("[jump:%d] Failed to get K8s client: %v", localPort, err)
		return
	}

	handler := NewJumpHandler(route, kubeconfigs, clientset, restConfig, s.verbose)
	if err := handler.HandleConnection(s.ctx, conn, localPort); err != nil {
		log.Printf("[jump:%d] Connection error: %v", localPort, err)
	}
}

func (s *Server) Shutdown() {
	s.cancel()

	s.mu.Lock()
	for port, pl := range s.listeners {
		close(pl.stopChan)
		pl.listener.Close()
		log.Printf("TCP listener stopped on port %d", port)
	}
	s.listeners = make(map[int]*portListener)
	s.mu.Unlock()

	s.wg.Wait()
}

