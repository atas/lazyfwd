package httpserver

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/atas/autotunnel/internal/tunnelmgr"
)

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	host := r.Host
	if idx := strings.LastIndex(host, ":"); idx != -1 {
		host = host[:idx]
	}

	if s.config.Verbose {
		log.Printf("[http] [%s] %s %s", host, r.Method, r.URL.Path)
	}

	tunnel, err := s.manager.GetOrCreateTunnel(host, "http")
	if err != nil {
		log.Printf("[http] [%s] Error: %v", host, err)
		http.Error(w, fmt.Sprintf("No service configured for host: %s", host), http.StatusBadGateway)
		return
	}

	if !tunnel.IsRunning() {
		if err := tunnel.Start(r.Context()); err != nil {
			log.Printf("[http] [%s] Failed to start tunnel: %v", host, err)
			http.Error(w, fmt.Sprintf("Failed to start tunnel: %v", err), http.StatusBadGateway)
			return
		}
	}

	tunnel.Touch()

	proxyErr := s.proxyRequest(w, r, host, tunnel)
	if proxyErr == nil {
		return
	}

	// Proxy failed -- the port-forward connection is likely dead.
	// Mark the tunnel as failed so GetOrCreateTunnel creates a fresh one,
	// then retry this request once with the new tunnel.
	tunnel.MarkFailed(proxyErr)
	log.Printf("[http] [%s] Retrying with fresh tunnel", host)

	tunnel, err = s.manager.GetOrCreateTunnel(host, "http")
	if err != nil {
		log.Printf("[http] [%s] Retry error: %v", host, err)
		http.Error(w, fmt.Sprintf("Proxy error for host '%s': %v", host, proxyErr), http.StatusBadGateway)
		return
	}

	if !tunnel.IsRunning() {
		if err := tunnel.Start(r.Context()); err != nil {
			log.Printf("[http] [%s] Retry failed to start tunnel: %v", host, err)
			http.Error(w, fmt.Sprintf("Failed to start tunnel: %v", err), http.StatusBadGateway)
			return
		}
	}

	tunnel.Touch()

	if retryErr := s.proxyRequest(w, r, host, tunnel); retryErr != nil {
		// Retry also failed, send error to client
		log.Printf("[http] [%s] Retry proxy error: %v", host, retryErr)
		http.Error(w, fmt.Sprintf("Proxy error for host '%s': %v", host, retryErr), http.StatusBadGateway)
	}
}

// proxyRequest forwards the HTTP request to the tunnel's local port.
// Returns a non-nil error if the proxy encountered a backend connection error
// before any response bytes were written to the client (allowing the caller to retry).
// Returns nil when the response was successfully proxied.
func (s *Server) proxyRequest(w http.ResponseWriter, r *http.Request, host string, tun tunnelmgr.TunnelHandle) error {
	scheme := tun.Scheme()
	targetURL := &url.URL{
		Scheme: scheme,
		Host:   fmt.Sprintf("127.0.0.1:%d", tun.LocalPort()),
	}

	proxy := httputil.NewSingleHostReverseProxy(targetURL)

	if scheme == "https" {
		proxy.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	}

	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = r.Host
		req.Header.Set("X-Forwarded-Proto", scheme)
		req.Header.Set("X-Forwarded-Host", r.Host)
		if r.RemoteAddr != "" {
			req.Header.Set("X-Forwarded-For", strings.Split(r.RemoteAddr, ":")[0])
		}
	}

	var proxyErr error
	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		if err == context.Canceled || strings.Contains(err.Error(), "context canceled") {
			return
		}
		log.Printf("[http] [%s] Proxy error: %v", host, err)
		proxyErr = err
		// Don't write an HTTP error response here -- let the caller decide
		// whether to retry or report the error.
	}

	proxy.ServeHTTP(w, r)

	return proxyErr
}
