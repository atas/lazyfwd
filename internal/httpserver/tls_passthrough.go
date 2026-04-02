package httpserver

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/atas/autotunnel/internal/netutil"
)

func (s *Server) handleTLSConnection(conn *peekConn) {
	defer conn.Close()

	// give slow clients time to send ClientHello
	_ = conn.Conn.SetReadDeadline(time.Now().Add(TLSClientHelloDeadline))

	// Read enough for ClientHello
	buf := make([]byte, 16384)
	n, err := conn.Read(buf)
	if err != nil {
		if s.config.Verbose {
			log.Printf("[tls] Error reading ClientHello: %v", err)
		}
		return
	}

	_ = conn.Conn.SetReadDeadline(time.Time{})

	sni, err := extractSNI(buf[:n])
	if err != nil {
		log.Printf("[tls] Failed to extract SNI: %v", err)
		s.sendTLSErrorPage(conn.Conn, buf[:n], "", tlsErrorSNIExtraction, fmt.Sprintf("Failed to extract SNI: %v", err))
		return
	}

	if s.config.Verbose {
		log.Printf("[tls] [%s] New connection", sni)
	}

	tunnel, err := s.manager.GetOrCreateTunnel(sni, "https")
	if err != nil {
		log.Printf("[tls] [%s] Error: %v", sni, err)
		s.sendTLSErrorPage(conn.Conn, buf[:n], sni, tlsErrorRouteNotFound, fmt.Sprintf("No service configured for host: %s", sni))
		return
	}

	if !tunnel.IsRunning() {
		ctx, cancel := context.WithTimeout(context.Background(), TLSTunnelStartTimeout)
		if err := tunnel.Start(ctx); err != nil {
			cancel()
			log.Printf("[tls] [%s] Failed to start tunnel: %v", sni, err)
			s.sendTLSErrorPage(conn.Conn, buf[:n], sni, tlsErrorTunnelStartup, fmt.Sprintf("Failed to start tunnel: %v", err))
			return
		}
		cancel()
	}

	tunnel.Touch()

	backendAddr := fmt.Sprintf("127.0.0.1:%d", tunnel.LocalPort())
	backendConn, err := net.DialTimeout("tcp", backendAddr, TLSBackendDialTimeout)
	if err != nil {
		log.Printf("[tls] [%s] Failed to connect to backend: %v", sni, err)
		// Port-forward is likely dead -- mark tunnel failed so next connection recreates it
		tunnel.MarkFailed(fmt.Errorf("backend dial failed: %w", err))
		s.sendTLSErrorPage(conn.Conn, buf[:n], sni, tlsErrorBackendConnection, fmt.Sprintf("Failed to connect to backend: %v", err))
		return
	}
	defer backendConn.Close()

	// replay the ClientHello we already read - backend hasn't seen it yet
	if _, err := backendConn.Write(buf[:n]); err != nil {
		log.Printf("[tls] [%s] Failed to forward ClientHello: %v", sni, err)
		s.sendTLSErrorPage(conn.Conn, buf[:n], sni, tlsErrorForwarding, fmt.Sprintf("Failed to forward ClientHello: %v", err))
		return
	}

	netutil.BidirectionalCopy(backendConn, conn.Conn)
}

// extractSNI parses the TLS ClientHello to find the Server Name Indication.
// This is how we know which backend to route to before TLS terminates.
func extractSNI(data []byte) (string, error) {
	handshake, err := validateTLSHandshake(data)
	if err != nil {
		return "", err
	}

	extStart, extLen, err := skipToExtensions(handshake)
	if err != nil {
		return "", err
	}

	return findSNIInExtensions(handshake[extStart:], extLen)
}

// validateTLSHandshake validates TLS record and ClientHello headers, returning handshake data
func validateTLSHandshake(data []byte) ([]byte, error) {
	// TLS record: type(1) + version(2) + length(2) = 5 bytes minimum
	if len(data) < 5 {
		return nil, fmt.Errorf("data too short for TLS record")
	}

	// Check if it's a TLS handshake (0x16)
	if data[0] != 0x16 {
		return nil, fmt.Errorf("not a TLS handshake record")
	}

	recordLen := int(data[3])<<8 | int(data[4])
	if len(data) < 5+recordLen {
		return nil, fmt.Errorf("incomplete TLS record")
	}

	handshake := data[5:]
	if len(handshake) < 4 {
		return nil, fmt.Errorf("data too short for handshake header")
	}

	// Check if it's a ClientHello (0x01)
	if handshake[0] != 0x01 {
		return nil, fmt.Errorf("not a ClientHello")
	}

	return handshake, nil
}

// skipToExtensions skips the variable-length fields in ClientHello to find extensions
// Returns the offset to extensions start and total extensions length
func skipToExtensions(handshake []byte) (int, int, error) {
	// skip: handshake header(4) + version(2) + random(32) = 38 bytes
	pos := 38
	if len(handshake) < pos+1 {
		return 0, 0, fmt.Errorf("data too short for session ID length")
	}

	sessionIDLen := int(handshake[pos])
	pos += 1 + sessionIDLen
	if len(handshake) < pos+2 {
		return 0, 0, fmt.Errorf("data too short for cipher suites length")
	}

	cipherSuitesLen := int(handshake[pos])<<8 | int(handshake[pos+1])
	pos += 2 + cipherSuitesLen
	if len(handshake) < pos+1 {
		return 0, 0, fmt.Errorf("data too short for compression methods length")
	}

	compressionLen := int(handshake[pos])
	pos += 1 + compressionLen
	if len(handshake) < pos+2 {
		return 0, 0, fmt.Errorf("no extensions present")
	}

	extensionsLen := int(handshake[pos])<<8 | int(handshake[pos+1])
	return pos + 2, extensionsLen, nil
}

// findSNIInExtensions scans TLS extensions to find and parse the SNI extension
func findSNIInExtensions(extensions []byte, extLen int) (string, error) {
	pos := 0
	for pos+4 <= extLen && pos+4 <= len(extensions) {
		extType := int(extensions[pos])<<8 | int(extensions[pos+1])
		thisExtLen := int(extensions[pos+2])<<8 | int(extensions[pos+3])
		pos += 4

		if pos+thisExtLen > len(extensions) {
			break
		}

		// SNI is extension type 0x0000
		if extType == 0 {
			extData := extensions[pos : pos+thisExtLen]
			if len(extData) < 5 {
				return "", fmt.Errorf("SNI extension too short")
			}
			// skip list length(2) + host type(1)
			nameLen := int(extData[3])<<8 | int(extData[4])
			if len(extData) < 5+nameLen {
				return "", fmt.Errorf("SNI name truncated")
			}
			return string(extData[5 : 5+nameLen]), nil
		}

		pos += thisExtLen
	}

	return "", fmt.Errorf("SNI extension not found")
}
