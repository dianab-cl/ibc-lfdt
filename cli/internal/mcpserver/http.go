// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	pkgerrors "github.com/pkg/errors"
)

const (
	httpReadHeaderTimeout = 10 * time.Second
	httpSessionTimeout    = 30 * time.Minute
	httpShutdownTimeout   = 5 * time.Second
)

// HTTPServer serves a Server over MCP streamable HTTP on its own listener,
// separate from the relayer's gRPC server: MCP holds requests open while it
// waits for packets, which the relayer server's short read timeouts would cut.
type HTTPServer struct {
	server   *http.Server
	listener net.Listener
	stop     context.CancelFunc
	done     chan struct{}
}

// ListenHTTP binds addr and serves in the background. Binding happens before
// it returns, so a taken port fails the caller's startup. The endpoint has no
// authentication and can sign transactions, so only loopback addresses are
// accepted.
func (s *Server) ListenHTTP(addr string) (*HTTPServer, error) {
	if err := RequireLoopback(addr); err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, pkgerrors.Wrapf(err, "MCP listen on %s", addr)
	}
	if err := requireBoundLoopback(listener.Addr()); err != nil {
		_ = listener.Close()
		return nil, err
	}

	logger := slog.With("module", "mcp")
	handler := mcp.NewStreamableHTTPHandler(
		func(*http.Request) *mcp.Server { return s.mcp },
		&mcp.StreamableHTTPOptions{Logger: logger, SessionTimeout: httpSessionTimeout},
	)
	return startHTTP(listener, handler, s.stop, logger), nil
}

// startHTTP serves handler on listener. ErrorLog is the relayer logger.
// A nil ErrorLog sends panics recovered by net/http, and accept failures,
// through the standard log package instead of that logger.
func startHTTP(listener net.Listener, handler http.Handler, stop context.CancelFunc, logger *slog.Logger) *HTTPServer {
	h := &HTTPServer{
		server: &http.Server{
			Handler:           handler,
			ReadHeaderTimeout: httpReadHeaderTimeout,
			ErrorLog:          processErrorLog(logger),
		},
		listener: listener,
		stop:     stop,
		done:     make(chan struct{}),
	}
	go func() {
		defer close(h.done)
		if err := h.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("MCP server stopped", "err", err)
		}
	}()
	return h
}

// Addr is the bound address, useful when listening on port 0.
func (h *HTTPServer) Addr() net.Addr { return h.listener.Addr() }

// URL is the endpoint MCP clients connect to.
func (h *HTTPServer) URL() string { return "http://" + h.Addr().String() }

// Stop cancels in-flight tool calls and shuts the listener down.
func (h *HTTPServer) Stop() error {
	h.stop()
	ctx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
	defer cancel()
	err := h.server.Shutdown(ctx)
	if h.waitServe(ctx) {
		return err
	}
	// A handler that ignored cancellation still holds the connection.
	// Close drops it so Serve can return.
	_ = h.server.Close()
	wait, waitCancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
	defer waitCancel()
	h.waitServe(wait)
	return err
}

func (h *HTTPServer) waitServe(ctx context.Context) bool {
	select {
	case <-h.done:
		return true
	case <-ctx.Done():
		return false
	}
}

// RequireLoopback refuses any address that is not host:port on a loopback host.
func RequireLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return pkgerrors.Wrapf(err, "invalid MCP listen address %q", addr)
	}
	if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return pkgerrors.Errorf(
			"MCP listen address %q is not loopback; the endpoint has no authentication", addr)
	}
	return nil
}

// requireBoundLoopback checks the address the kernel actually bound. A name
// such as localhost is only accepted when it resolves to a loopback IP.
func requireBoundLoopback(addr net.Addr) error {
	tcp, ok := addr.(*net.TCPAddr)
	if !ok || tcp == nil || tcp.IP == nil || !tcp.IP.IsLoopback() {
		return pkgerrors.Errorf(
			"MCP listen address %q is not loopback; the endpoint has no authentication", addr)
	}
	return nil
}
