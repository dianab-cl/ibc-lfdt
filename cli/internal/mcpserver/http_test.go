// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

func TestListenHTTP(t *testing.T) {
	srv, err := New(&fakeChain{}, &fakeRelayer{}, Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })

	h, err := srv.ListenHTTP("127.0.0.1:0")
	require.NoError(t, err)
	require.NotNil(t, h.server.ErrorLog, "panics recovered by net/http go to the relayer logger")

	ctx := context.Background()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).
		Connect(ctx, &mcp.StreamableClientTransport{Endpoint: h.URL()}, nil)
	require.NoError(t, err)

	res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "ibc_get_context"})
	require.NoError(t, err)
	require.False(t, res.IsError)
	require.NoError(t, cs.Close())

	require.NoError(t, h.Stop())
	_, err = mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).
		Connect(ctx, &mcp.StreamableClientTransport{Endpoint: h.URL(), MaxRetries: -1}, nil)
	require.Error(t, err, "stopped server refuses connections")
}

func TestStopCancelsInFlightWait(t *testing.T) {
	rel := &fakeRelayer{}
	started := make(chan struct{})
	var once sync.Once
	rel.onPackets = func(ctx context.Context) {
		once.Do(func() { close(started) })
		<-ctx.Done()
	}
	srv, err := New(&fakeChain{}, rel, Options{PollInterval: time.Hour})
	require.NoError(t, err)
	h, err := srv.ListenHTTP("127.0.0.1:0")
	require.NoError(t, err)

	ctx := context.Background()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).
		Connect(ctx, &mcp.StreamableClientTransport{Endpoint: h.URL()}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = cs.CallTool(ctx, &mcp.CallToolParams{
			Name: "ibc_wait_for_packets",
			Arguments: map[string]any{
				"chain_id":        "1",
				"tx_hash":         txHash,
				"timeout_seconds": 60,
			},
		})
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("wait did not reach the relayer")
	}

	err = h.Stop()
	// A connected client's event stream stays active until Shutdown's deadline,
	// then Stop closes it. The tool call must already have been canceled.
	if err != nil {
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight wait still running after stop")
	}
	_, err = mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).
		Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: h.URL(), MaxRetries: -1}, nil)
	require.Error(t, err, "stopped server refuses connections")
}

func TestStartRelayerMCPHTTPPanicOnProcessLog(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	_, stop := context.WithCancel(context.Background())
	const marker = "mcp-http-panic-for-container-log"
	h := startHTTP(ln, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(marker)
	}), stop, slog.Default())
	t.Cleanup(func() { _ = h.Stop() })

	// The recovered panic closes the connection. The client error is not the signal.
	resp, _ := http.Get("http://" + ln.Addr().String())
	if resp != nil {
		resp.Body.Close()
	}
	require.Contains(t, logs.String(), "http: panic serving")
	require.Contains(t, logs.String(), marker)
}

func TestStartRelayerMCPSessionLogOnProcessLog(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	srv, err := New(&fakeChain{}, &fakeRelayer{}, Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })
	h, err := srv.ListenHTTP("127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Stop() })

	ctx := context.Background()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).
		Connect(ctx, &mcp.StreamableClientTransport{Endpoint: h.URL()}, nil)
	require.NoError(t, err)
	require.NoError(t, cs.Close())

	require.Contains(t, logs.String(), "server session connected")
}

func TestRequireLoopback(t *testing.T) {
	for _, ok := range []string{"127.0.0.1:3001", "localhost:3001", "[::1]:3001"} {
		require.NoError(t, RequireLoopback(ok), ok)
	}
	for _, bad := range []string{"0.0.0.0:3001", ":3001", "10.0.0.5:3001", "relayer.internal:3001", "127.0.0.1"} {
		require.Error(t, RequireLoopback(bad), bad)
	}

	srv, err := New(&fakeChain{}, &fakeRelayer{}, Options{})
	require.NoError(t, err)
	_, err = srv.ListenHTTP("0.0.0.0:0")
	require.ErrorContains(t, err, "not loopback")
}
