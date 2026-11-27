// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
)

const (
	chainA   = "1"
	chainB   = "8453"
	tokenA   = "0x00000000000000000000000000000000000000aA"
	agentKey = "agent-hot-key"
	agentHex = "0x1111111111111111111111111111111111111111"
	bobHex   = "0x2222222222222222222222222222222222222222"
	txHash   = "0x" + "ab" + "000000000000000000000000000000000000000000000000000000000000cd"
)

// --- fakes ---

type fakeChain struct {
	mu        sync.Mutex
	sends     []SendRequest
	sendErr   error
	autoRelay bool
}

func (f *fakeChain) Chains(context.Context) ([]ChainInfo, error) {
	return []ChainInfo{
		{ChainID: chainA, Type: "evm"},
		{ChainID: chainB, Type: "evm"},
		{ChainID: "999", Type: "evm"},
	}, nil
}

func (f *fakeChain) Connections(context.Context) ([]ConnectionInfo, error) {
	return []ConnectionInfo{{
		Alias:   "eth-base",
		ClientA: ClientEndInfo{ChainID: chainA, ClientID: "base-0", Signer: agentKey, AutoRelay: f.autoRelay},
		ClientB: ClientEndInfo{ChainID: chainB, ClientID: "ethereum-0", Signer: "relayer-key", AutoRelay: f.autoRelay},
	}}, nil
}

func (f *fakeChain) Signers(context.Context) ([]SignerInfo, error) {
	return []SignerInfo{{Alias: agentKey, Address: agentHex}, {Alias: "relayer-key", Address: bobHex}}, nil
}

func (f *fakeChain) ResolveAddress(_ context.Context, v string) (string, error) {
	switch {
	case v == agentKey:
		return agentHex, nil
	case strings.HasPrefix(v, "0x") && len(v) == 42:
		return v, nil
	default:
		return "", NewError(CodeInvalidArgument, "invalid address %q", v)
	}
}

func (f *fakeChain) TokenInfo(context.Context, string, string) (TokenInfo, error) {
	return TokenInfo{Symbol: "USDX", Decimals: 6}, nil
}

func (f *fakeChain) IFTBalance(context.Context, string, string, string) (*big.Int, error) {
	return big.NewInt(125_000_000), nil
}

func (f *fakeChain) IFTSend(_ context.Context, req SendRequest) (TxRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sends = append(f.sends, req)
	if f.sendErr != nil {
		return TxRef{}, f.sendErr
	}
	return TxRef{ChainID: req.ChainID, TxHash: txHash}, nil
}

func (f *fakeChain) sendCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sends)
}

type fakeRelayer struct {
	mu           sync.Mutex
	relays       int
	polls        int
	unreach      bool
	relayErr     error
	relayPackets []ObservedPacket
	// script[i] is returned on poll i; the last entry repeats.
	script [][]PacketStatus
	// onPackets, when set, runs before the page is built. Tests block here.
	onPackets func(context.Context)
}

func (f *fakeRelayer) Relay(_ context.Context, _, _ string, _ []PacketSelector) ([]ObservedPacket, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.relays++
	if f.relayErr != nil {
		return nil, f.relayErr
	}
	if f.relayPackets != nil {
		return f.relayPackets, nil
	}
	return []ObservedPacket{{SourceClientID: "base-0", Sequence: "42", Selection: SelectionSelected}}, nil
}

func (f *fakeRelayer) Packets(ctx context.Context, _ PacketFilter, _ uint32, _ string) (PacketsPage, error) {
	if f.onPackets != nil {
		f.onPackets(ctx)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unreach {
		return PacketsPage{}, NewError(CodeRelayerUnavailable, "connection refused")
	}
	if len(f.script) == 0 {
		return PacketsPage{Packets: []PacketStatus{}}, nil
	}
	i := min(f.polls, len(f.script)-1)
	f.polls++
	return PacketsPage{Packets: f.script[i]}, nil
}

func (f *fakeRelayer) relayCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.relays
}

// --- harness ---

type harness struct {
	t        *testing.T
	srv      *Server
	chain    *fakeChain
	relayer  *fakeRelayer
	session  *mcp.ClientSession
	progress []string
	mu       sync.Mutex
}

type clientOpts struct {
	elicit          func(*mcp.ElicitParams) (*mcp.ElicitResult, error)
	protocolVersion string
	// meter, when set, counts tool calls on the server.
	meter metric.Meter
}

func newHarness(t *testing.T, co clientOpts) *harness {
	t.Helper()
	h := &harness{t: t, chain: &fakeChain{}, relayer: &fakeRelayer{}}
	h.start(co, true)
	return h
}

// newConfirmingHarness asks before every send, as the server does by default.
func newConfirmingHarness(t *testing.T, co clientOpts) *harness {
	t.Helper()
	h := &harness{t: t, chain: &fakeChain{}, relayer: &fakeRelayer{}}
	h.start(co, false)
	return h
}

func (h *harness) start(co clientOpts, skipConfirmation bool) {
	t := h.t
	srv, err := New(
		h.chain,
		h.relayer,
		Options{PollInterval: 10 * time.Millisecond, Meter: co.meter},
	)
	require.NoError(t, err)
	srv.skipConfirmation = skipConfirmation
	h.srv = srv

	copts := &mcp.ClientOptions{
		ProgressNotificationHandler: func(_ context.Context, req *mcp.ProgressNotificationClientRequest) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.progress = append(h.progress, req.Params.Message)
		},
	}
	if co.elicit != nil {
		copts.ElicitationHandler = func(_ context.Context, req *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
			return co.elicit(req.Params)
		}
	}

	ct, st := mcp.NewInMemoryTransports()
	ctx := context.Background()
	ss, err := srv.MCP().Connect(ctx, st, nil)
	require.NoError(t, err)
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, copts).
		Connect(ctx, ct, &mcp.ClientSessionOptions{ProtocolVersion: co.protocolVersion})
	require.NoError(t, err)
	h.session = cs
	t.Cleanup(func() {
		_ = cs.Close()
		_ = ss.Wait()
		_ = srv.Close()
	})
}

// call returns the structured output, or the ToolError for an error result.
func (h *harness) call(name string, args map[string]any, progressToken any) (map[string]any, *ToolError) {
	h.t.Helper()
	params := &mcp.CallToolParams{Name: name, Arguments: args}
	if progressToken != nil {
		params.SetProgressToken(progressToken)
	}
	res, err := h.session.CallTool(context.Background(), params)
	require.NoError(h.t, err)
	if res.IsError {
		text := res.Content[0].(*mcp.TextContent).Text
		var te ToolError
		if json.Unmarshal([]byte(text), &te) != nil {
			// rejected by the SDK's schema validation before reaching the handler
			return nil, &ToolError{Message: text}
		}
		return nil, &te
	}
	bz, err := json.Marshal(res.StructuredContent)
	require.NoError(h.t, err)
	var out map[string]any
	require.NoError(h.t, json.Unmarshal(bz, &out))
	return out, nil
}

func sendArgs(amount string) map[string]any {
	return map[string]any{
		"chain_id": chainA, "ift": tokenA, "client_id": "base-0",
		"to": bobHex, "amount": amount, "from": agentKey,
	}
}

// --- tests ---

func TestToolsAreListedWithHints(t *testing.T) {
	h := newHarness(t, clientOpts{})
	res, err := h.session.ListTools(context.Background(), nil)
	require.NoError(t, err)

	tools := map[string]*mcp.Tool{}
	for _, tool := range res.Tools {
		tools[tool.Name] = tool
	}
	require.Len(t, tools, 6)
	for _, name := range []string{"ibc_get_context", "ibc_ift_balance", "ibc_get_packets", "ibc_wait_for_packets"} {
		require.True(t, tools[name].Annotations.ReadOnlyHint, name)
	}
	require.False(t, tools["ibc_ift_send"].Annotations.ReadOnlyHint)
	require.True(t, *tools["ibc_ift_send"].Annotations.DestructiveHint)
	require.True(t, tools["ibc_relay_tx"].Annotations.IdempotentHint)

	schema, err := json.Marshal(tools["ibc_ift_send"].InputSchema)
	require.NoError(t, err)
	require.Contains(t, string(schema), `"enum":["auto","always","never"]`)
	require.Contains(t, string(schema), `"required":["chain_id","ift","client_id","to","amount","from"]`)
	require.Equal(t,
		"packet lifetime from now (default 900 seconds), not how long the tool waits for mining",
		schemaDescription(t, tools["ibc_ift_send"].InputSchema, "timeout_seconds"),
	)

	require.Equal(t,
		"the wait ended before done; this is not an error. If no packets have been seen, call ibc_relay_tx, then call ibc_wait_for_packets again. If packets are already moving, call ibc_wait_for_packets again. A not_selected packet does not wait out this timeout. Do not call ibc_ift_send again",
		schemaDescription(t, tools["ibc_wait_for_packets"].OutputSchema, "timed_out"),
	)
	require.Equal(t,
		"every packet reached the until condition, including ack, timeout, rejection, and relay failure. Read each packet state and summary before telling the user the transfer arrived. succeeded is the delivered state. timed_out, rejected, and relay_failed are finished and failed. Do not call ibc_ift_send again because a packet timed out or the wait timed out",
		schemaDescription(t, tools["ibc_wait_for_packets"].OutputSchema, "done"),
	)

	instructions := h.session.InitializeResult().Instructions
	require.Contains(t, instructions, "After ibc_ift_send, follow the next field on the result.")
	require.Contains(t, instructions,
		"ibc_ift_send blocks until the user approves and the source tx is mined or the confirm timeout elapses. If the call is cancelled or the client times out and a transaction hash was returned, call ibc_get_packets with that hash as source_tx_hash. Do not call ibc_ift_send again. If no hash was returned, ask the user and do not call ibc_ift_send. TX_NOT_CONFIRMED is not retryable.",
	)
	require.NotContains(t, instructions, "or the user confirms the first one did not broadcast")
	require.NotContains(t, tools["ibc_ift_send"].Description, "Follow delivery with ibc_wait_for_packets")
	require.Contains(t, tools["ibc_ift_send"].Description, "does not return until the user accepts or declines")
	require.Contains(t, tools["ibc_ift_send"].Description, "POLICY_DENIED")
	require.Contains(t, tools["ibc_ift_send"].Description, "5 minutes")
	require.Equal(t, 5*time.Minute, defaultConfirmTimeout)
	require.Contains(t, tools["ibc_ift_send"].Description, "default 900 seconds")
	require.NotContains(t, tools["ibc_ift_send"].Description, "timeout_seconds is how long the tool waits")

	waitTimeout := schemaDescription(t, tools["ibc_wait_for_packets"].InputSchema, "timeout_seconds")
	require.Contains(t, waitTimeout, "600")
	require.Contains(t, waitTimeout, "1800")
	require.Equal(t, 600, int(defaultWaitTimeout.Seconds()))
	require.Equal(t, 1800, int(maxWaitTimeout.Seconds()))
	require.NotContains(t, waitTimeout, "server config")
	require.Contains(t, tools["ibc_wait_for_packets"].Description, "call ibc_wait_for_packets again, not ibc_ift_send")
}

func schemaDescription(t *testing.T, schema any, prop string) string {
	t.Helper()
	bz, err := json.Marshal(schema)
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(bz, &doc))
	props, ok := doc["properties"].(map[string]any)
	require.True(t, ok)
	field, ok := props[prop].(map[string]any)
	require.True(t, ok, prop)
	desc, ok := field["description"].(string)
	require.True(t, ok, prop)
	return desc
}

func TestGetContextAppliesPolicy(t *testing.T) {
	h := newHarness(t, clientOpts{})
	out, terr := h.call("ibc_get_context", nil, nil)
	require.Nil(t, terr)

	signers := out["signers"].([]any)
	require.Len(t, signers, 2, "both connection signers are exposed")
	require.Equal(t, agentKey, signers[0].(map[string]any)["alias"])
	require.Equal(t, "relayer-key", signers[1].(map[string]any)["alias"])

	chains := out["chains"].([]any)
	require.Equal(t, true, chains[0].(map[string]any)["writable"])
	require.Equal(t, true, chains[1].(map[string]any)["writable"])
	require.Equal(t, false, chains[2].(map[string]any)["writable"])

	require.Equal(t, true, out["relayer"].(map[string]any)["ready"])
	require.Equal(t, false, out["require_confirmation"])
}

func TestBalance(t *testing.T) {
	h := newHarness(t, clientOpts{})
	out, terr := h.call("ibc_ift_balance", map[string]any{"chain_id": chainB, "ift": tokenA, "address": agentKey}, nil)
	require.Nil(t, terr)
	require.Equal(t, agentHex, out["address"])
	require.Equal(t, "125000000", out["balance"])
	require.Equal(t, "USDX", out["symbol"])
}

func TestSendPolicyDenials(t *testing.T) {
	h := newHarness(t, clientOpts{})

	cases := map[string]struct {
		mutate func(map[string]any)
		code   ErrorCode
		msg    string
	}{
		"signer": {func(a map[string]any) { a["from"] = "relayer-key" }, CodePolicyDenied, "not configured on client"},
		"client": {func(a map[string]any) { a["chain_id"] = chainB }, CodePolicyDenied, "not on this relayer"},
		"amount": {func(a map[string]any) { a["amount"] = "1.5" }, CodeInvalidArgument, "positive integer"},
		"zero":   {func(a map[string]any) { a["amount"] = "0" }, CodeInvalidArgument, "positive integer"},
		// 2^256 + 5 would pack as 5.
		"uint256 overflow": {func(a map[string]any) {
			a["amount"] = "115792089237316195423570985008687907853269984665640564039457584007913129639941"
		}, CodeInvalidArgument, "at most 2^256-1"},
		"uint256 max is not refused as an amount": {func(a map[string]any) {
			a["amount"] = "115792089237316195423570985008687907853269984665640564039457584007913129639935"
			a["from"] = "relayer-key"
		}, CodePolicyDenied, "not configured on client"},
		"relay":   {func(a map[string]any) { a["relay"] = "sometimes" }, "", "enum"},
		"timeout": {func(a map[string]any) { a["timeout_seconds"] = 5 }, "", "minimum"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			args := sendArgs("10")
			tc.mutate(args)
			_, terr := h.call("ibc_ift_send", args, nil)
			require.NotNil(t, terr)
			require.Equal(t, tc.code, terr.Code)
			require.Contains(t, terr.Message, tc.msg)
		})
	}
	require.Zero(t, h.chain.sendCount(), "nothing reaches the chain")
}

func TestSendRelaysUnlessAutoRelay(t *testing.T) {
	h := newHarness(t, clientOpts{})

	out, terr := h.call("ibc_ift_send", sendArgs("100"), nil)
	require.Nil(t, terr)
	require.Equal(t, "mined", out["status"])
	require.Equal(t, txHash, out["tx"].(map[string]any)["tx_hash"])
	require.Equal(t, true, out["relay_requested"])
	require.Len(t, out["packets"].([]any), 1)
	require.Equal(t, 1, h.relayer.relayCount())
	require.Equal(t, bobHex, h.chain.sends[0].To)
	require.Equal(t, defaultSendTimeout, h.chain.sends[0].Timeout)
	require.Equal(t, "Call ibc_wait_for_packets with this chain_id and tx_hash to follow delivery.", out["next"])

	h.chain.autoRelay = true
	out, terr = h.call("ibc_ift_send", sendArgs("100"), nil)
	require.Nil(t, terr)
	require.Equal(t, false, out["relay_requested"])
	require.Equal(t, 1, h.relayer.relayCount(), "auto mode skips relay when the client auto-relays")
	require.Equal(t,
		"The relayer will pick the packet up. Call ibc_wait_for_packets with this chain_id and tx_hash.",
		out["next"],
	)

	args := sendArgs("100")
	args["relay"] = "always"
	out, terr = h.call("ibc_ift_send", args, nil)
	require.Nil(t, terr)
	require.Equal(t, 2, h.relayer.relayCount())
	require.Equal(t, true, out["relay_requested"])
	require.Equal(t, "Call ibc_wait_for_packets with this chain_id and tx_hash to follow delivery.", out["next"])

	args = sendArgs("100")
	args["relay"] = "never"
	out, terr = h.call("ibc_ift_send", args, nil)
	require.Nil(t, terr)
	require.Equal(t, "mined", out["status"])
	require.Equal(t, txHash, out["tx"].(map[string]any)["tx_hash"])
	require.Equal(t, false, out["relay_requested"])
	require.Equal(t, 2, h.relayer.relayCount(), "relay=never does not call the relayer")
	require.Equal(t,
		"Delivery will not start until you call ibc_relay_tx with this chain_id and tx_hash. Waiting is not a substitute.",
		out["next"],
	)
	require.NotContains(t, out["next"], "ibc_wait_for_packets")

	h.chain.autoRelay = false
	h.relayer.relayErr = errors.New("relay down")
	out, terr = h.call("ibc_ift_send", sendArgs("100"), nil)
	require.Nil(t, terr, "a relay error after the transfer is mined is not a failed tool call")
	require.Equal(t, "mined", out["status"])
	require.Equal(t, txHash, out["tx"].(map[string]any)["tx_hash"])
	require.Contains(t, out["warnings"].([]any)[0], "tx is mined but the relay request failed")
	require.Contains(t, out["warnings"].([]any)[0], "relay down")
	require.Equal(t, "Call ibc_relay_tx with this chain_id and tx_hash. Do not wait first.", out["next"])
	require.NotContains(t, out["next"], "ibc_wait_for_packets")
	require.NotContains(t, out["next"], "ibc_ift_send")

	h.relayer.relayErr = nil
	h.relayer.relayPackets = []ObservedPacket{{
		SourceClientID: "base-0", Sequence: "9", Selection: SelectionUnconfigured,
	}}
	out, terr = h.call("ibc_ift_send", sendArgs("100"), nil)
	require.Nil(t, terr)
	require.Equal(t, "mined", out["status"])
	require.Equal(t, txHash, out["tx"].(map[string]any)["tx_hash"])
	require.Equal(t, true, out["relay_requested"])
	require.Contains(t, out["warnings"].([]any)[0], "base-0/9")
	require.Contains(t, out["warnings"].([]any)[0], "no route")
	require.Equal(t,
		"The source transfer is already mined. Do not call ibc_ift_send again. "+
			"This relayer will not deliver packet base-0/9; it has no route on this relayer. Do not wait for this relayer to deliver it.",
		out["next"],
	)
	requireMinedUnconfiguredNext(t, out["next"].(string))

	h.relayer.relayPackets = []ObservedPacket{
		{SourceClientID: "base-0", Sequence: "9", Selection: SelectionUnconfigured},
		{SourceClientID: "base-0", Sequence: "10", Selection: SelectionSelected},
	}
	out, terr = h.call("ibc_ift_send", sendArgs("100"), nil)
	require.Nil(t, terr)
	require.Contains(t, out["warnings"].([]any)[0], "base-0/9")
	require.Equal(t,
		"This relayer will not deliver packet base-0/9; it has no route on this relayer. Do not wait for this relayer to deliver it. "+
			"Call ibc_wait_for_packets with this chain_id and tx_hash to follow delivery.",
		out["next"],
	)
}

func TestSendNextAllUnconfiguredNamesNoFollowUp(t *testing.T) {
	one := []ObservedPacket{{
		SourceClientID: "base-0", Sequence: "9", Selection: SelectionUnconfigured,
	}}
	want := "The source transfer is already mined. Do not call ibc_ift_send again. " +
		"This relayer will not deliver packet base-0/9; it has no route on this relayer. Do not wait for this relayer to deliver it."
	for _, mode := range []string{relayAuto, relayAlways, relayNever} {
		got := sendNext(mode, true, false, false, one)
		require.Equal(t, want, got)
		requireMinedUnconfiguredNext(t, got)
	}
	// A relay failure does not add a follow-up tool when every packet is unconfigured.
	failed := sendNext(relayAlways, false, true, false, one)
	require.Equal(t, want, failed)
	requireMinedUnconfiguredNext(t, failed)

	two := sendNext(relayAuto, true, false, false, []ObservedPacket{
		{SourceClientID: "base-0", Sequence: "9", Selection: SelectionUnconfigured},
		{SourceClientID: "base-1", Sequence: "3", Selection: SelectionUnconfigured},
	})
	require.Contains(t, two, "already mined")
	require.Contains(t, two, "packets base-0/9, base-1/3")
	requireMinedUnconfiguredNext(t, two)

	mixed := sendNext(relayAuto, true, false, false, []ObservedPacket{
		{SourceClientID: "base-0", Sequence: "9", Selection: SelectionUnconfigured},
		{SourceClientID: "base-0", Sequence: "10", Selection: SelectionSelected},
	})
	require.Contains(t, mixed, "ibc_wait_for_packets")
	require.NotContains(t, mixed, "already mined")
}

func requireMinedUnconfiguredNext(t *testing.T, next string) {
	t.Helper()
	require.Contains(t, next, "already mined")
	require.Contains(t, next, "Do not call ibc_ift_send")
	require.Contains(t, next, "no route")
	require.NotContains(t, next, "Call ibc_ift_send")
	require.NotContains(t, next, "ibc_wait_for_packets")
	require.NotContains(t, next, "ibc_relay_tx")
	require.NotContains(t, next, "ibc_get_packets")
}

func TestSendReportsUnconfirmedTx(t *testing.T) {
	h := newHarness(t, clientOpts{})
	h.chain.sendErr = NewTxError(CodeTxNotConfirmed, TxRef{ChainID: chainA, TxHash: txHash}, "not mined")
	_, terr := h.call("ibc_ift_send", sendArgs("500"), nil)
	require.Equal(t, CodeTxNotConfirmed, terr.Code)
	require.Equal(t, txHash, terr.Tx.TxHash)
	require.False(t, terr.Retryable)
}

func TestConfirmation(t *testing.T) {
	// 2026-07-28 uses multi round-trip input requests; older versions get a
	// server-initiated elicitation/create from the SDK. Both must behave the same.
	for _, version := range []string{"2026-07-28", "2025-11-25"} {
		t.Run(version, func(t *testing.T) {
			t.Run("client without elicitation is refused", func(t *testing.T) {
				h := newConfirmingHarness(t, clientOpts{protocolVersion: version})
				_, terr := h.call("ibc_ift_send", sendArgs("10"), nil)
				require.Equal(t, CodePolicyDenied, terr.Code)
				require.Contains(t, terr.Message, "elicitation")
				require.Zero(t, h.chain.sendCount())
			})

			t.Run("declined", func(t *testing.T) {
				h := newConfirmingHarness(
					t,
					clientOpts{protocolVersion: version, elicit: func(*mcp.ElicitParams) (*mcp.ElicitResult, error) {
						return &mcp.ElicitResult{Action: "decline"}, nil
					}},
				)
				_, terr := h.call("ibc_ift_send", sendArgs("10"), nil)
				require.Equal(t, CodeUserDeclined, terr.Code)
				require.Zero(t, h.chain.sendCount())
			})

			t.Run("approved", func(t *testing.T) {
				var shown string
				h := newConfirmingHarness(
					t,
					clientOpts{protocolVersion: version, elicit: func(p *mcp.ElicitParams) (*mcp.ElicitResult, error) {
						shown = p.Message
						return &mcp.ElicitResult{Action: "accept", Content: map[string]any{"approve": true}}, nil
					}},
				)
				out, terr := h.call("ibc_ift_send", sendArgs("150"), nil)
				require.Nil(t, terr)
				require.Equal(t, "mined", out["status"])
				require.Equal(t, 1, h.chain.sendCount())
				require.Contains(t, shown, "0.00015 USDX (150 base units)")
				require.Contains(t, shown, bobHex)
				require.Contains(t, shown, "over client base-0")
				require.Contains(t, shown, "relay auto")
			})

			t.Run("accepted without approving", func(t *testing.T) {
				h := newConfirmingHarness(
					t,
					clientOpts{protocolVersion: version, elicit: func(*mcp.ElicitParams) (*mcp.ElicitResult, error) {
						return &mcp.ElicitResult{Action: "accept", Content: map[string]any{"approve": false}}, nil
					}},
				)
				_, terr := h.call("ibc_ift_send", sendArgs("10"), nil)
				require.Equal(t, CodeUserDeclined, terr.Code)
				require.Zero(t, h.chain.sendCount())
			})
		})
	}
}

// A retry must carry a state token for exactly the write the user approved.
func TestConfirmationIsBoundToTheWrite(t *testing.T) {
	h := newConfirmingHarness(t, clientOpts{})
	ctx := context.Background()

	// Drive the round trip by hand on the raw handler, as a hostile client could.
	call := func(args SendInput, state string, approve bool) (*mcp.CallToolResult, error) {
		req := &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{RequestState: state}}
		if state != "" {
			req.Params.InputResponses = mcp.InputResponseMap{
				confirmInputKey: &mcp.ElicitResult{Action: "accept", Content: map[string]any{"approve": approve}},
			}
		}
		res, _, err := h.srv.iftSend(ctx, req, args)
		return res, err
	}
	in := SendInput{ChainID: chainA, IFT: tokenA, ClientID: "base-0", To: bobHex, Amount: "100", From: agentKey}

	// without client capabilities the first round is refused...
	_, err := call(in, "", false)
	require.ErrorContains(t, err, "elicitation")

	// ...so mint a token directly for the approved write
	fields := []string{
		"Send",
		chainA,
		strings.ToLower(tokenA),
		agentKey,
		strings.ToLower(bobHex),
		"100",
		"base-0",
		defaultSendTimeout.String(),
		relayAuto,
	}
	state := h.srv.confirm.issue(fields...)

	tampered := in
	tampered.Amount = "900"
	_, err = call(tampered, state, true)
	require.ErrorContains(t, err, "does not match")
	require.ErrorContains(t, err, "call the tool again")
	require.NotContains(t, err.Error(), "may already be broadcast")
	require.Zero(t, h.chain.sendCount())

	_, err = call(in, state, true)
	require.NoError(t, err)
	require.Equal(t, 1, h.chain.sendCount())

	_, err = call(in, state, true)
	require.ErrorContains(t, err, "already used")
	require.ErrorContains(t, err, "may already be broadcast")
	require.ErrorContains(t, err, "ibc_get_packets")
	require.ErrorContains(t, err, "source_tx_hash")
	require.Contains(t, err.Error(), "Do not call ibc_ift_send")
	require.NotContains(t, err.Error(), "Call ibc_ift_send")
	require.NotContains(t, err.Error(), "call the tool again")
	var te *ToolError
	require.ErrorAs(t, err, &te)
	require.Equal(t, CodePolicyDenied, te.Code)
	require.False(t, te.Retryable)
	require.NotNil(t, te.Tx)
	require.Equal(t, txHash, te.Tx.TxHash)
	require.Equal(t, chainA, te.Tx.ChainID)
	require.Equal(t, 1, h.chain.sendCount())
}

func TestConfirmationsExpire(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	c := newConfirmations(func() time.Time { return now })
	state := c.issue("a", "b")
	require.Equal(t, confirmMismatch, c.redeem(state, "a", "c"))
	now = now.Add(confirmStateTTL + time.Second)
	require.Equal(t, confirmExpired, c.redeem(state, "a", "b"))
	require.Equal(t, confirmMismatch, c.redeem("garbage", "a", "b"))
	// Mismatch and expiry leave the nonce unspent.
	now = now.Add(-confirmStateTTL - time.Second)
	require.Equal(t, confirmAccepted, c.redeem(state, "a", "b"))
	require.Equal(t, confirmUsed, c.redeem(state, "a", "b"))

	// A spent token stays used after it expires, until spentRetention passes.
	now = now.Add(confirmStateTTL + time.Second)
	require.Equal(t, confirmUsed, c.redeem(state, "a", "b"))
	now = now.Add(spentRetention - 2*time.Second)
	require.Equal(t, confirmUsed, c.redeem(state, "a", "b"))
	now = now.Add(2 * time.Second)
	require.Equal(t, confirmStale, c.redeem(state, "a", "b"))
	require.Empty(t, c.used, "records past retention are dropped")

	// An unspent token past retention is stale too: it cannot be told apart.
	fresh := c.issue("a", "b")
	now = now.Add(confirmStateTTL + spentRetention + time.Second)
	require.Equal(t, confirmStale, c.redeem(fresh, "a", "b"))

	// A token from before a restart cannot be checked, used or not.
	restarted := newConfirmations(func() time.Time { return now })
	earlier := c.issue("a", "b")
	require.Equal(t, confirmOtherRun, restarted.redeem(earlier, "a", "b"))
	require.Equal(t, confirmMismatch, restarted.redeem("a.b.c", "a", "b"))
}

func TestApprovalFromBeforeRestartAsksTheUser(t *testing.T) {
	h := newConfirmingHarness(t, clientOpts{})
	fields := []string{
		"Send", chainA, strings.ToLower(tokenA), agentKey, strings.ToLower(bobHex),
		"100", "base-0", defaultSendTimeout.String(), relayAuto,
	}
	earlier := newConfirmations(time.Now).issue(fields...)
	in := SendInput{ChainID: chainA, IFT: tokenA, ClientID: "base-0", To: bobHex, Amount: "100", From: agentKey}
	req := &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{
		RequestState: earlier,
		InputResponses: mcp.InputResponseMap{
			confirmInputKey: &mcp.ElicitResult{Action: "accept", Content: map[string]any{"approve": true}},
		},
	}}
	_, _, err := h.srv.iftSend(context.Background(), req, in)
	var te *ToolError
	require.ErrorAs(t, err, &te)
	require.Equal(t, CodePolicyDenied, te.Code)
	require.Contains(t, te.Message, "restarted")
	require.Contains(t, te.Message, "Ask the user")
	require.NotContains(t, te.Message, "call the tool again")
	require.Zero(t, h.chain.sendCount())
}

func TestExpiredConfirmationSaysCallAgain(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	chain := &fakeChain{}
	srv, err := New(chain, &fakeRelayer{}, Options{Now: func() time.Time { return now }})
	require.NoError(t, err)

	fields := []string{
		"Send", chainA, strings.ToLower(tokenA), agentKey, strings.ToLower(bobHex),
		"100", "base-0", defaultSendTimeout.String(), relayAuto,
	}
	state := srv.confirm.issue(fields...)
	in := SendInput{
		ChainID: chainA, IFT: tokenA, ClientID: "base-0", To: bobHex, Amount: "100", From: agentKey,
	}
	call := func(args SendInput, token string) error {
		req := &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{
			RequestState: token,
			InputResponses: mcp.InputResponseMap{
				confirmInputKey: &mcp.ElicitResult{Action: "accept", Content: map[string]any{"approve": true}},
			},
		}}
		_, _, callErr := srv.iftSend(context.Background(), req, args)
		return callErr
	}

	now = now.Add(confirmStateTTL + time.Second)
	err = call(in, state)
	require.ErrorContains(t, err, "expired")
	require.ErrorContains(t, err, "call the tool again")
	require.NotContains(t, err.Error(), "may already be broadcast")
	require.NotContains(t, err.Error(), "already used")
	var te *ToolError
	require.ErrorAs(t, err, &te)
	require.Equal(t, CodePolicyDenied, te.Code)
	require.False(t, te.Retryable)
	require.Zero(t, chain.sendCount())

	now = now.Add(-confirmStateTTL - time.Second)
	require.NoError(t, call(in, state))
	require.Equal(t, 1, chain.sendCount())

	err = call(in, state)
	require.ErrorAs(t, err, &te)
	require.False(t, te.Retryable)
	require.Contains(t, te.Message, "already used")
	require.Contains(t, te.Message, "may already be broadcast")
	require.Contains(t, te.Message, "source_tx_hash")
	require.Contains(t, te.Message, "Do not call ibc_ift_send")
	require.NotContains(t, te.Message, "Call ibc_ift_send")
	require.NotContains(t, te.Message, "call the tool again")
	require.NotNil(t, te.Tx)
	require.Equal(t, txHash, te.Tx.TxHash)
	require.Equal(t, 1, chain.sendCount())

	// After the token expires, a replay of the spent approval still says
	// already used, not expired, so the agent is not told to send again.
	now = now.Add(confirmStateTTL + time.Minute)
	err = call(in, state)
	require.ErrorAs(t, err, &te)
	require.Contains(t, te.Message, "already used")
	require.NotContains(t, te.Message, "call the tool again")
	require.NotNil(t, te.Tx)
	require.Equal(t, txHash, te.Tx.TxHash)

	now = now.Add(spentRetention)
	err = call(in, state)
	require.ErrorAs(t, err, &te)
	require.Contains(t, te.Message, "may already be broadcast")
	require.Contains(t, te.Message, "Ask the user")
	require.NotContains(t, te.Message, "call the tool again")
	require.Equal(t, 1, chain.sendCount())
}

func TestSpentApprovalWithoutHashAsksTheUser(t *testing.T) {
	h := newConfirmingHarness(t, clientOpts{})
	fields := []string{
		"Send", chainA, strings.ToLower(tokenA), agentKey, strings.ToLower(bobHex),
		"100", "base-0", defaultSendTimeout.String(), relayAuto,
	}
	state := h.srv.confirm.issue(fields...)
	require.Equal(t, confirmAccepted, h.srv.confirm.redeem(state, fields...))

	in := SendInput{ChainID: chainA, IFT: tokenA, ClientID: "base-0", To: bobHex, Amount: "100", From: agentKey}
	req := &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{
		RequestState: state,
		InputResponses: mcp.InputResponseMap{
			confirmInputKey: &mcp.ElicitResult{Action: "accept", Content: map[string]any{"approve": true}},
		},
	}}
	_, _, err := h.srv.iftSend(context.Background(), req, in)
	var te *ToolError
	require.ErrorAs(t, err, &te)
	require.Equal(t, CodePolicyDenied, te.Code)
	require.False(t, te.Retryable)
	require.Nil(t, te.Tx)
	require.Contains(t, te.Message, "Ask the user")
	require.Contains(t, te.Message, "Do not call ibc_ift_send")
	require.NotContains(t, te.Message, "source_tx_hash")
	require.NotContains(t, err.Error(), "source_tx_hash")
	require.NotContains(t, te.Message, "call the tool again")
	require.Zero(t, h.chain.sendCount())
}

func TestSpentApprovalKeepsBroadcastHash(t *testing.T) {
	h := newConfirmingHarness(t, clientOpts{})
	h.chain.sendErr = NewTxError(CodeTxNotConfirmed, TxRef{ChainID: chainA, TxHash: txHash}, "not mined")
	fields := []string{
		"Send", chainA, strings.ToLower(tokenA), agentKey, strings.ToLower(bobHex),
		"100", "base-0", defaultSendTimeout.String(), relayAuto,
	}
	state := h.srv.confirm.issue(fields...)
	in := SendInput{ChainID: chainA, IFT: tokenA, ClientID: "base-0", To: bobHex, Amount: "100", From: agentKey}
	call := func() error {
		req := &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{
			RequestState: state,
			InputResponses: mcp.InputResponseMap{
				confirmInputKey: &mcp.ElicitResult{Action: "accept", Content: map[string]any{"approve": true}},
			},
		}}
		_, _, callErr := h.srv.iftSend(context.Background(), req, in)
		return callErr
	}

	err := call()
	var te *ToolError
	require.ErrorAs(t, err, &te)
	require.Equal(t, CodeTxNotConfirmed, te.Code)
	require.Equal(t, txHash, te.Tx.TxHash)

	err = call()
	require.ErrorAs(t, err, &te)
	require.Equal(t, CodePolicyDenied, te.Code)
	require.False(t, te.Retryable)
	require.NotNil(t, te.Tx)
	require.Equal(t, chainA, te.Tx.ChainID)
	require.Equal(t, txHash, te.Tx.TxHash)
	require.Contains(t, te.Message, "source_tx_hash")
	require.Contains(t, te.Message, "Do not call ibc_ift_send")
	require.Equal(t, 1, h.chain.sendCount())
}

func packet(state PacketState, recv bool) PacketStatus {
	p := PacketStatus{
		SourceClientID: "base-0", Sequence: "42", State: state, Final: state.Final(),
		SendTx: &TxRef{ChainID: chainA, TxHash: txHash},
	}
	if recv {
		p.RecvTx = &TxRef{ChainID: chainB, TxHash: "0xrecv"}
	}
	if state == StateSucceeded {
		p.AckTx = &TxRef{ChainID: chainA, TxHash: "0xack"}
	}
	return p
}

func TestWaitForPacketsReportsProgress(t *testing.T) {
	h := newHarness(t, clientOpts{})
	h.relayer.script = [][]PacketStatus{
		{},
		{packet(StatePending, false)},
		{packet(StatePending, true)},
		{packet(StateSucceeded, true)},
	}

	out, terr := h.call("ibc_wait_for_packets", map[string]any{"chain_id": chainA, "tx_hash": txHash}, "tok-1")
	require.Nil(t, terr)
	require.Equal(t, true, out["done"])
	require.Equal(t, false, out["timed_out"])
	require.InDelta(t, 1, out["summary"].(map[string]any)["succeeded"], 0)
	p := out["packets"].([]any)[0].(map[string]any)
	require.Equal(t, "succeeded", p["state"])
	require.Equal(t, "0xack", p["ack_tx"].(map[string]any)["tx_hash"])
	require.Equal(t, waitDoneHint, out["hint"])
	require.NotContains(t, out["hint"], "call ibc_ift_send again.")
	require.Contains(t, out["hint"], "Do not call ibc_ift_send again")

	require.Eventually(t, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return len(h.progress) == 2
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, "packet base-0/42: pending (0/1 packets done)", h.progress[0])
	require.Equal(t, "packet base-0/42: pending -> succeeded (1/1 packets done)", h.progress[1])
}

func TestWaitUntilReceived(t *testing.T) {
	h := newHarness(t, clientOpts{})
	h.relayer.script = [][]PacketStatus{{packet(StatePending, false)}, {packet(StatePending, true)}}

	out, terr := h.call("ibc_wait_for_packets", map[string]any{
		"chain_id": chainA, "tx_hash": txHash, "until": "received",
	}, nil)
	require.Nil(t, terr)
	require.Equal(t, true, out["done"])
	require.InDelta(t, 1, out["summary"].(map[string]any)["pending"], 0)
}

func TestWaitTimesOutWithHint(t *testing.T) {
	cases := []struct {
		name   string
		script [][]PacketStatus
		hint   string
	}{
		{
			name:   "no packets",
			script: nil,
			hint:   waitNoPacketsHint,
		},
		{
			name:   "moving",
			script: [][]PacketStatus{{packet(StatePending, false)}},
			hint:   waitMovingHint,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, clientOpts{})
			h.relayer.script = tc.script

			out, terr := h.call("ibc_wait_for_packets", map[string]any{
				"chain_id": chainA, "tx_hash": txHash, "timeout_seconds": 1,
			}, nil)
			require.Nil(t, terr, "a wait timeout is not an error")
			require.Equal(t, false, out["done"])
			require.Equal(t, true, out["timed_out"])
			require.Equal(t, tc.hint, out["hint"])
			require.Contains(t, out["hint"], "Do not call ibc_ift_send again")
		})
	}
}

func TestWaitTimeoutBoundsSlowRelayerQuery(t *testing.T) {
	h := newHarness(t, clientOpts{})
	var polls atomic.Int32
	h.relayer.onPackets = func(ctx context.Context) {
		// The first poll answers; the second hangs until its context ends.
		if polls.Add(1) > 1 {
			<-ctx.Done()
		}
	}
	h.relayer.script = [][]PacketStatus{{packet(StatePending, false)}}

	start := time.Now()
	out, terr := h.call("ibc_wait_for_packets", map[string]any{
		"chain_id": chainA, "tx_hash": txHash, "timeout_seconds": 1,
	}, nil)
	require.Less(t, time.Since(start), 3*time.Second, "the query is bound by timeout_seconds")
	require.Nil(t, terr, "a wait timeout is not an error")
	require.Equal(t, true, out["timed_out"])
	require.Equal(t, waitMovingHint, out["hint"])
	require.Len(t, out["packets"], 1, "the last answered poll is reported")
}

func TestWaitNotSelectedReturnsImmediately(t *testing.T) {
	relayer := &fakeRelayer{script: [][]PacketStatus{{
		{
			SourceClientID: "base-0", Sequence: "1", State: StatePending,
			SendTx: &TxRef{ChainID: chainA, TxHash: txHash},
		},
		{
			SourceClientID: "base-0", Sequence: "2", State: StateNotSelected,
			SendTx: &TxRef{ChainID: chainA, TxHash: txHash},
		},
	}}}
	srv, err := New(&fakeChain{}, relayer, Options{PollInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	_, out, err := srv.waitForPackets(ctx, nil, WaitInput{
		ChainID: chainA, TxHash: txHash, TimeoutSeconds: 600,
	})
	require.NoError(t, err)
	require.Less(t, time.Since(start), time.Second)
	require.False(t, out.Done)
	require.False(t, out.TimedOut)
	require.Equal(t, waitNotSelectedHint, out.Hint)
	require.Equal(t, 1, out.Summary.Pending)
	require.Equal(t, 1, out.Summary.NotSelected)
	require.Contains(t, out.Hint, "Call ibc_relay_tx")
	require.Contains(t, out.Hint, "ibc_wait_for_packets")
	require.Contains(t, out.Hint, "Do not call ibc_ift_send")
}

func TestWaitCancelIsToolError(t *testing.T) {
	relayer := &fakeRelayer{script: [][]PacketStatus{{packet(StatePending, false)}}}
	started := make(chan struct{})
	relayer.onPackets = func(ctx context.Context) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-ctx.Done()
	}
	srv, err := New(&fakeChain{}, relayer, Options{PollInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-started
		cancel()
	}()
	_, _, err = srv.waitForPackets(ctx, nil, WaitInput{
		ChainID: chainA, TxHash: txHash, TimeoutSeconds: 600,
	})
	var te *ToolError
	require.ErrorAs(t, err, &te)
	require.Equal(t, CodeRelayerError, te.Code)
	require.False(t, te.Retryable)
	require.Equal(t, waitCanceledMessage, te.Message)
	require.Contains(t, te.Message, "Call ibc_wait_for_packets again")
	require.Contains(t, te.Message, "not ibc_ift_send")
	require.NotContains(t, te.Message, "context canceled")
	require.NotContains(t, err.Error(), "context canceled")
	var raw map[string]any
	require.NoError(t, json.Unmarshal([]byte(err.Error()), &raw))
	require.Equal(t, string(CodeRelayerError), raw["code"])
	require.Equal(t, waitCanceledMessage, raw["message"])
	require.Equal(t, false, raw["retryable"])

	kept := waitCancelError(NewError(CodeRelayerUnavailable, "connection refused"))
	require.True(t, kept.Retryable)
	require.Equal(t, CodeRelayerUnavailable, kept.Code)
	rpc := waitCancelError(NewError(CodeChainRPCError, "timeout"))
	require.True(t, rpc.Retryable)
	plain := waitCancelError(context.DeadlineExceeded)
	require.False(t, plain.Retryable)
	require.NotEqual(t, CodeChainRPCError, plain.Code)
	require.NotEqual(t, CodeRelayerUnavailable, plain.Code)
	require.Equal(t, waitCanceledMessage, plain.Message)
}

func TestWaitDoneReadsTerminalFailure(t *testing.T) {
	h := newHarness(t, clientOpts{})
	h.relayer.script = [][]PacketStatus{{packet(StateTimedOut, false)}}

	out, terr := h.call("ibc_wait_for_packets", map[string]any{
		"chain_id": chainA, "tx_hash": txHash,
	}, nil)
	require.Nil(t, terr)
	require.Equal(t, true, out["done"])
	require.Equal(t, false, out["timed_out"])
	require.InDelta(t, 1, out["summary"].(map[string]any)["timed_out"], 0)
	require.Equal(t, waitDoneHint, out["hint"])
	require.Contains(t, out["hint"], "succeeded is the delivered state")
	require.Contains(t, out["hint"], "Do not call ibc_ift_send again")
}

func TestReadValidation(t *testing.T) {
	h := newHarness(t, clientOpts{})

	_, terr := h.call("ibc_wait_for_packets", map[string]any{"chain_id": chainA, "tx_hash": "0x12"}, nil)
	require.Equal(t, CodeInvalidArgument, terr.Code)

	_, terr = h.call("ibc_relay_tx", map[string]any{"chain_id": chainA, "tx_hash": "nope"}, nil)
	require.Equal(t, CodeInvalidArgument, terr.Code)

	out, terr := h.call("ibc_get_packets", map[string]any{"source_tx_hash": txHash}, nil)
	require.Nil(t, terr)
	require.Equal(t, []any{}, out["packets"])

	_, terr = h.call("ibc_get_packets", map[string]any{"sequence": "-1"}, nil)
	require.Equal(t, CodeInvalidArgument, terr.Code)

	h.relayer.unreach = true
	_, terr = h.call("ibc_get_packets", nil, nil)
	require.Equal(t, CodeRelayerUnavailable, terr.Code)
	require.True(t, terr.Retryable)
}

func TestPacketsResource(t *testing.T) {
	h := newHarness(t, clientOpts{})
	h.relayer.script = [][]PacketStatus{{packet(StateSucceeded, true)}}

	uri := "ibc://packets/" + chainA + "/" + txHash
	res, err := h.session.ReadResource(context.Background(), &mcp.ReadResourceParams{URI: uri})
	require.NoError(t, err)
	require.Contains(t, res.Contents[0].Text, `"state":"succeeded"`)

	_, err = h.session.ReadResource(context.Background(), &mcp.ReadResourceParams{URI: "ibc://packets/1/bad"})
	require.Error(t, err)
}

func TestFormatUnits(t *testing.T) {
	require.Equal(t, "1.5", formatUnits(big.NewInt(1_500_000), 6))
	require.Equal(t, "0.000001", formatUnits(big.NewInt(1), 6))
	require.Equal(t, "2", formatUnits(big.NewInt(2_000_000), 6))
	require.Equal(t, "42", formatUnits(big.NewInt(42), 0))
}

func TestRelayRequiresPolicyChain(t *testing.T) {
	h := newHarness(t, clientOpts{})
	_, terr := h.call("ibc_relay_tx", map[string]any{"chain_id": "999", "tx_hash": txHash}, nil)
	require.Equal(t, CodePolicyDenied, terr.Code)
	require.Contains(t, terr.Message, "no connection")
	require.Zero(t, h.relayer.relayCount())

	_, terr = h.call("ibc_relay_tx", map[string]any{"chain_id": chainA, "tx_hash": txHash}, nil)
	require.Nil(t, terr)
	require.Equal(t, 1, h.relayer.relayCount())
}

func TestRelayNextDoesNotWaitWhenNothingQueued(t *testing.T) {
	notSelected := []ObservedPacket{
		{SourceClientID: "base-0", Sequence: "1", Selection: SelectionNotSelected},
		{SourceClientID: "base-0", Sequence: "2", Selection: SelectionNotSelected},
	}
	for _, packets := range [][]ObservedPacket{notSelected, {}} {
		got := sendNext(relayAuto, true, false, false, packets)
		require.NotContains(t, got, "ibc_wait_for_packets")
		require.Contains(t, got, "ibc_relay_tx")
		require.NotContains(t, got, "ibc_ift_send")
	}
	selected := sendNext(relayAuto, true, false, false, []ObservedPacket{
		{SourceClientID: "base-0", Sequence: "1", Selection: SelectionNotSelected},
		{SourceClientID: "base-0", Sequence: "2", Selection: SelectionSelected},
	})
	require.Contains(t, selected, "ibc_wait_for_packets")

	h := newHarness(t, clientOpts{})
	h.relayer.relayPackets = notSelected
	out, terr := h.call("ibc_relay_tx", map[string]any{"chain_id": chainA, "tx_hash": txHash}, nil)
	require.Nil(t, terr)
	require.NotContains(t, out["next"], "ibc_wait_for_packets")
	require.Contains(t, out["next"], "not_selected")
	require.Contains(t, out["next"], "ibc_relay_tx")

	h.relayer.relayPackets = []ObservedPacket{}
	out, terr = h.call("ibc_relay_tx", map[string]any{"chain_id": chainA, "tx_hash": txHash}, nil)
	require.Nil(t, terr)
	require.NotContains(t, out["next"], "ibc_wait_for_packets")
	require.Contains(t, out["next"], "no packets")
	require.Contains(t, out["next"], "ibc_relay_tx")

	h.relayer.relayPackets = []ObservedPacket{{
		SourceClientID: "base-0", Sequence: "9", Selection: SelectionUnconfigured,
	}}
	out, terr = h.call("ibc_relay_tx", map[string]any{"chain_id": chainA, "tx_hash": txHash}, nil)
	require.Nil(t, terr)
	requireMinedUnconfiguredNext(t, out["next"].(string))
	require.Contains(t, out["next"], "base-0/9")

	h.relayer.relayPackets = []ObservedPacket{
		{SourceClientID: "base-0", Sequence: "1", Selection: SelectionNotSelected},
		{SourceClientID: "base-0", Sequence: "2", Selection: SelectionSelected},
	}
	out, terr = h.call("ibc_relay_tx", map[string]any{"chain_id": chainA, "tx_hash": txHash}, nil)
	require.Nil(t, terr)
	require.Contains(t, out["next"], "ibc_wait_for_packets")

	h.relayer.relayPackets = notSelected
	out, terr = h.call("ibc_ift_send", sendArgs("100"), nil)
	require.Nil(t, terr)
	require.NotContains(t, out["next"], "ibc_wait_for_packets")
	require.Contains(t, out["next"], "not_selected")
	require.Contains(t, out["next"], "ibc_relay_tx")

	h.relayer.relayPackets = []ObservedPacket{}
	out, terr = h.call("ibc_ift_send", sendArgs("100"), nil)
	require.Nil(t, terr)
	require.NotContains(t, out["next"], "ibc_wait_for_packets")
	require.Contains(t, out["next"], "no packets")
	require.Contains(t, out["next"], "ibc_relay_tx")
}

type loopCursorRelayer struct{}

func (loopCursorRelayer) Relay(context.Context, string, string, []PacketSelector) ([]ObservedPacket, error) {
	return nil, nil
}

func (loopCursorRelayer) Packets(context.Context, PacketFilter, uint32, string) (PacketsPage, error) {
	return PacketsPage{HasMore: true, NextCursor: "again"}, nil
}

func TestTxPacketsStopsOnRepeatedCursor(t *testing.T) {
	srv, err := New(&fakeChain{}, loopCursorRelayer{}, Options{})
	require.NoError(t, err)
	_, _, err = srv.txPackets(context.Background(), chainA, txHash)
	require.ErrorContains(t, err, "repeated")
}

// panicChain panics while reading token metadata.
type panicChain struct{ *fakeChain }

func (panicChain) TokenInfo(context.Context, string, string) (TokenInfo, error) {
	var info *TokenInfo
	return *info, nil
}

func TestHandlerPanicIsAnErrorNotACrash(t *testing.T) {
	srv, err := New(panicChain{&fakeChain{}}, &fakeRelayer{}, Options{})
	require.NoError(t, err)
	ctx := context.Background()
	ct, st := mcp.NewInMemoryTransports()
	ss, err := srv.MCP().Connect(ctx, st, nil)
	require.NoError(t, err)
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).Connect(ctx, ct, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = cs.Close()
		_ = ss.Wait()
		_ = srv.Close()
	})

	_, err = cs.CallTool(ctx, &mcp.CallToolParams{Name: "ibc_ift_balance", Arguments: map[string]any{
		"chain_id": chainA, "ift": tokenA, "address": agentKey,
	}})
	require.ErrorContains(t, err, "internal error")

	res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "ibc_get_context"})
	require.NoError(t, err, "the server keeps serving after a handler panic")
	require.False(t, res.IsError)
}
