// SPDX-License-Identifier: Apache-2.0

// Package mcpserver serves IBC CLI operations as MCP tools inside `ibc relayer run`,
// so an agent can send IFT transfers with the relayer's signers and follow their packets.
package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.opentelemetry.io/otel/metric"
)

const (
	serverName = "ibc"

	// defaultSendTimeout is the packet lifetime when timeout_seconds is omitted, not the mining wait.
	defaultSendTimeout = 15 * time.Minute
	minSendTimeout     = time.Minute
	maxSendTimeout     = 24 * time.Hour

	defaultPacketsLimit = 20
	maxPacketsLimit     = 1000
)

// Values of the relay and until enum inputs.
const (
	relayAuto     = "auto"
	relayAlways   = "always"
	relayNever    = "never"
	untilFinal    = "final"
	untilReceived = "received"
)

const instructions = `Tools for sending IFT tokens across IBC and following the packets.
Call ibc_get_context first: it lists this relayer's chains, clients and signers.
A send must use the signer configured on that client in ibc.yml.
Amounts are decimal strings in the token's base unit.
After ibc_ift_send, follow the next field on the result.
ibc_ift_send blocks until the user approves and the source tx is mined or the confirm timeout elapses. If the call is cancelled or the client times out and a transaction hash was returned, call ibc_get_packets with that hash as source_tx_hash. Do not call ibc_ift_send again. If no hash was returned, ask the user and do not call ibc_ift_send. TX_NOT_CONFIRMED is not retryable.`

const (
	defaultConfirmTimeout = 5 * time.Minute
	defaultWaitTimeout    = 10 * time.Minute
	maxWaitTimeout        = 30 * time.Minute
	defaultPollInterval   = 5 * time.Second
	statusMined           = "mined"
)

const (
	nextWait        = "Call ibc_wait_for_packets with this chain_id and tx_hash to follow delivery."
	nextRelayFailed = "Call ibc_relay_tx with this chain_id and tx_hash. Do not wait first."
	nextNever       = "Delivery will not start until you call ibc_relay_tx with this chain_id and tx_hash. Waiting is not a substitute."
	nextAutoRelay   = "The relayer will pick the packet up. Call ibc_wait_for_packets with this chain_id and tx_hash."
	nextNoPackets   = "The relay returned no packets. Call ibc_relay_tx again with this chain_id and tx_hash."
	nextNotSelected = "Every packet is not_selected: this request did not queue them. Call ibc_relay_tx again with this chain_id and tx_hash."
	nextNotQueued   = "This relayer did not queue these packets. Call ibc_relay_tx again with this chain_id and tx_hash."
)

// writeAction is the first approval field and the verb in the prompt.
const writeAction = "Send"

var txHashPattern = regexp.MustCompile(`^0x[0-9a-fA-F]{64}$`)

// writeDrainTimeout bounds Close's wait, from shutdown start, for writes to log their outcome.
// It is below httpShutdownTimeout so MCP shutdown stays within that one deadline.
var writeDrainTimeout = 3 * time.Second

// Options tune a Server; the zero value is usable.
type Options struct {
	Version string
	// Now overrides the clock (tests).
	Now func() time.Time
	// PollInterval overrides how often a wait polls. Zero uses the default.
	PollInterval time.Duration
	// Audit records MCP writes. Nil discards them; tests do that. `ibc relayer run`
	// opens the file before the server starts.
	Audit *AuditLog
	// Meter counts tool calls on the relayer's meter; nil skips metrics.
	// This server does not listen for scrapes.
	Meter metric.Meter
}

// Server holds the MCP server and the state behind its tools.
type Server struct {
	chain          ChainBackend
	relayer        RelayerBackend
	confirm        *confirmations
	now            func() time.Time
	mcp            *mcp.Server
	confirmTimeout time.Duration
	waitDefault    time.Duration
	waitMax        time.Duration
	poll           time.Duration
	// skipConfirmation skips the approval prompt. Unexported so only tests here
	// can set it; the bearer token, not the prompt, keeps other callers out.
	skipConfirmation bool
	audit            auditSink
	toolCalls        metric.Int64Counter
	knownTools       map[string]struct{}
	// replaced groups the hashes of sends the relayer re-signed.
	replaced replacements
	// writes and inFlight count audited writes without an outcome line yet; Close
	// waits for them. writesMu guards stopping, inFlight and writes.Add.
	writesMu    sync.Mutex
	stopping    bool
	stopStarted time.Time
	writes      sync.WaitGroup
	inFlight    int
	// shutdown cancels in-flight tool calls when the server stops.
	shutdown context.Context
	stop     context.CancelFunc
}

// ContextInput is the (empty) input of ibc_get_context.
type ContextInput struct{}

// WritableChain is a configured chain and whether this relayer has a connection on it.
type WritableChain struct {
	ChainInfo
	Writable bool `json:"writable" jsonschema:"true when a relayer connection includes this chain"`
}

// RelayerStatus says whether the relayer answered a packet query.
type RelayerStatus struct {
	Ready bool   `json:"ready"`
	Error string `json:"error,omitempty"`
}

// ContextOutput is the result of ibc_get_context.
type ContextOutput struct {
	Chains              []WritableChain  `json:"chains"`
	Connections         []ConnectionInfo `json:"connections"`
	Signers             []SignerInfo     `json:"signers"              jsonschema:"signer aliases this relayer uses on its clients"`
	RequireConfirmation bool             `json:"require_confirmation"`
	Relayer             RelayerStatus    `json:"relayer"`
}

// BalanceInput is the input of ibc_ift_balance.
type BalanceInput struct {
	ChainID string `json:"chain_id" jsonschema:"chain the IFT token is deployed on"`
	IFT     string `json:"ift"      jsonschema:"IFT token address"`
	Address string `json:"address"  jsonschema:"hex address or a configured signer alias"`
}

// BalanceOutput is the result of ibc_ift_balance.
type BalanceOutput struct {
	ChainID  string `json:"chain_id"`
	IFT      string `json:"ift"`
	Address  string `json:"address"`
	Symbol   string `json:"symbol"`
	Decimals uint8  `json:"decimals"`
	Balance  string `json:"balance"  jsonschema:"base units"`
}

// SendInput is the input of ibc_ift_send.
type SendInput struct {
	ChainID        string `json:"chain_id"                  jsonschema:"source chain"`
	IFT            string `json:"ift"                       jsonschema:"IFT token address on the source chain"`
	ClientID       string `json:"client_id"                 jsonschema:"source client the bridge is registered for, e.g. base-0"`
	To             string `json:"to"                        jsonschema:"receiver on the counterparty chain: hex address or signer alias"`
	Amount         string `json:"amount"                    jsonschema:"amount in the token's base unit, decimal digits only"`
	From           string `json:"from"                      jsonschema:"signer alias configured on this client in ibc.yml"`
	TimeoutSeconds int    `json:"timeout_seconds,omitempty" jsonschema:"packet lifetime from now (default 900 seconds), not how long the tool waits for mining"`
	Relay          string `json:"relay,omitempty"           jsonschema:"auto (default): request relaying unless the client has autoRelay; always; never"`
}

// SendOutput is the result of ibc_ift_send.
type SendOutput struct {
	Tx             TxRef            `json:"tx"`
	Status         string           `json:"status"`
	RelayRequested bool             `json:"relay_requested"`
	Packets        []ObservedPacket `json:"packets,omitempty"  jsonschema:"present when a relay was requested"`
	Warnings       []string         `json:"warnings,omitempty"`
	Next           string           `json:"next"`
}

// RelayInput is the input of ibc_relay_tx.
type RelayInput struct {
	ChainID string           `json:"chain_id"          jsonschema:"source chain of the tx"`
	TxHash  string           `json:"tx_hash"           jsonschema:"source tx that emitted the packets"`
	Packets []PacketSelector `json:"packets,omitempty" jsonschema:"packets to relay; omit for all packets in the tx"`
}

// RelayOutput is the result of ibc_relay_tx.
type RelayOutput struct {
	Packets  []ObservedPacket `json:"packets"            jsonschema:"every send packet in the tx and whether this relayer will deliver it"`
	Warnings []string         `json:"warnings,omitempty"`
	Next     string           `json:"next"               jsonschema:"what to call next. ibc_wait_for_packets only when at least one packet was selected. When every packet is unconfigured, the transfer is already mined and next names no follow-up tool. When every packet is not_selected, or no packets were returned, call ibc_relay_tx again"`
}

// PacketsInput is the input of ibc_get_packets.
type PacketsInput struct {
	SourceChainID       string `json:"source_chain_id,omitempty"`
	DestinationChainID  string `json:"destination_chain_id,omitempty"`
	SourceClientID      string `json:"source_client_id,omitempty"`
	DestinationClientID string `json:"destination_client_id,omitempty"`
	SourceTxHash        string `json:"source_tx_hash,omitempty"        jsonschema:"usually what you filter on"`
	Sequence            string `json:"sequence,omitempty"              jsonschema:"packet sequence; pair it with source_client_id"`
	State               string `json:"state,omitempty"`
	Limit               int    `json:"limit,omitempty"                 jsonschema:"page size; default 20"`
	Cursor              string `json:"cursor,omitempty"                jsonschema:"next_cursor from the previous page"`
}

// writeRequest is an ibc_ift_send call before authorization. ibc_ift_send is
// the only tool that signs.
type writeRequest struct {
	chainID, ift, from, to, amount, clientID, relay string
	timeout                                         time.Duration
}

// authorizedWrite is a send the relayer's client signer may make.
type authorizedWrite struct {
	to     string
	amount *big.Int
}

// New builds a Server and registers its tools. Sends use the signer configured on the relayer's client.
func New(chain ChainBackend, rel RelayerBackend, opts Options) (*Server, error) {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	poll := opts.PollInterval
	if poll <= 0 {
		poll = defaultPollInterval
	}
	toolCalls, err := newToolCallCounter(opts.Meter)
	if err != nil {
		return nil, fmt.Errorf("register mcp_tool_calls_total: %w", err)
	}
	shutdown, cancel := context.WithCancel(context.Background())
	var audit auditSink
	if opts.Audit != nil {
		audit = opts.Audit
	}
	s := &Server{
		chain: chain, relayer: rel, confirm: newConfirmations(now), now: now,
		confirmTimeout: defaultConfirmTimeout, waitDefault: defaultWaitTimeout,
		waitMax: maxWaitTimeout, poll: poll,
		audit: audit, toolCalls: toolCalls, knownTools: map[string]struct{}{},
		shutdown: shutdown,
	}
	// stop is called by HTTPServer.Stop and Close. The first call starts the
	// write drain's clock, so Close's wait counts from the start of shutdown.
	s.stop = func() {
		s.writesMu.Lock()
		if s.stopStarted.IsZero() {
			s.stopStarted = time.Now()
		}
		s.writesMu.Unlock()
		cancel()
	}

	version := opts.Version
	if version == "" {
		version = "dev"
	}
	s.mcp = mcp.NewServer(
		&mcp.Implementation{Name: serverName, Title: "IBC CLI", Version: version},
		&mcp.ServerOptions{
			Instructions: instructions,
			// A nil logger is discarded by the SDK, so session and jsonrpc
			// errors never reach stderr. This is the relayer logger.
			Logger: slog.With("module", "mcp"),
		},
	)
	s.register()
	// Outside the SDK's multi round-trip middleware, which prompts older clients
	// with the request context, so bindRequest also ends that wait on stop.
	s.mcp.AddReceivingMiddleware(s.observeToolCall, s.recoverPanic, s.bindRequest)
	return s, nil
}

// recoverPanic turns a handler panic into a call error: the SDK does not recover,
// and this server shares the relayer's process.
func (s *Server) recoverPanic(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, req mcp.Request) (res mcp.Result, err error) {
		defer func() {
			if v := recover(); v != nil {
				slog.With("module", "mcp").Error("MCP handler panic",
					"method", method, "panic", v, "stack", string(debug.Stack()))
				res, err = nil, fmt.Errorf("internal error handling %s", method)
			}
		}()
		return next(ctx, method, req)
	}
}

// bindRequest ends a request's context on stop, including the SDK's elicitation wait outside handlers.
func (s *Server) bindRequest(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
		ctx, cancel := s.bind(ctx)
		defer cancel()
		return next(ctx, method, req)
	}
}

// bind returns a context that ends when ctx ends or the server stops.
func (s *Server) bind(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.shutdown, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

// MCP returns the underlying server, to run on a transport.
func (s *Server) MCP() *mcp.Server { return s.mcp }

// Close cancels in-flight calls, waits up to writeDrainTimeout from shutdown start
// for sends and relays to log their outcome, then closes the audit log.
func (s *Server) Close() error {
	s.stop()
	s.writesMu.Lock()
	s.stopping = true
	deadline := s.stopStarted.Add(writeDrainTimeout)
	// No write can begin once stopping is set. Skip the wait when none runs:
	// an expired timer could otherwise win select over a drained channel.
	running := s.inFlight > 0
	s.writesMu.Unlock()
	if running {
		drained := make(chan struct{})
		go func() {
			s.writes.Wait()
			close(drained)
		}()
		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		select {
		case <-drained:
		case <-timer.C:
			slog.With("module", "mcp").Warn(
				"MCP write still running at shutdown; its outcome line may be missing from the audit log")
		}
	}
	if s.audit == nil {
		return nil
	}
	return s.audit.Close()
}

// beginWrite registers a send or relay before its audit intent line; it refuses once Close has started.
func (s *Server) beginWrite() bool {
	s.writesMu.Lock()
	defer s.writesMu.Unlock()
	if s.stopping {
		return false
	}
	s.inFlight++
	s.writes.Add(1)
	return true
}

// endWrite finishes a write registered by beginWrite.
func (s *Server) endWrite() {
	s.writesMu.Lock()
	s.inFlight--
	s.writesMu.Unlock()
	s.writes.Done()
}

func (s *Server) register() {
	f, t := false, true

	mcp.AddTool(s.mcp, s.trackTool(&mcp.Tool{
		Name:        "ibc_get_context",
		Description: "List this relayer's chains, connections (client IDs and signers) and whether it can answer packet queries. Call this first.",
		Annotations: &mcp.ToolAnnotations{Title: "Get IBC context", ReadOnlyHint: true, OpenWorldHint: &f},
	}), s.getContext)

	mcp.AddTool(s.mcp, s.trackTool(&mcp.Tool{
		Name:        "ibc_ift_balance",
		Description: "Get the IFT token balance of an address (hex address or signer alias) on a chain.",
		Annotations: &mcp.ToolAnnotations{Title: "IFT balance", ReadOnlyHint: true, OpenWorldHint: &t},
	}), s.iftBalance)

	mcp.AddTool(s.mcp, s.trackTool(&mcp.Tool{
		Name: "ibc_ift_send",
		Description: "Send IFT tokens over IBC to the counterparty chain of client_id, using the signer configured on that client. " +
			"The call does not return until the user accepts or declines the approval. " +
			"A client without elicitation gets POLICY_DENIED. " +
			"Mining then waits up to 5 minutes. " +
			"timeout_seconds is the packet lifetime (default 900 seconds), not the mining wait. " +
			"Follow the next field on the result.",
		Annotations: &mcp.ToolAnnotations{Title: "Send IFT", DestructiveHint: &t, OpenWorldHint: &t},
		InputSchema: mustSchema[SendInput](func(s *jsonschema.Schema) {
			s.Properties["relay"].Enum = []any{relayAuto, relayAlways, relayNever}
			s.Properties["timeout_seconds"].Minimum = ptr(minSendTimeout.Seconds())
			s.Properties["timeout_seconds"].Maximum = ptr(maxSendTimeout.Seconds())
		}),
	}), s.iftSend)

	mcp.AddTool(s.mcp, s.trackTool(&mcp.Tool{
		Name: "ibc_relay_tx",
		Description: "Ask the relayer to deliver the packets emitted by a source transaction on a chain it relays. Safe to repeat. " +
			"Use when a packet shows not_selected, or the tx was sent with relay=never or outside this server. " +
			"Read next on the result. It names ibc_wait_for_packets only when at least one packet was selected. " +
			"When every packet is unconfigured, the source transfer is already mined, this relayer has no route, and next names no follow-up tool. Do not call ibc_ift_send again. " +
			"When every packet is not_selected, or no packets were returned, next names ibc_relay_tx again.",
		Annotations: &mcp.ToolAnnotations{
			Title: "Relay tx", DestructiveHint: &f, IdempotentHint: true, OpenWorldHint: &f,
		},
	}), s.relayTx)

	mcp.AddTool(s.mcp, s.trackTool(&mcp.Tool{
		Name:        "ibc_get_packets",
		Description: "List packets the relayer knows, newest first, with their relay state and send/recv/ack/timeout tx hashes. Filters combine with AND.",
		Annotations: &mcp.ToolAnnotations{Title: "Get packets", ReadOnlyHint: true, OpenWorldHint: &f},
		InputSchema: mustSchema[PacketsInput](func(s *jsonschema.Schema) {
			s.Properties["state"].Enum = statesAsAny()
			s.Properties["limit"].Minimum = ptr(1.0)
			s.Properties["limit"].Maximum = ptr(float64(maxPacketsLimit))
		}),
	}), s.getPackets)

	mcp.AddTool(s.mcp, s.trackTool(&mcp.Tool{
		Name: "ibc_wait_for_packets",
		Description: "Wait until every packet of a source tx reaches a final state (ack, timeout, rejection or relay failure), " +
			"or is received on the destination with until=received. " +
			"A packet that is not_selected returns immediately: call ibc_relay_tx, then ibc_wait_for_packets again. Do not call ibc_ift_send. " +
			"Sends progress notifications on each state change. " +
			"A wait that times out is not an error: it returns done=false with the latest states. " +
			"Cancelling this call is a reason to call ibc_wait_for_packets again, not ibc_ift_send.",
		Annotations: &mcp.ToolAnnotations{Title: "Wait for packets", ReadOnlyHint: true, OpenWorldHint: &f},
		InputSchema: mustSchema[WaitInput](func(sc *jsonschema.Schema) {
			sc.Properties["until"].Enum = []any{untilFinal, untilReceived}
			sc.Properties["timeout_seconds"].Minimum = ptr(1.0)
			sc.Properties["timeout_seconds"].Maximum = ptr(s.waitMax.Seconds())
		}),
	}), s.waitForPackets)

	s.mcp.AddResourceTemplate(&mcp.ResourceTemplate{
		Name:        "packets",
		Title:       "Packets of a source tx",
		Description: "Relay status of every packet emitted by a source transaction, as JSON.",
		URITemplate: "ibc://packets/{chain_id}/{tx_hash}",
		MIMEType:    "application/json",
	}, s.readPacketsResource)
}

func (s *Server) getContext(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	_ ContextInput,
) (*mcp.CallToolResult, ContextOutput, error) {
	ctx, cancel := s.bind(ctx)
	defer cancel()

	out := ContextOutput{
		Chains:              []WritableChain{},
		Connections:         []ConnectionInfo{},
		Signers:             []SignerInfo{},
		RequireConfirmation: !s.skipConfirmation,
	}

	chains, err := s.chain.Chains(ctx)
	if err != nil {
		return nil, out, asToolError(err, CodeConfigError)
	}
	conns, err := s.chain.Connections(ctx)
	if err != nil {
		return nil, out, asToolError(err, CodeConfigError)
	}
	out.Connections = append(out.Connections, conns...)
	relayed := map[string]struct{}{}
	used := map[string]struct{}{}
	for _, c := range conns {
		for _, end := range []ClientEndInfo{c.ClientA, c.ClientB} {
			if end.ChainID != "" {
				relayed[end.ChainID] = struct{}{}
			}
			if end.Signer != "" {
				used[end.Signer] = struct{}{}
			}
		}
	}
	for _, c := range chains {
		_, ok := relayed[c.ChainID]
		out.Chains = append(out.Chains, WritableChain{ChainInfo: c, Writable: ok})
	}

	signers, err := s.chain.Signers(ctx)
	if err != nil {
		return nil, out, asToolError(err, CodeConfigError)
	}
	for _, sg := range signers {
		if _, ok := used[sg.Alias]; ok {
			out.Signers = append(out.Signers, sg)
		}
	}

	if _, err := s.relayer.Packets(ctx, PacketFilter{}, 1, ""); err != nil {
		out.Relayer.Error = asToolError(err, CodeRelayerUnavailable).Message
	} else {
		out.Relayer.Ready = true
	}

	return nil, out, nil
}

func (s *Server) iftBalance(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in BalanceInput,
) (*mcp.CallToolResult, BalanceOutput, error) {
	ctx, cancel := s.bind(ctx)
	defer cancel()

	if err := requireIFT(in.IFT); err != nil {
		return nil, BalanceOutput{}, err
	}
	addr, err := s.chain.ResolveAddress(ctx, in.Address)
	if err != nil {
		return nil, BalanceOutput{}, asToolError(err, CodeInvalidArgument)
	}
	info, err := s.chain.TokenInfo(ctx, in.ChainID, in.IFT)
	if err != nil {
		return nil, BalanceOutput{}, asToolError(err, CodeChainRPCError)
	}
	bal, err := s.chain.IFTBalance(ctx, in.ChainID, in.IFT, addr)
	if err != nil {
		return nil, BalanceOutput{}, asToolError(err, CodeChainRPCError)
	}
	return nil, BalanceOutput{
		ChainID: in.ChainID, IFT: in.IFT, Address: addr,
		Symbol: info.Symbol, Decimals: info.Decimals, Balance: bal.String(),
	}, nil
}

func (s *Server) iftSend(
	ctx context.Context,
	req *mcp.CallToolRequest,
	in SendInput,
) (*mcp.CallToolResult, SendOutput, error) {
	ctx, cancel := s.bind(ctx)
	defer cancel()

	timeout := defaultSendTimeout
	if in.TimeoutSeconds != 0 {
		timeout = time.Duration(in.TimeoutSeconds) * time.Second
	}
	relayMode := in.Relay
	if relayMode == "" {
		relayMode = relayAuto
	}

	switch {
	case in.ClientID == "":
		return s.rejectSend(in, relayMode, NewError(CodeInvalidArgument, "client_id is required"))
	case timeout < minSendTimeout || timeout > maxSendTimeout:
		return s.rejectSend(in, relayMode, NewError(CodeInvalidArgument, "timeout_seconds must be between %d and %d",
			int(minSendTimeout.Seconds()), int(maxSendTimeout.Seconds())))
	case relayMode != relayAuto && relayMode != relayAlways && relayMode != relayNever:
		return s.rejectSend(in, relayMode, NewError(CodeInvalidArgument, "relay must be auto, always or never"))
	}

	w, ask, err := s.authorizeWrite(ctx, req, writeRequest{
		chainID: in.ChainID, ift: in.IFT, from: in.From, to: in.To, amount: in.Amount,
		clientID: in.ClientID, timeout: timeout, relay: relayMode,
	})
	if err != nil {
		return s.rejectSend(in, relayMode, err)
	}
	if ask != nil {
		return ask, SendOutput{}, nil
	}

	if !s.beginWrite() {
		return s.rejectSend(in, relayMode, NewError(CodeRelayerUnavailable, "the MCP server is stopping"))
	}
	defer s.endWrite()
	rec, err := s.beginAudit(auditRecord{
		Tool: "ibc_ift_send", ChainID: in.ChainID, IFT: in.IFT, From: in.From, To: w.to,
		Amount: w.amount.String(), ClientID: in.ClientID, Relay: relayMode,
	})
	if err != nil {
		return nil, SendOutput{}, err
	}

	tx, err := s.chain.IFTSend(ctx, SendRequest{
		ChainID: in.ChainID, IFT: in.IFT, ClientID: in.ClientID, To: w.to, Amount: w.amount,
		From: in.From, Timeout: timeout, ConfirmTimeout: s.confirmTimeout,
		OnReplaced: func(previous, replacement string) {
			s.noteReplaced(rec, req, in.ChainID, previous, replacement)
		},
	})
	// The approval is already spent. Keep a returned hash so a retry of the
	// same approval can show it; never invent one.
	if ref, ok := observedBroadcast(tx, err); ok {
		s.noteBroadcast(req, ref)
	}
	if err != nil {
		_ = s.finish(rec, err)
		return nil, SendOutput{}, asToolError(err, CodeChainRPCError)
	}
	out := SendOutput{
		Tx:     tx,
		Status: statusMined,
	}

	requestRelay, skippedForAutoRelay := s.planRelay(ctx, relayMode, in.ChainID, in.ClientID)
	relayFailed := false
	if requestRelay {
		packets, err := s.relayer.Relay(ctx, tx.ChainID, tx.TxHash, nil)
		if err != nil {
			relayFailed = true
			out.Warnings = append(out.Warnings, "tx is mined but the relay request failed; retry with ibc_relay_tx: "+
				asToolError(err, CodeRelayerError).Message)
		} else {
			out.RelayRequested = true
			out.Packets = packets
			for _, p := range packets {
				if p.Selection == SelectionUnconfigured {
					out.Warnings = append(out.Warnings, fmt.Sprintf(
						"packet %s/%s has no route on this relayer and will not be delivered by it",
						p.SourceClientID, p.Sequence))
				}
			}
		}
	}
	out.Next = sendNext(relayMode, out.RelayRequested, relayFailed, skippedForAutoRelay, out.Packets)
	rec.Outcome = outcomeMined
	rec.TxHash = tx.TxHash
	if err := s.record(rec); err != nil {
		out.Warnings = append(out.Warnings, "audit log update failed: "+err.Error())
	}
	return nil, out, nil
}

// rejectSend audits a send refused before submission.
func (s *Server) rejectSend(in SendInput, relay string, err error) (*mcp.CallToolResult, SendOutput, error) {
	s.note(auditRecord{
		Tool: "ibc_ift_send", ChainID: in.ChainID, IFT: in.IFT, From: in.From, To: in.To,
		Amount: in.Amount, ClientID: in.ClientID, Relay: relay,
	}, err)
	return nil, SendOutput{}, err
}

// planRelay applies the relay mode; auto skips the request when the source client end has autoRelay.
func (s *Server) planRelay(ctx context.Context, mode, chainID, clientID string) (request, skippedForAutoRelay bool) {
	switch mode {
	case relayNever:
		return false, false
	case relayAlways:
		return true, false
	}
	end, found, err := s.clientEnd(ctx, chainID, clientID)
	if err != nil || !found {
		return true, false
	}
	return !end.AutoRelay, end.AutoRelay
}

// sendNext is the agent's next step after a mined transfer. It suggests waiting only when a packet
// was selected, and names no follow-up tool when every packet is unconfigured.
func sendNext(mode string, requested, failed, skippedForAutoRelay bool, packets []ObservedPacket) string {
	var unconfigured []string
	selected, notSelected := false, 0
	for _, p := range packets {
		switch p.Selection {
		case SelectionUnconfigured:
			unconfigured = append(unconfigured, p.SourceClientID+"/"+p.Sequence)
		case SelectionSelected:
			selected = true
		case SelectionNotSelected:
			notSelected++
		}
	}
	if len(packets) > 0 && len(unconfigured) == len(packets) {
		return "The source transfer is already mined. Do not call ibc_ift_send again. " + unconfiguredNext(unconfigured)
	}
	var parts []string
	if len(unconfigured) > 0 {
		parts = append(parts, unconfiguredNext(unconfigured))
	}
	if failed {
		parts = append(parts, nextRelayFailed)
	}
	switch {
	case mode == relayNever:
		parts = append(parts, nextNever)
	case failed:
	case skippedForAutoRelay:
		parts = append(parts, nextAutoRelay)
	case !requested:
	case selected:
		parts = append(parts, nextWait)
	case len(unconfigured) > 0:
	case len(packets) == 0:
		parts = append(parts, nextNoPackets)
	case notSelected == len(packets):
		parts = append(parts, nextNotSelected)
	default:
		parts = append(parts, nextNotQueued)
	}
	return strings.Join(parts, " ")
}

// unconfiguredNext warns that this relayer will not deliver packets ids.
func unconfiguredNext(ids []string) string {
	if len(ids) == 1 {
		return fmt.Sprintf(
			"This relayer will not deliver packet %s; it has no route on this relayer. Do not wait for this relayer to deliver it.",
			ids[0],
		)
	}
	return fmt.Sprintf(
		"This relayer will not deliver packets %s; it has no route for them on this relayer. Do not wait for this relayer to deliver them.",
		strings.Join(ids, ", "),
	)
}

func (s *Server) relayTx(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in RelayInput,
) (*mcp.CallToolResult, RelayOutput, error) {
	ctx, cancel := s.bind(ctx)
	defer cancel()

	if err := requireTxHash(in.TxHash); err != nil {
		return s.rejectRelay(in, err)
	}
	_, ok, err := s.clientEnd(ctx, in.ChainID, "")
	if err != nil {
		return s.rejectRelay(in, asToolError(err, CodeConfigError))
	}
	if !ok {
		return s.rejectRelay(in, NewError(CodePolicyDenied, "chain %q has no connection on this relayer", in.ChainID))
	}

	if !s.beginWrite() {
		return s.rejectRelay(in, NewError(CodeRelayerUnavailable, "the MCP server is stopping"))
	}
	defer s.endWrite()
	rec, err := s.beginAudit(auditRecord{
		Tool: "ibc_relay_tx", ChainID: in.ChainID, TxHash: in.TxHash,
	})
	if err != nil {
		return nil, RelayOutput{}, err
	}
	packets, used, err := s.relayFamily(ctx, in)
	if err != nil {
		_ = s.finish(rec, asToolError(err, CodeRelayerError))
		return nil, RelayOutput{}, asToolError(err, CodeRelayerError)
	}
	if packets == nil {
		packets = []ObservedPacket{}
	}
	out := RelayOutput{Packets: packets, Next: relayNext(packets)}
	if note := replacedNote(in.TxHash, used); note != "" {
		out.Warnings = append(out.Warnings, note)
	}
	rec.Outcome = outcomeRelayed
	rec.TxHash = used
	if err := s.record(rec); err != nil {
		out.Warnings = append(out.Warnings, "audit log update failed: "+err.Error())
	}
	return nil, out, nil
}

// relayFamily relays in.TxHash, or the mined hash of the same transfer if the relayer
// re-signed it, and returns the hash relayed.
func (s *Server) relayFamily(ctx context.Context, in RelayInput) ([]ObservedPacket, string, error) {
	hashes := s.replaced.family(in.TxHash)
	if len(hashes) == 1 {
		packets, err := s.relayer.Relay(ctx, in.ChainID, in.TxHash, in.Packets)
		return packets, in.TxHash, err
	}
	var askedPackets []ObservedPacket
	var askedErr error
	for _, h := range hashes {
		packets, err := s.relayer.Relay(ctx, in.ChainID, h, in.Packets)
		if err == nil && len(packets) > 0 {
			return packets, h, nil
		}
		if strings.EqualFold(h, in.TxHash) {
			askedPackets, askedErr = packets, err
		}
		if ctx.Err() != nil {
			return nil, in.TxHash, ctx.Err()
		}
	}
	return askedPackets, in.TxHash, askedErr
}

// relayNext is the next step after a relay that returned.
func relayNext(packets []ObservedPacket) string {
	return sendNext(relayAlways, true, false, false, packets)
}

// rejectRelay audits a relay refused before the relayer was asked.
func (s *Server) rejectRelay(in RelayInput, err error) (*mcp.CallToolResult, RelayOutput, error) {
	s.note(auditRecord{Tool: "ibc_relay_tx", ChainID: in.ChainID, TxHash: in.TxHash}, err)
	return nil, RelayOutput{}, err
}

func (s *Server) getPackets(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in PacketsInput,
) (*mcp.CallToolResult, PacketsPage, error) {
	ctx, cancel := s.bind(ctx)
	defer cancel()

	filter := PacketFilter{
		SourceChainID: in.SourceChainID, DestinationChainID: in.DestinationChainID,
		SourceClientID: in.SourceClientID, DestinationClientID: in.DestinationClientID,
		SourceTxHash: in.SourceTxHash, State: PacketState(in.State),
	}
	if in.Sequence != "" {
		seq, ok := parseAmount(in.Sequence)
		if !ok || !seq.IsUint64() {
			return nil, PacketsPage{}, NewError(CodeInvalidArgument, "invalid sequence %q", in.Sequence)
		}
		v := seq.Uint64()
		filter.Sequence = &v
	}
	limit := in.Limit
	if limit == 0 {
		limit = defaultPacketsLimit
	}
	if limit < 1 || limit > maxPacketsLimit {
		return nil, PacketsPage{}, NewError(CodeInvalidArgument, "limit must be between 1 and %d", maxPacketsLimit)
	}

	page, err := s.relayer.Packets(ctx, filter, uint32(limit), in.Cursor)
	if err != nil {
		return nil, PacketsPage{}, asToolError(err, CodeRelayerError)
	}
	// A send the relayer re-signed: its packets are on whichever hash was mined.
	if len(page.Packets) == 0 && in.SourceTxHash != "" && in.Cursor == "" {
		for _, h := range s.replaced.family(in.SourceTxHash) {
			if strings.EqualFold(h, in.SourceTxHash) {
				continue
			}
			filter.SourceTxHash = h
			other, err := s.relayer.Packets(ctx, filter, uint32(limit), "")
			if err != nil {
				return nil, PacketsPage{}, asToolError(err, CodeRelayerError)
			}
			if len(other.Packets) > 0 {
				page = other
				break
			}
		}
	}
	if page.Packets == nil {
		page.Packets = []PacketStatus{}
	}
	return nil, page, nil
}

func (s *Server) readPacketsResource(
	ctx context.Context,
	req *mcp.ReadResourceRequest,
) (*mcp.ReadResourceResult, error) {
	ctx, cancel := s.bind(ctx)
	defer cancel()

	uri := req.Params.URI
	rest, ok := strings.CutPrefix(uri, "ibc://packets/")
	parts := strings.Split(rest, "/")
	if !ok || len(parts) != 2 || parts[0] == "" || !txHashPattern.MatchString(parts[1]) {
		return nil, mcp.ResourceNotFoundError(uri)
	}
	packets, _, err := s.txPackets(ctx, parts[0], parts[1])
	if err != nil {
		return nil, err
	}
	bz, err := json.Marshal(map[string]any{"packets": packets})
	if err != nil {
		return nil, err
	}
	return &mcp.ReadResourceResult{
		Contents: []*mcp.ResourceContents{{URI: uri, MIMEType: "application/json", Text: string(bz)}},
	}, nil
}

// authorizeWrite checks the send against this relayer's connections, then returns ask to get
// user approval via a multi round-trip request; the checks rerun on the client's retry.
func (s *Server) authorizeWrite(
	ctx context.Context, req *mcp.CallToolRequest, w writeRequest,
) (_ authorizedWrite, ask *mcp.CallToolResult, _ error) {
	if err := requireIFT(w.ift); err != nil {
		return authorizedWrite{}, nil, err
	}
	amount, ok := parseAmount(w.amount)
	// ABI packing reduces a uint256 modulo 2^256, so a larger amount would be
	// approved and audited as one number and sent as another.
	if !ok || amount.Sign() == 0 || amount.BitLen() > 256 {
		return authorizedWrite{}, nil, NewError(CodeInvalidArgument,
			"amount %q must be a positive integer in base units, at most 2^256-1", w.amount)
	}
	end, found, err := s.clientEnd(ctx, w.chainID, w.clientID)
	if err != nil {
		return authorizedWrite{}, nil, asToolError(err, CodeConfigError)
	}
	if !found {
		return authorizedWrite{}, nil, NewError(
			CodePolicyDenied, "client %q on chain %q is not on this relayer", w.clientID, w.chainID)
	}
	if w.from != end.Signer {
		return authorizedWrite{}, nil, NewError(
			CodePolicyDenied, "signer %q is not configured on client %q", w.from, w.clientID)
	}

	to, err := s.chain.ResolveAddress(ctx, w.to)
	if err != nil {
		return authorizedWrite{}, nil, asToolError(err, CodeInvalidArgument)
	}

	if !s.skipConfirmation {
		fields := []string{
			writeAction, w.chainID, strings.ToLower(w.ift), w.from, strings.ToLower(to),
			amount.String(), w.clientID, w.timeout.String(), w.relay,
		}
		if len(req.Params.InputResponses) == 0 {
			ask, askErr := s.askConfirmation(ctx, req, w, to, amount, fields)
			return authorizedWrite{}, ask, askErr
		}
		if confirmErr := s.checkConfirmation(req, fields); confirmErr != nil {
			return authorizedWrite{}, nil, confirmErr
		}
	}

	return authorizedWrite{to: to, amount: amount}, nil, nil
}

// clientEnd finds the connection end for clientID on chainID; an empty clientID matches any.
func (s *Server) clientEnd(ctx context.Context, chainID, clientID string) (ClientEndInfo, bool, error) {
	conns, err := s.chain.Connections(ctx)
	if err != nil {
		return ClientEndInfo{}, false, err
	}
	for _, c := range conns {
		for _, end := range []ClientEndInfo{c.ClientA, c.ClientB} {
			if end.ChainID == chainID && (clientID == "" || end.ClientID == clientID) {
				return end, true, nil
			}
		}
	}
	return ClientEndInfo{}, false, nil
}

// askConfirmation builds the approval request; a client without elicitation is refused, not sent unconfirmed.
func (s *Server) askConfirmation(
	ctx context.Context, req *mcp.CallToolRequest, w writeRequest, to string, amount *big.Int, fields []string,
) (*mcp.CallToolResult, error) {
	caps := req.ClientCapabilities()
	if caps == nil || caps.Elicitation == nil {
		return nil, NewError(CodePolicyDenied,
			"user confirmation is required but this MCP client does not support elicitation")
	}

	shown := amount.String() + " base units"
	if info, err := s.chain.TokenInfo(ctx, w.chainID, w.ift); err == nil {
		shown = fmt.Sprintf("%s %s (%s base units)", formatUnits(amount, info.Decimals), info.Symbol, amount)
	}
	msg := fmt.Sprintf(
		"%s %s of token %s on chain %s from signer %q to %s, over client %s with a %s packet timeout, "+
			"relay %s. Approve this transaction?",
		writeAction, shown, w.ift, w.chainID, w.from, to, w.clientID, w.timeout, w.relay)

	return &mcp.CallToolResult{
		InputRequests: mcp.InputRequestMap{
			confirmInputKey: &mcp.ElicitParams{
				Message: msg,
				RequestedSchema: &jsonschema.Schema{
					Type: "object",
					Properties: map[string]*jsonschema.Schema{
						"approve": {
							Type:        "boolean",
							Title:       "Approve",
							Description: "Sign and broadcast this transaction",
						},
					},
					Required: []string{"approve"},
				},
			},
		},
		RequestState: s.confirm.issue(fields...),
	}, nil
}

// checkConfirmation verifies the retry carries the user's approval for exactly this write.
func (s *Server) checkConfirmation(req *mcp.CallToolRequest, fields []string) error {
	switch s.confirm.redeem(req.Params.RequestState, fields...) {
	case confirmAccepted:
	case confirmUsed:
		return spentApprovalError(s.confirm.spentTx(req.Params.RequestState))
	case confirmOtherRun:
		return NewError(CodePolicyDenied,
			"confirmation was issued before this relayer restarted, so whether it was used is not known; "+
				"the transfer may already be broadcast. Ask the user before calling ibc_ift_send again.")
	case confirmStale:
		return NewError(CodePolicyDenied,
			"confirmation expired more than %s ago and this server no longer knows whether it was used; "+
				"the transfer may already be broadcast. Ask the user before calling ibc_ift_send again.",
			spentRetention)
	default:
		// Mismatch and expiry did not spend the token.
		return NewError(CodePolicyDenied,
			"confirmation does not match this transaction or has expired; call the tool again")
	}
	res, ok := req.Params.InputResponses[confirmInputKey].(*mcp.ElicitResult)
	if !ok || res == nil {
		return NewError(CodeUserDeclined, "no confirmation answer was returned")
	}
	if res.Action != "accept" {
		return NewError(CodeUserDeclined, "user did not approve the transaction (%s)", res.Action)
	}
	if approve, _ := res.Content["approve"].(bool); !approve {
		return NewError(CodeUserDeclined, "user did not approve the transaction")
	}
	return nil
}

// spentApprovalError reports a replayed approval. Approvals are spent before broadcast,
// so a hash is present only if that broadcast has since returned one.
func spentApprovalError(tx *TxRef) *ToolError {
	if tx == nil || tx.TxHash == "" {
		return NewError(CodePolicyDenied,
			"confirmation was already used; the transfer may already be broadcast. No transaction hash was returned. Ask the user. Do not call ibc_ift_send.")
	}
	e := NewError(CodePolicyDenied,
		"confirmation was already used; the transfer may already be broadcast. Call ibc_get_packets with source_tx_hash. Do not call ibc_ift_send.")
	copied := *tx
	e.Tx = &copied
	return e
}

// observedBroadcast returns the broadcast tx hash, taken from the tool error when IFTSend failed.
func observedBroadcast(tx TxRef, err error) (TxRef, bool) {
	if tx.TxHash != "" {
		return tx, true
	}
	var te *ToolError
	if errors.As(err, &te) && te.Tx != nil && te.Tx.TxHash != "" {
		return *te.Tx, true
	}
	return TxRef{}, false
}

// noteBroadcast stores a hash on the approval that authorized this send.
func (s *Server) noteBroadcast(req *mcp.CallToolRequest, tx TxRef) {
	if req == nil || req.Params == nil {
		return
	}
	s.confirm.noteTx(req.Params.RequestState, tx)
}

func requireIFT(addr string) error {
	if !common.IsHexAddress(addr) {
		return NewError(CodeInvalidArgument, "ift %q is not a hex address", addr)
	}
	return nil
}

func requireTxHash(h string) error {
	if !txHashPattern.MatchString(h) {
		return NewError(CodeInvalidArgument, "tx_hash %q must be 0x followed by 64 hex characters", h)
	}
	return nil
}

// formatUnits renders base units with decimals, trimming trailing zeros.
func formatUnits(v *big.Int, decimals uint8) string {
	if decimals == 0 {
		return v.String()
	}
	s := v.String()
	d := int(decimals)
	if len(s) <= d {
		s = strings.Repeat("0", d-len(s)+1) + s
	}
	whole, frac := s[:len(s)-d], strings.TrimRight(s[len(s)-d:], "0")
	if frac == "" {
		return whole
	}
	return whole + "." + frac
}

func mustSchema[T any](patch func(*jsonschema.Schema)) *jsonschema.Schema {
	sc, err := jsonschema.For[T](nil)
	if err != nil {
		panic(err)
	}
	patch(sc)
	return sc
}

func statesAsAny() []any {
	out := make([]any, 0, len(PacketStates))
	for _, st := range PacketStates {
		out = append(out, string(st))
	}
	return out
}

func ptr[T any](v T) *T { return &v }
