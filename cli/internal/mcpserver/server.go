// SPDX-License-Identifier: Apache-2.0

// Package mcpserver exposes IBC CLI operations as Model Context Protocol tools,
// so an AI agent can send IFT transfers and follow their packets. It runs
// inside `ibc relayer run`, on its own port, and calls that relayer directly.
// Every write uses the signer the relayer already has on that client.
package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"regexp"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.opentelemetry.io/otel/metric"
)

const (
	serverName = "ibc"

	// defaultSendTimeout is the packet lifetime (15 minutes, 900 seconds) when
	// timeout_seconds is omitted. It is not how long the tool waits for mining.
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

var txHashPattern = regexp.MustCompile(`^0x[0-9a-fA-F]{64}$`)

// Options tune a Server; the zero value is usable.
type Options struct {
	Version string
	// Now overrides the clock (tests).
	Now func() time.Time
	// PollInterval overrides how often a wait polls. Zero uses the default.
	PollInterval time.Duration
	// SkipConfirmation skips the approval prompt. Tests use it. The server asks
	// before every send otherwise, because the endpoint has no authentication.
	SkipConfirmation bool
	// Audit records MCP writes. Nil discards them; tests do that. `ibc relayer run`
	// opens the file before the server starts.
	Audit *AuditLog
	// Meter counts tool calls on the relayer's existing meter. Nil skips
	// metrics. `ibc relayer run` passes that meter; this server does not
	// listen for scrapes.
	Meter metric.Meter
}

// Server holds the MCP server and the state behind its tools.
type Server struct {
	chain            ChainBackend
	relayer          RelayerBackend
	confirm          *confirmations
	now              func() time.Time
	mcp              *mcp.Server
	confirmTimeout   time.Duration
	waitDefault      time.Duration
	waitMax          time.Duration
	poll             time.Duration
	skipConfirmation bool
	audit            auditSink
	toolCalls        metric.Int64Counter
	knownTools       map[string]struct{}
	// shutdown cancels in-flight tool calls when the server stops.
	shutdown context.Context
	stop     context.CancelFunc
}

// New builds a Server and registers its tools. What a caller may do is the
// relayer's own connections: a send uses the signer configured on that client.
// A non-nil Options.Meter counts each tool call.
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
	shutdown, stop := context.WithCancel(context.Background())
	var audit auditSink
	if opts.Audit != nil {
		audit = opts.Audit
	}
	s := &Server{
		chain: chain, relayer: rel, confirm: newConfirmations(now), now: now,
		confirmTimeout: defaultConfirmTimeout, waitDefault: defaultWaitTimeout,
		waitMax: maxWaitTimeout, poll: poll, skipConfirmation: opts.SkipConfirmation,
		audit: audit, toolCalls: toolCalls, knownTools: map[string]struct{}{},
		shutdown: shutdown, stop: stop,
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
	s.mcp.AddReceivingMiddleware(s.observeToolCall)
	return s, nil
}

// MCP returns the underlying server, to run on a transport.
func (s *Server) MCP() *mcp.Server { return s.mcp }

// Close cancels in-flight tool calls and closes the audit log. The relayer
// keeps its own state. The log lock covers a line that is still syncing.
func (s *Server) Close() error {
	s.stop()
	if s.audit == nil {
		return nil
	}
	return s.audit.Close()
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

// --- ibc_get_context ---

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

// --- ibc_ift_balance ---

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

// --- ibc_ift_send ---

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
		return s.rejectSend(in, relayMode, newError(CodeInvalidArgument, "client_id is required"))
	case timeout < minSendTimeout || timeout > maxSendTimeout:
		return s.rejectSend(in, relayMode, newError(CodeInvalidArgument, "timeout_seconds must be between %d and %d",
			int(minSendTimeout.Seconds()), int(maxSendTimeout.Seconds())))
	case relayMode != relayAuto && relayMode != relayAlways && relayMode != relayNever:
		return s.rejectSend(in, relayMode, newError(CodeInvalidArgument, "relay must be auto, always or never"))
	}

	w, ask, err := s.authorizeWrite(ctx, req, writeRequest{
		action: "Send", chainID: in.ChainID, ift: in.IFT, from: in.From, to: in.To, amount: in.Amount,
		clientID: in.ClientID, timeout: timeout, relay: relayMode,
	})
	if err != nil {
		return s.rejectSend(in, relayMode, err)
	}
	if ask != nil {
		return ask, SendOutput{}, nil
	}

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
	})
	// The approval was spent before this call. Keep the hash, once broadcast
	// has returned one, so a later retry of the same approval can show it.
	// A hash that broadcast has not returned is not invented.
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
			out.Warnings = append(
				out.Warnings,
				"tx is mined but the relay request failed; retry with ibc_relay_tx: "+asToolError(
					err,
					CodeRelayerError,
				).Message,
			)
		} else {
			out.RelayRequested = true
			out.Packets = packets
			for _, p := range packets {
				if p.Selection == SelectionUnconfigured {
					out.Warnings = append(out.Warnings, fmt.Sprintf(
						"packet %s/%s has no route on this relayer and will not be delivered by it",
						p.SourceClientID,
						p.Sequence,
					))
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

// rejectSend records a send that was refused before anything was submitted.
func (s *Server) rejectSend(in SendInput, relay string, err error) (*mcp.CallToolResult, SendOutput, error) {
	s.note(auditRecord{
		Tool: "ibc_ift_send", ChainID: in.ChainID, IFT: in.IFT, From: in.From, To: in.To,
		Amount: in.Amount, ClientID: in.ClientID, Relay: relay,
	}, err)
	return nil, SendOutput{}, err
}

// planRelay applies the relay mode. auto requests relaying unless the source
// client end has autoRelay enabled, in which case skippedForAutoRelay is true
// and this server does not send a relay request. never does not request a relay.
func (s *Server) planRelay(ctx context.Context, mode, chainID, clientID string) (request, skippedForAutoRelay bool) {
	switch mode {
	case relayNever:
		return false, false
	case relayAlways:
		return true, false
	}
	ends, err := s.connectionEnds(ctx)
	if err != nil {
		return true, false
	}
	for _, end := range ends {
		if end.ChainID == chainID && end.ClientID == clientID {
			return !end.AutoRelay, end.AutoRelay
		}
	}
	return true, false
}

const (
	nextWait        = "Call ibc_wait_for_packets with this chain_id and tx_hash to follow delivery."
	nextRelayFailed = "Call ibc_relay_tx with this chain_id and tx_hash. Do not wait first."
	nextNever       = "Delivery will not start until you call ibc_relay_tx with this chain_id and tx_hash. Waiting is not a substitute."
	nextAutoRelay   = "The relayer will pick the packet up. Call ibc_wait_for_packets with this chain_id and tx_hash."
	nextNoPackets   = "The relay returned no packets. Call ibc_relay_tx again with this chain_id and tx_hash."
	nextNotSelected = "Every packet is not_selected: this request did not queue them. Call ibc_relay_tx again with this chain_id and tx_hash."
	nextNotQueued   = "This relayer did not queue these packets. Call ibc_relay_tx again with this chain_id and tx_hash."
)

// sendNext tells the agent the next tool call for this mined transfer.
// When every observed packet is unconfigured, the transfer is already mined,
// this relayer has no route, and the text names no follow-up tool.
// An unconfigured packet among others, and a relay failure, both stay in the
// text. A relay failure is not a reason to wait first. Waiting is only a next
// step when at least one packet was selected. not_selected means this request
// did not queue the packet, and an empty relay result queued nothing.
func sendNext(mode string, requested, failed, skippedForAutoRelay bool, packets []ObservedPacket) string {
	if allUnconfigured(packets) {
		return allUnconfiguredNext(packets)
	}
	var parts []string
	if msg := unconfiguredNext(packets); msg != "" {
		parts = append(parts, msg)
	}
	if failed {
		parts = append(parts, nextRelayFailed)
	}
	switch {
	case mode == relayNever:
		parts = append(parts, nextNever)
	case !failed && skippedForAutoRelay:
		parts = append(parts, nextAutoRelay)
	case requested && !failed && packetsSelected(packets):
		parts = append(parts, nextWait)
	case requested && !failed && len(unconfiguredPacketIDs(packets)) == 0:
		parts = append(parts, relayAgainNext(packets))
	}
	return strings.Join(parts, " ")
}

// relayAgainNext is the next step when a relay ran and queued nothing.
// not_selected and an empty packet list both mean another ibc_relay_tx, not a wait.
func relayAgainNext(packets []ObservedPacket) string {
	if len(packets) == 0 {
		return nextNoPackets
	}
	for _, p := range packets {
		if p.Selection != SelectionNotSelected {
			return nextNotQueued
		}
	}
	return nextNotSelected
}

func allUnconfigured(packets []ObservedPacket) bool {
	if len(packets) == 0 {
		return false
	}
	for _, p := range packets {
		if p.Selection != SelectionUnconfigured {
			return false
		}
	}
	return true
}

// allUnconfiguredNext is the next step when this relayer has no route for any
// observed packet. It names no tool to call.
func allUnconfiguredNext(packets []ObservedPacket) string {
	return "The source transfer is already mined. Do not call ibc_ift_send again. " + unconfiguredNext(packets)
}

func unconfiguredNext(packets []ObservedPacket) string {
	ids := unconfiguredPacketIDs(packets)
	if len(ids) == 0 {
		return ""
	}
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

func unconfiguredPacketIDs(packets []ObservedPacket) []string {
	ids := make([]string, 0, len(packets))
	for _, p := range packets {
		if p.Selection == SelectionUnconfigured {
			ids = append(ids, p.SourceClientID+"/"+p.Sequence)
		}
	}
	return ids
}

func packetsSelected(packets []ObservedPacket) bool {
	for _, p := range packets {
		if p.Selection == SelectionSelected {
			return true
		}
	}
	return false
}

// --- ibc_relay_tx ---

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
	ok, err := s.relaysChain(ctx, in.ChainID)
	if err != nil {
		return s.rejectRelay(in, asToolError(err, CodeConfigError))
	}
	if !ok {
		return s.rejectRelay(in, newError(CodePolicyDenied, "chain %q has no connection on this relayer", in.ChainID))
	}

	rec, err := s.beginAudit(auditRecord{
		Tool: "ibc_relay_tx", ChainID: in.ChainID, TxHash: in.TxHash,
	})
	if err != nil {
		return nil, RelayOutput{}, err
	}
	packets, err := s.relayer.Relay(ctx, in.ChainID, in.TxHash, in.Packets)
	if err != nil {
		_ = s.finish(rec, asToolError(err, CodeRelayerError))
		return nil, RelayOutput{}, asToolError(err, CodeRelayerError)
	}
	if packets == nil {
		packets = []ObservedPacket{}
	}
	next := relayNext(packets)
	rec.Outcome = outcomeRelayed
	if err := s.record(rec); err != nil {
		return nil, RelayOutput{
			Packets:  packets,
			Warnings: []string{"audit log update failed: " + err.Error()},
			Next:     next,
		}, nil
	}
	return nil, RelayOutput{Packets: packets, Next: next}, nil
}

// relayNext is the next step after a relay that returned. It waits only when
// at least one packet was selected.
func relayNext(packets []ObservedPacket) string {
	return sendNext(relayAlways, true, false, false, packets)
}

// rejectRelay records a relay that was refused before the relayer was asked.
func (s *Server) rejectRelay(in RelayInput, err error) (*mcp.CallToolResult, RelayOutput, error) {
	s.note(auditRecord{Tool: "ibc_relay_tx", ChainID: in.ChainID, TxHash: in.TxHash}, err)
	return nil, RelayOutput{}, err
}

// --- ibc_get_packets ---

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
			return nil, PacketsPage{}, newError(CodeInvalidArgument, "invalid sequence %q", in.Sequence)
		}
		v := seq.Uint64()
		filter.Sequence = &v
	}
	limit := in.Limit
	if limit == 0 {
		limit = defaultPacketsLimit
	}
	if limit < 1 || limit > maxPacketsLimit {
		return nil, PacketsPage{}, newError(CodeInvalidArgument, "limit must be between 1 and %d", maxPacketsLimit)
	}

	page, err := s.relayer.Packets(ctx, filter, uint32(limit), in.Cursor)
	if err != nil {
		return nil, PacketsPage{}, asToolError(err, CodeRelayerError)
	}
	if page.Packets == nil {
		page.Packets = []PacketStatus{}
	}
	return nil, page, nil
}

// --- resource ---

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
	packets, err := s.txPackets(ctx, parts[0], parts[1])
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

// --- write authorization ---

type writeRequest struct {
	action, chainID, ift, from, to, amount string
	// send only
	clientID string
	timeout  time.Duration
	relay    string
}

// authorizedWrite is a send the relayer's client signer may make.
type authorizedWrite struct {
	to     string
	amount *big.Int
}

// authorizeWrite checks the send against this relayer's connections, then asks
// the user to confirm.
//
// Confirmation uses MCP multi round-trip requests: the first call returns
// ask, an input-required result carrying an elicitation, and the client
// retries with the user's answer. The SDK turns this into a plain
// elicitation/create for clients on older protocol versions. The connection
// check runs again on the retry.
func (s *Server) authorizeWrite(
	ctx context.Context, req *mcp.CallToolRequest, w writeRequest,
) (_ authorizedWrite, ask *mcp.CallToolResult, _ error) {
	if err := requireIFT(w.ift); err != nil {
		return authorizedWrite{}, nil, err
	}
	amount, ok := parseAmount(w.amount)
	if !ok || amount.Sign() == 0 {
		return authorizedWrite{}, nil, newError(
			CodeInvalidArgument,
			"amount %q must be a positive integer in base units",
			w.amount,
		)
	}
	end, found, err := s.clientEnd(ctx, w.chainID, w.clientID)
	if err != nil {
		return authorizedWrite{}, nil, asToolError(err, CodeConfigError)
	}
	if !found {
		return authorizedWrite{}, nil, newError(
			CodePolicyDenied, "client %q on chain %q is not on this relayer", w.clientID, w.chainID)
	}
	if w.from != end.Signer {
		return authorizedWrite{}, nil, newError(
			CodePolicyDenied, "signer %q is not configured on client %q", w.from, w.clientID)
	}

	to, err := s.chain.ResolveAddress(ctx, w.to)
	if err != nil {
		return authorizedWrite{}, nil, asToolError(err, CodeInvalidArgument)
	}

	if !s.skipConfirmation {
		fields := []string{
			w.action,
			w.chainID,
			strings.ToLower(w.ift),
			w.from,
			strings.ToLower(to),
			amount.String(),
			w.clientID,
			w.timeout.String(),
			w.relay,
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

func (s *Server) clientEnd(ctx context.Context, chainID, clientID string) (ClientEndInfo, bool, error) {
	ends, err := s.connectionEnds(ctx)
	if err != nil {
		return ClientEndInfo{}, false, err
	}
	for _, end := range ends {
		if end.ChainID == chainID && end.ClientID == clientID {
			return end, true, nil
		}
	}
	return ClientEndInfo{}, false, nil
}

func (s *Server) relaysChain(ctx context.Context, chainID string) (bool, error) {
	ends, err := s.connectionEnds(ctx)
	if err != nil {
		return false, err
	}
	for _, end := range ends {
		if end.ChainID == chainID {
			return true, nil
		}
	}
	return false, nil
}

// connectionEnds lists both sides of every relayer connection.
func (s *Server) connectionEnds(ctx context.Context) ([]ClientEndInfo, error) {
	conns, err := s.chain.Connections(ctx)
	if err != nil {
		return nil, err
	}
	ends := make([]ClientEndInfo, 0, len(conns)*2)
	for _, c := range conns {
		ends = append(ends, c.ClientA, c.ClientB)
	}
	return ends, nil
}

// askConfirmation builds the input-required result that asks the user to
// approve. A client that cannot be asked is refused, rather than writing
// unconfirmed.
func (s *Server) askConfirmation(
	ctx context.Context, req *mcp.CallToolRequest, w writeRequest, to string, amount *big.Int, fields []string,
) (*mcp.CallToolResult, error) {
	caps := req.ClientCapabilities()
	if caps == nil || caps.Elicitation == nil {
		return nil, newError(CodePolicyDenied,
			"user confirmation is required but this MCP client does not support elicitation")
	}

	shown := amount.String() + " base units"
	if info, err := s.chain.TokenInfo(ctx, w.chainID, w.ift); err == nil {
		shown = fmt.Sprintf("%s %s (%s base units)", formatUnits(amount, info.Decimals), info.Symbol, amount)
	}
	msg := fmt.Sprintf(
		"%s %s of token %s on chain %s from signer %q to %s",
		w.action,
		shown,
		w.ift,
		w.chainID,
		w.from,
		to,
	)
	if w.clientID != "" {
		msg += fmt.Sprintf(", over client %s with a %s packet timeout", w.clientID, w.timeout)
	}
	if w.relay != "" {
		msg += fmt.Sprintf(", relay %s", w.relay)
	}
	msg += ". Approve this transaction?"

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

// checkConfirmation verifies the retry carries the user's approval for
// exactly this write.
func (s *Server) checkConfirmation(req *mcp.CallToolRequest, fields []string) error {
	status := s.confirm.redeem(req.Params.RequestState, fields...)
	if status == confirmUsed {
		return spentApprovalError(s.confirm.spentTx(req.Params.RequestState))
	}
	if status != confirmAccepted {
		// Mismatch and expiry did not spend the token.
		return newError(CodePolicyDenied,
			"confirmation does not match this transaction or has expired; call the tool again")
	}
	res, ok := req.Params.InputResponses[confirmInputKey].(*mcp.ElicitResult)
	if !ok || res == nil {
		return newError(CodeUserDeclined, "no confirmation answer was returned")
	}
	if res.Action != "accept" {
		return newError(CodeUserDeclined, "user did not approve the transaction (%s)", res.Action)
	}
	if approve, _ := res.Content["approve"].(bool); !approve {
		return newError(CodeUserDeclined, "user did not approve the transaction")
	}
	return nil
}

// --- helpers ---

// spentApprovalError is the result of replaying an approval that was already
// redeemed. The approval is spent before broadcast, so a hash exists here only
// when that broadcast has since returned one.
func spentApprovalError(tx *TxRef) *ToolError {
	if tx == nil || tx.TxHash == "" {
		return newError(CodePolicyDenied,
			"confirmation was already used; the transfer may already be broadcast. No transaction hash was returned. Ask the user. Do not call ibc_ift_send.")
	}
	e := newError(CodePolicyDenied,
		"confirmation was already used; the transfer may already be broadcast. Call ibc_get_packets with source_tx_hash. Do not call ibc_ift_send.")
	copied := *tx
	e.Tx = &copied
	return e
}

// observedBroadcast is the tx hash broadcast returned. The value result is
// empty when IFTSend fails, and the hash then sits on the tool error.
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
		return newError(CodeInvalidArgument, "ift %q is not a hex address", addr)
	}
	return nil
}

func requireTxHash(h string) error {
	if !txHashPattern.MatchString(h) {
		return newError(CodeInvalidArgument, "tx_hash %q must be 0x followed by 64 hex characters", h)
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
