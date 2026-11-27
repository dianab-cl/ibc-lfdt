// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Send statuses, in order.
const (
	sendSubmitting = "submitting"
	sendBroadcast  = "broadcast"
	sendMined      = "mined"
	sendFailed     = "failed"
)

const (
	// maxSends caps the sends kept in memory for ibc_list_sends.
	maxSends          = 100
	defaultSendsLimit = 20
)

// SendRecord is one ibc_ift_send of this run and how far it got.
type SendRecord struct {
	ID             string           `json:"id"`
	Time           time.Time        `json:"time"`
	ChainID        string           `json:"chain_id"`
	ClientID       string           `json:"client_id"`
	IFT            string           `json:"ift"`
	From           string           `json:"from"`
	To             string           `json:"to"`
	Amount         string           `json:"amount"                    jsonschema:"base units"`
	AmountDisplay  string           `json:"amount_display,omitempty"  jsonschema:"amount in whole tokens with symbol; empty when token metadata could not be read"`
	Relay          string           `json:"relay"`
	Tx             *TxRef           `json:"tx,omitempty"              jsonschema:"latest broadcast tx; the mined one once mined"`
	Status         string           `json:"status"                    jsonschema:"submitting, broadcast, mined or failed"`
	RelayRequested bool             `json:"relay_requested"`
	Packets        []ObservedPacket `json:"packets,omitempty"         jsonschema:"packets the relay request found"`
	Warnings       []string         `json:"warnings,omitempty"`
	Error          *ToolError       `json:"error,omitempty"`
	Next           NextStep         `json:"next"`
}

// ListSendsInput is the input of ibc_list_sends.
type ListSendsInput struct {
	Limit int `json:"limit,omitempty" jsonschema:"most recent sends to return; default 20"`
}

// ListSendsOutput is the result of ibc_list_sends.
type ListSendsOutput struct {
	Sends []SendRecord `json:"sends" jsonschema:"newest first"`
}

// sendLog keeps the latest sends of this run.
type sendLog struct {
	mu    sync.Mutex
	sends []*SendRecord // oldest first
}

func (l *sendLog) add(r SendRecord) *SendRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.sends) == maxSends {
		l.sends = l.sends[1:]
	}
	p := &r
	l.sends = append(l.sends, p)
	return p
}

func (l *sendLog) update(r *SendRecord, f func(*SendRecord)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	f(r)
}

func (l *sendLog) get(r *SendRecord) SendRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	return copySend(r)
}

// recent returns up to limit sends, newest first.
func (l *sendLog) recent(limit int) []SendRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]SendRecord, 0, min(limit, len(l.sends)))
	for i := len(l.sends) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, copySend(l.sends[i]))
	}
	return out
}

// byHash finds the newest send that broadcast any of hashes.
func (l *sendLog) byHash(hashes []string) (SendRecord, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := len(l.sends) - 1; i >= 0; i-- {
		r := l.sends[i]
		for _, h := range hashes {
			if r.Tx != nil && strings.EqualFold(r.Tx.TxHash, h) {
				return copySend(r), true
			}
		}
	}
	return SendRecord{}, false
}

// relayed records a successful ibc_relay_tx for any of hashes on the send that broadcast it.
func (l *sendLog) relayed(hashes []string, next NextStep) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := len(l.sends) - 1; i >= 0; i-- {
		r := l.sends[i]
		for _, h := range hashes {
			if r.Status == sendMined && r.Tx != nil && strings.EqualFold(r.Tx.TxHash, h) {
				r.RelayRequested, r.Next = true, next
				return
			}
		}
	}
}

func copySend(r *SendRecord) SendRecord {
	c := *r
	if r.Tx != nil {
		tx := *r.Tx
		c.Tx = &tx
	}
	c.Packets = append([]ObservedPacket(nil), r.Packets...)
	c.Warnings = append([]string(nil), r.Warnings...)
	return c
}

// sendNextStep is the next step for a send in status st.
func sendNextStep(r SendRecord) NextStep {
	switch r.Status {
	case sendSubmitting:
		return NextStep{Tool: toolListSends, Reason: "Not broadcast yet; check this send again."}
	case sendBroadcast:
		return txNext(toolWait, r.Tx, "Waiting for the source tx to be mined.")
	case sendFailed:
		if r.Error != nil && r.Error.Code == CodeTxNotConfirmed && r.Tx != nil {
			return txNext(toolRelay, r.Tx,
				"Not mined within the confirm wait; it may mine later. Relay it (safe to repeat), then wait; do not send again.")
		}
		return NextStep{Reason: "The send failed; see error."}
	}
	return r.Next
}

// txNext is a call to tool for tx's chain_id and tx_hash.
func txNext(tool string, tx *TxRef, reason string) NextStep {
	if tx == nil {
		return NextStep{Reason: reason}
	}
	return NextStep{Tool: tool, Arguments: map[string]string{"chain_id": tx.ChainID, "tx_hash": tx.TxHash}, Reason: reason}
}

func (s *Server) listSends(
	_ context.Context,
	_ *mcp.CallToolRequest,
	in ListSendsInput,
) (*mcp.CallToolResult, ListSendsOutput, error) {
	limit := in.Limit
	if limit == 0 {
		limit = defaultSendsLimit
	}
	if limit < 1 || limit > maxSends {
		return nil, ListSendsOutput{}, NewError(CodeInvalidArgument, "limit must be between 1 and %d", maxSends)
	}
	sends := s.sends.recent(limit)
	for i := range sends {
		sends[i].Next = sendNextStep(sends[i])
	}
	return nil, ListSendsOutput{Sends: sends}, nil
}

// SendInput is the input of ibc_ift_send.
type SendInput struct {
	ChainID              string `json:"chain_id"                  jsonschema:"source chain"`
	IFT                  string `json:"ift,omitempty"                    jsonschema:"IFT token address; optional when the chain has one configured token"`
	ClientID             string `json:"client_id"                        jsonschema:"source client, e.g. base-0"`
	To                   string `json:"to"                               jsonschema:"receiver: hex address or signer alias"`
	Amount               string `json:"amount"                           jsonschema:"base units, digits only"`
	From                 string `json:"from,omitempty"                   jsonschema:"signer alias; defaults to the client's signer"`
	PacketTimeoutSeconds int    `json:"packet_timeout_seconds,omitempty" jsonschema:"packet lifetime from now; default 900"`
	Relay                string `json:"relay,omitempty"                  jsonschema:"auto (default): relay unless the client auto-relays; always; never"`
}

// SendOutput is the result of ibc_ift_send.
type SendOutput = SendRecord

// writeRequest is an ibc_ift_send call before authorization. ibc_ift_send is
// the only tool that signs.
type writeRequest struct {
	chainID, ift, from, to, amount, clientID, relay string
	timeout                                         time.Duration
}

// authorizedWrite is a send the relayer's client signer may make.
type authorizedWrite struct {
	from, to string
	amount   *big.Int
}

func (s *Server) iftSend(
	ctx context.Context,
	req *mcp.CallToolRequest,
	in SendInput,
) (*mcp.CallToolResult, SendOutput, error) {
	reqCtx := ctx
	ctx, cancel := s.bind(ctx)
	defer cancel()

	timeout := defaultSendTimeout
	if in.PacketTimeoutSeconds != 0 {
		timeout = time.Duration(in.PacketTimeoutSeconds) * time.Second
	}
	relayMode := in.Relay
	if relayMode == "" {
		relayMode = relayAuto
	}

	switch {
	case in.ClientID == "":
		return s.rejectSend(in, relayMode, NewError(CodeInvalidArgument, "client_id is required"))
	case timeout < minSendTimeout || timeout > maxSendTimeout:
		return s.rejectSend(in, relayMode, NewError(CodeInvalidArgument, "packet_timeout_seconds must be between %d and %d",
			int(minSendTimeout.Seconds()), int(maxSendTimeout.Seconds())))
	case relayMode != relayAuto && relayMode != relayAlways && relayMode != relayNever:
		return s.rejectSend(in, relayMode, NewError(CodeInvalidArgument, "relay must be auto, always or never"))
	}
	ift, err := s.resolveIFT(in.ChainID, in.IFT)
	if err != nil {
		return s.rejectSend(in, relayMode, err)
	}
	in.IFT = ift

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
	in.From = w.from
	display := s.displayAmount(ctx, in.ChainID, in.IFT, w.amount)

	if !s.beginWrite() {
		return s.rejectSend(in, relayMode, NewError(CodeRelayerUnavailable, "the MCP server is stopping"))
	}
	rec, err := s.beginAudit(auditRecord{
		Tool: toolSend, ChainID: in.ChainID, IFT: in.IFT, From: in.From, To: w.to,
		Amount: w.amount.String(), ClientID: in.ClientID, Relay: relayMode,
	})
	if err != nil {
		s.endWrite()
		return nil, SendOutput{}, err
	}
	st := s.sends.add(SendRecord{
		ID: rec.ID, Time: s.now().UTC(), ChainID: in.ChainID, ClientID: in.ClientID, IFT: in.IFT,
		From: in.From, To: w.to, Amount: w.amount.String(), AmountDisplay: display, Relay: relayMode, Status: sendSubmitting,
	})

	// The send outlives this call: it returns at broadcast, and mining and the
	// relay request finish in the background, bound only to the server's life.
	broadcast := make(chan struct{})
	var once sync.Once
	finished := make(chan struct{})
	go func() {
		defer s.endWrite()
		defer close(finished)
		s.runSend(rec, req, st, SendRequest{
			ChainID: in.ChainID, IFT: in.IFT, ClientID: in.ClientID, To: w.to, Amount: w.amount,
			From: in.From, Timeout: timeout, ConfirmTimeout: s.confirmTimeout,
			OnBroadcast: func(tx TxRef) {
				s.noteBroadcast(req, tx)
				s.sends.update(st, func(r *SendRecord) { r.Tx, r.Status = &tx, sendBroadcast })
				once.Do(func() { close(broadcast) })
			},
			OnReplaced: func(previous, replacement string) {
				s.noteReplaced(rec, req, in.ChainID, previous, replacement)
				s.sends.update(st, func(r *SendRecord) {
					if r.Status != sendMined {
						r.Tx = &TxRef{ChainID: in.ChainID, TxHash: replacement}
					}
				})
			},
		}, relayMode)
	}()
	select {
	case <-broadcast:
	case <-finished:
	case <-reqCtx.Done():
	}
	out := s.sends.get(st)
	if out.Status == sendFailed {
		return nil, SendOutput{}, out.Error
	}
	out.Next = sendNextStep(out)
	return nil, out, nil
}

// runSend submits the transfer, waits for mining and requests the relay, recording each step on st.
func (s *Server) runSend(rec auditRecord, req *mcp.CallToolRequest, st *SendRecord, sr SendRequest, relayMode string) {
	ctx := s.shutdown
	tx, err := s.chain.IFTSend(ctx, sr)
	// The approval is already spent. Keep a returned hash so a retry of the
	// same approval can show it; never invent one.
	if ref, ok := observedBroadcast(tx, err); ok {
		s.noteBroadcast(req, ref)
	}
	if err != nil {
		_ = s.finish(rec, err)
		te := asToolError(err, CodeChainRPCError)
		s.sends.update(st, func(r *SendRecord) {
			r.Status, r.Error = sendFailed, te
			if te.Tx != nil {
				tx := *te.Tx
				r.Tx = &tx
			}
		})
		return
	}

	var warnings []string
	var packets []ObservedPacket
	requestRelay, skippedForAutoRelay := s.planRelay(ctx, relayMode, sr.ChainID, sr.ClientID)
	relayed, relayFailed := false, false
	if requestRelay {
		got, err := s.relayer.Relay(ctx, tx.ChainID, tx.TxHash, nil)
		if err != nil {
			relayFailed = true
			warnings = append(warnings, "tx is mined but the relay request failed: "+
				asToolError(err, CodeRelayerError).Message)
		} else {
			relayed, packets = true, got
			for _, p := range packets {
				if p.Selection == SelectionUnconfigured {
					warnings = append(warnings, fmt.Sprintf(
						"packet %s/%s has no route on this relayer and will not be delivered by it",
						p.SourceClientID, p.Sequence))
				}
			}
		}
	}
	rec.Outcome = outcomeMined
	rec.TxHash = tx.TxHash
	if err := s.record(rec); err != nil {
		warnings = append(warnings, "audit log update failed: "+err.Error())
	}
	next := sendNext(relayMode, relayed, relayFailed, skippedForAutoRelay, packets)
	s.sends.update(st, func(r *SendRecord) {
		r.Tx, r.Status, r.RelayRequested, r.Packets = &tx, sendMined, relayed, packets
		r.Warnings = append(r.Warnings, warnings...)
		r.Next = next.at(&tx)
	})
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

// sendNext is the next step after a mined transfer. It waits only when a packet was selected,
// and names no tool when no packet is left for this relayer. Arguments are set by at.
func sendNext(mode string, requested, failed, skippedForAutoRelay bool, packets []ObservedPacket) NextStep {
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
	var reasons []string
	if len(unconfigured) > 0 {
		reasons = append(reasons, unconfiguredNext(unconfigured))
	}
	if len(packets) > 0 && len(unconfigured) == len(packets) {
		return NextStep{Reason: "The transfer is mined. " + reasons[0]}
	}
	var tool string
	switch {
	case failed:
		tool, reasons = toolRelay, append(reasons, nextRelayFailed)
	case mode == relayNever:
		tool, reasons = toolRelay, append(reasons, nextNever)
	case skippedForAutoRelay:
		tool, reasons = toolWait, append(reasons, nextAutoRelay)
	case !requested:
	case selected:
		tool, reasons = toolWait, append(reasons, nextWait)
	case len(unconfigured) > 0:
	case len(packets) == 0:
		tool, reasons = toolRelay, append(reasons, nextNoPackets)
	case notSelected == len(packets):
		tool, reasons = toolRelay, append(reasons, nextNotSelected)
	default:
		tool, reasons = toolRelay, append(reasons, nextNotQueued)
	}
	return NextStep{Tool: tool, Reason: strings.Join(reasons, " ")}
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
			CodePolicyDenied, "client %q on chain %q is not on this relayer; pick a connection end from ibc_get_context", w.clientID, w.chainID)
	}
	if w.from == "" {
		w.from = end.Signer
	}
	if w.from != end.Signer {
		return authorizedWrite{}, nil, NewError(
			CodePolicyDenied, "signer %q is not configured on client %q; omit from to use the client's signer", w.from, w.clientID)
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

	return authorizedWrite{from: w.from, to: to, amount: amount}, nil, nil
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
