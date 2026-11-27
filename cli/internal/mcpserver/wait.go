// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	waitDoneHint = "Every packet reached the until condition, including ack, timeout, rejection, and relay failure. " +
		"Read each packet state and summary before telling the user the transfer arrived. " +
		"succeeded is the delivered state. timed_out, rejected, and relay_failed are finished and failed. " +
		"Do not call ibc_ift_send again because a packet timed out or the wait timed out."
	waitNoPacketsHint = "No packets have been seen. Call ibc_relay_tx with this chain_id and tx_hash, " +
		"then call ibc_wait_for_packets again. Do not call ibc_ift_send again."
	waitNotSelectedHint = "Some packets are not_selected. Call ibc_relay_tx with this chain_id and tx_hash, " +
		"then call ibc_wait_for_packets again. Do not call ibc_ift_send again."
	waitMovingHint = "Packets are already moving. Call ibc_wait_for_packets again. Do not call ibc_ift_send again."
	// waitCanceledMessage names the next call, since a cancelled wait is not retryable.
	waitCanceledMessage = "The wait was cancelled. Call ibc_wait_for_packets again, not ibc_ift_send."
)

// WaitInput is the input of ibc_wait_for_packets.
type WaitInput struct {
	ChainID        string `json:"chain_id"                  jsonschema:"source chain"`
	TxHash         string `json:"tx_hash"                   jsonschema:"source tx"`
	Until          string `json:"until,omitempty"           jsonschema:"final (default): ack, timeout, rejection or relay failure; received: recv tx seen on the destination"`
	TimeoutSeconds int    `json:"timeout_seconds,omitempty" jsonschema:"how long to wait in seconds; default 600, maximum 1800"`
}

// WaitSummary counts packets by state.
type WaitSummary struct {
	Succeeded   int `json:"succeeded"`
	Pending     int `json:"pending"`
	TimedOut    int `json:"timed_out"`
	Rejected    int `json:"rejected"`
	RelayFailed int `json:"relay_failed"`
	NotSelected int `json:"not_selected"`
}

// WaitOutput is the result of ibc_wait_for_packets.
type WaitOutput struct {
	Done           bool           `json:"done"            jsonschema:"every packet reached the until condition, including ack, timeout, rejection, and relay failure. Read each packet state and summary before telling the user the transfer arrived. succeeded is the delivered state. timed_out, rejected, and relay_failed are finished and failed. Do not call ibc_ift_send again because a packet timed out or the wait timed out"`
	TimedOut       bool           `json:"timed_out"       jsonschema:"the wait ended before done; this is not an error. If no packets have been seen, call ibc_relay_tx, then call ibc_wait_for_packets again. If packets are already moving, call ibc_wait_for_packets again. A not_selected packet does not wait out this timeout. Do not call ibc_ift_send again"`
	ElapsedSeconds int            `json:"elapsed_seconds"`
	Summary        WaitSummary    `json:"summary"`
	Packets        []PacketStatus `json:"packets"`
	Hint           string         `json:"hint,omitempty"`
}

// progressNotifier reports packet state changes to clients that sent a
// progress token. Progress counts packets that reached the until condition.
type progressNotifier struct {
	req   *mcp.CallToolRequest
	token any
	step  float64
}

func (s *Server) waitForPackets(
	ctx context.Context,
	req *mcp.CallToolRequest,
	in WaitInput,
) (*mcp.CallToolResult, WaitOutput, error) {
	ctx, cancel := s.bind(ctx)
	defer cancel()

	if err := requireTxHash(in.TxHash); err != nil {
		return nil, WaitOutput{}, err
	}
	until := in.Until
	if until == "" {
		until = untilFinal
	}
	if until != untilFinal && until != untilReceived {
		return nil, WaitOutput{}, NewError(CodeInvalidArgument, "until must be final or received")
	}
	timeout := s.waitDefault
	if in.TimeoutSeconds > 0 {
		timeout = time.Duration(in.TimeoutSeconds) * time.Second
	}
	if timeout > s.waitMax {
		timeout = s.waitMax
	}

	start := time.Now()
	// waitCtx bounds each relayer query as well as the sleep between them, so
	// a slow or paginated query cannot hold the call past timeout_seconds.
	waitCtx, waitCancel := context.WithTimeout(ctx, timeout)
	defer waitCancel()
	ticker := time.NewTicker(s.poll)
	defer ticker.Stop()

	notifier := newProgressNotifier(req)
	seen := map[string]PacketState{}
	var packets []PacketStatus
	var lastErr *ToolError
	used := in.TxHash
	result := func(done, timedOut bool, hint string) (*mcp.CallToolResult, WaitOutput, error) {
		out := waitResult(packets, done, timedOut, start)
		out.Hint = hint
		if note := replacedNote(in.TxHash, used); note != "" {
			out.Hint = note + " " + hint
		}
		return nil, out, nil
	}

	for {
		got, gotHash, err := s.txPackets(waitCtx, in.ChainID, in.TxHash)
		if err == nil {
			packets, used, lastErr = got, gotHash, nil
			notifier.changes(ctx, seen, packets, until)
			if len(packets) > 0 && allReached(packets, until) {
				return result(true, false, waitDoneHint)
			}
			// not_selected is not in progress. Return the relay hint now
			// instead of holding the call until timeout_seconds.
			if countState(packets, StateNotSelected) > 0 {
				return result(false, false, waitNotSelectedHint)
			}
		}

		if ctx.Err() != nil {
			return nil, WaitOutput{}, waitCancelError(ctx.Err())
		}
		if waitCtx.Err() != nil {
			// The wait's own deadline, not a cancellation: a normal timed-out result.
			return result(false, true, waitTimeoutHint(packets, lastErr))
		}
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, WaitOutput{}, waitCancelError(err)
			}
			lastErr = asToolError(err, CodeRelayerError)
			if !lastErr.Retryable {
				return nil, WaitOutput{}, lastErr
			}
		}

		select {
		case <-ctx.Done():
			return nil, WaitOutput{}, waitCancelError(ctx.Err())
		case <-waitCtx.Done():
			return result(false, true, waitTimeoutHint(packets, lastErr))
		case <-ticker.C:
		}
	}
}

// waitCancelError makes a cancelled wait non-retryable, except a CHAIN_RPC_ERROR
// or RELAYER_UNAVAILABLE that is not itself a context error.
func waitCancelError(err error) *ToolError {
	var te *ToolError
	if errors.As(err, &te) && (te.Code == CodeChainRPCError || te.Code == CodeRelayerUnavailable) &&
		!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return te
	}
	return NewError(CodeRelayerError, "%s", waitCanceledMessage)
}

// txPackets lists a source tx's packets, trying each re-signed hash newest
// first, and returns the hash that had them.
func (s *Server) txPackets(ctx context.Context, chainID, txHash string) ([]PacketStatus, string, error) {
	for _, h := range s.replaced.family(txHash) {
		packets, err := s.txHashPackets(ctx, chainID, h)
		if err != nil {
			return nil, txHash, err
		}
		if len(packets) > 0 {
			return packets, h, nil
		}
	}
	return []PacketStatus{}, txHash, nil
}

// txHashPackets lists every packet of one source tx hash across all pages.
func (s *Server) txHashPackets(ctx context.Context, chainID, txHash string) ([]PacketStatus, error) {
	filter := PacketFilter{SourceChainID: chainID, SourceTxHash: txHash}
	all := []PacketStatus{}
	seen := map[string]struct{}{}
	cursor := ""
	for {
		if _, dup := seen[cursor]; dup {
			return nil, NewError(CodeRelayerError, "packet page cursor %q repeated", cursor)
		}
		seen[cursor] = struct{}{}
		page, err := s.relayer.Packets(ctx, filter, maxPacketsLimit, cursor)
		if err != nil {
			return nil, err
		}
		all = append(all, page.Packets...)
		if !page.HasMore || page.NextCursor == "" {
			return all, nil
		}
		cursor = page.NextCursor
	}
}

func reached(p PacketStatus, until string) bool {
	if p.Final {
		return true
	}
	return until == untilReceived && p.RecvTx != nil
}

func allReached(packets []PacketStatus, until string) bool {
	for _, p := range packets {
		if !reached(p, until) {
			return false
		}
	}
	return true
}

// waitTimeoutHint is the next step after a wait that ended without done.
func waitTimeoutHint(packets []PacketStatus, lastErr *ToolError) string {
	switch {
	case lastErr != nil:
		msg := strings.TrimSpace(lastErr.Message)
		if msg != "" && !strings.HasSuffix(msg, ".") {
			msg += "."
		}
		return "relayer was unreachable at the last poll: " + msg +
			" Call ibc_wait_for_packets again. Do not call ibc_ift_send again."
	case len(packets) == 0:
		return waitNoPacketsHint
	case countState(packets, StateNotSelected) > 0:
		return waitNotSelectedHint
	default:
		return waitMovingHint
	}
}

func countState(packets []PacketStatus, st PacketState) int {
	n := 0
	for _, p := range packets {
		if p.State == st {
			n++
		}
	}
	return n
}

func waitResult(packets []PacketStatus, done, timedOut bool, start time.Time) WaitOutput {
	if packets == nil {
		packets = []PacketStatus{}
	}
	out := WaitOutput{
		Done: done, TimedOut: timedOut, Packets: packets,
		ElapsedSeconds: int(time.Since(start).Seconds()),
	}
	for _, p := range packets {
		switch p.State {
		case StateSucceeded:
			out.Summary.Succeeded++
		case StatePending:
			out.Summary.Pending++
		case StateTimedOut:
			out.Summary.TimedOut++
		case StateRejected:
			out.Summary.Rejected++
		case StateRelayFailed:
			out.Summary.RelayFailed++
		case StateNotSelected:
			out.Summary.NotSelected++
		}
	}
	return out
}

func newProgressNotifier(req *mcp.CallToolRequest) *progressNotifier {
	n := &progressNotifier{req: req}
	if req != nil && req.Params != nil {
		n.token = req.Params.GetProgressToken()
	}
	return n
}

// changes notifies one progress step per packet whose state differs from seen.
func (n *progressNotifier) changes(
	ctx context.Context,
	seen map[string]PacketState,
	packets []PacketStatus,
	until string,
) {
	if n.token == nil || n.req.Session == nil {
		return
	}
	done := 0
	for _, p := range packets {
		if reached(p, until) {
			done++
		}
	}
	for _, p := range packets {
		key := p.SourceClientID + "/" + p.Sequence
		prev, known := seen[key]
		if known && prev == p.State {
			continue
		}
		seen[key] = p.State

		msg := fmt.Sprintf("packet %s: %s", key, p.State)
		if known {
			msg = fmt.Sprintf("packet %s: %s -> %s", key, prev, p.State)
		}
		// Progress must strictly increase and packets can appear mid-wait, so it
		// counts state changes; the message carries the packet tally.
		n.step++
		_ = n.req.Session.NotifyProgress(ctx, &mcp.ProgressNotificationParams{
			ProgressToken: n.token,
			Message:       fmt.Sprintf("%s (%d/%d packets done)", msg, done, len(packets)),
			Progress:      n.step,
		})
	}
}
