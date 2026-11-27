// SPDX-License-Identifier: Apache-2.0

package mcpserver

// Packet states, mirroring the relayer's PacketState enum.
const (
	StateUnspecified PacketState = "unspecified"
	StateNotSelected PacketState = "not_selected"
	StatePending     PacketState = "pending"
	StateSucceeded   PacketState = "succeeded"
	StateTimedOut    PacketState = "timed_out"
	StateRejected    PacketState = "rejected"
	StateRelayFailed PacketState = "relay_failed"
)

// Packet selections, mirroring the relayer's PacketSelection enum.
const (
	SelectionUnspecified  PacketSelection = "unspecified"
	SelectionSelected     PacketSelection = "selected"
	SelectionNotSelected  PacketSelection = "not_selected"
	SelectionUnconfigured PacketSelection = "unconfigured"
)

// PacketStates lists every state accepted as a filter, in lifecycle order.
var PacketStates = []PacketState{
	StateNotSelected, StatePending, StateSucceeded, StateTimedOut, StateRejected, StateRelayFailed,
}

// TxRef identifies a transaction on a chain.
type TxRef struct {
	ChainID string `json:"chain_id" jsonschema:"EVM chain ID"`
	TxHash  string `json:"tx_hash"  jsonschema:"transaction hash, 0x-prefixed"`
}

// PacketState is the relay state of a packet, as reported by the relayer.
type PacketState string

// PacketStatus is one packet and the transactions that moved it.
// A packet is identified by send_tx.chain_id, source_client_id and sequence.
type PacketStatus struct {
	SourceClientID string      `json:"source_client_id"`
	Sequence       string      `json:"sequence"             jsonschema:"packet sequence number, as a decimal string"`
	State          PacketState `json:"state"`
	Final          bool        `json:"final"                jsonschema:"true when the packet will not change state again"`
	SendTx         *TxRef      `json:"send_tx,omitempty"    jsonschema:"source-chain SendPacket transaction"`
	RecvTx         *TxRef      `json:"recv_tx,omitempty"    jsonschema:"destination-chain receive transaction"`
	AckTx          *TxRef      `json:"ack_tx,omitempty"     jsonschema:"source-chain acknowledgement transaction"`
	TimeoutTx      *TxRef      `json:"timeout_tx,omitempty" jsonschema:"source-chain timeout transaction"`
}

// PacketSelection says whether a relayer will deliver a packet it was asked about.
type PacketSelection string

// ObservedPacket is a send packet found in a transaction by Relay.
type ObservedPacket struct {
	SourceClientID string          `json:"source_client_id"`
	Sequence       string          `json:"sequence"`
	Selection      PacketSelection `json:"selection"        jsonschema:"selected: queued for delivery; not_selected: routed but not picked; unconfigured: this relayer has no route for it"`
}

// PacketSelector picks one packet of a transaction.
type PacketSelector struct {
	SourceClientID string `json:"source_client_id"`
	Sequence       string `json:"sequence"         jsonschema:"packet sequence number, as a decimal string"`
}

// PacketFilter narrows a packet listing; empty fields match everything.
type PacketFilter struct {
	SourceChainID       string
	DestinationChainID  string
	SourceClientID      string
	DestinationClientID string
	SourceTxHash        string
	Sequence            *uint64
	State               PacketState
}

// PacketsPage is one page of a packet listing, newest first.
type PacketsPage struct {
	Packets    []PacketStatus `json:"packets"`
	HasMore    bool           `json:"has_more"`
	NextCursor string         `json:"next_cursor,omitempty"`
}

// ChainInfo describes a configured chain.
type ChainInfo struct {
	ChainID string `json:"chain_id"`
	Type    string `json:"type"`
}

// ClientEndInfo is one side of a relayed connection.
type ClientEndInfo struct {
	ChainID   string `json:"chain_id"`
	ClientID  string `json:"client_id"`
	Signer    string `json:"signer"     jsonschema:"signer alias this relayer uses for this client"`
	AutoRelay bool   `json:"auto_relay" jsonschema:"true when the relayer picks up packets from this end without a relay request"`
}

// ConnectionInfo is a relayed connection between two light clients.
type ConnectionInfo struct {
	Alias   string        `json:"alias"`
	ClientA ClientEndInfo `json:"client_a"`
	ClientB ClientEndInfo `json:"client_b"`
}

// SignerInfo is a configured signer the agent may name in "from".
type SignerInfo struct {
	Alias   string `json:"alias"`
	Address string `json:"address,omitempty"`
}

// TokenInfo is IFT token metadata.
type TokenInfo struct {
	Symbol   string `json:"symbol"`
	Decimals uint8  `json:"decimals"`
}

// Final reports whether no further state change is expected.
func (s PacketState) Final() bool {
	switch s {
	case StateSucceeded, StateTimedOut, StateRejected, StateRelayFailed:
		return true
	default:
		return false
	}
}
