// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"math/big"
	"time"
)

// ChainBackend performs the chain-side work behind the tools. Implementations
// reach the chain and the relayer's signer. The server never sees private keys.
type ChainBackend interface {
	// Chains, connections and signers as configured in ibc.yml.
	Chains(ctx context.Context) ([]ChainInfo, error)
	Connections(ctx context.Context) ([]ConnectionInfo, error)
	Signers(ctx context.Context) ([]SignerInfo, error)

	// ResolveAddress returns the hex address for a hex address or a signer alias.
	ResolveAddress(ctx context.Context, value string) (string, error)

	TokenInfo(ctx context.Context, chainID, ift string) (TokenInfo, error)
	IFTBalance(ctx context.Context, chainID, ift, address string) (*big.Int, error)

	// IFTSend broadcasts, then waits until the tx is mined or confirmTimeout
	// passes. It returns *ToolError with TX_REVERTED or TX_NOT_CONFIRMED
	// (both carrying the tx) once a tx has been broadcast.
	IFTSend(ctx context.Context, req SendRequest) (TxRef, error)
}

// RelayerBackend is the relayer that runs this MCP server.
type RelayerBackend interface {
	Relay(ctx context.Context, chainID, txHash string, packets []PacketSelector) ([]ObservedPacket, error)
	Packets(ctx context.Context, filter PacketFilter, limit uint32, cursor string) (PacketsPage, error)
}

// SendRequest is an IFT transfer the relayer's client signer may send.
type SendRequest struct {
	ChainID        string
	IFT            string
	ClientID       string
	To             string // hex address on the counterparty chain
	Amount         *big.Int
	From           string // signer alias
	Timeout        time.Duration
	ConfirmTimeout time.Duration
}
