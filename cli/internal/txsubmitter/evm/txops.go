// SPDX-License-Identifier: Apache-2.0

package evm

import (
	"context"
	"io"
	"math/big"
	"net"
	"net/url"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/pkg/errors"

	chainsevm "github.com/cosmos/ibc/cli/internal/chains/evm"
	"github.com/cosmos/ibc/cli/internal/service/signer"
)

func estimateFees(
	ctx context.Context,
	eth ETHClient,
	baseFee *big.Int,
) (fee, tip *big.Int, err error) {
	if baseFee == nil {
		return nil, nil, errors.New("chain must support EIP-1559")
	}
	tip, err = eth.SuggestGasTipCap(ctx)
	if err != nil {
		return nil, nil, errors.Wrap(err, "getting suggested gas tip cap")
	}
	fee = new(big.Int).Add(tip, new(big.Int).Mul(baseFee, big.NewInt(2)))
	return fee, tip, nil
}

func capFees(fee, tip, limit *big.Int) (*big.Int, *big.Int) {
	if fee.Cmp(limit) > 0 {
		fee = limit
	}
	if tip.Cmp(fee) > 0 {
		tip = fee
	}
	return fee, tip
}

func signTransaction(
	ctx context.Context,
	tx *types.Transaction,
	chainSigner signer.Signer,
	ethSigner types.Signer,
	address common.Address,
) (*types.Transaction, error) {
	sig, err := chainSigner.Sign(ctx, ethSigner.Hash(tx).Bytes())
	if err != nil {
		return nil, errors.Wrapf(err, "signing transaction with address %s", address)
	}
	signed, err := tx.WithSignature(ethSigner, sig)
	if err != nil {
		return nil, errors.Wrap(err, "attaching transaction signature")
	}
	sender, err := types.Sender(ethSigner, signed)
	if err != nil {
		return nil, errors.Wrap(err, "recovering transaction sender")
	}
	if sender != address {
		return nil, errors.New("transaction signature does not match configured sender")
	}
	return signed, nil
}

func broadcast(ctx context.Context, eth ETHClient, tx *types.Transaction) error {
	err := eth.SendTransaction(ctx, tx)
	if err != nil {
		message := strings.ToLower(err.Error())
		if strings.Contains(message, "already known") || strings.HasPrefix(message, "known transaction") {
			return nil
		}
	}
	return err
}

func isReplacementUnderpriced(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "replacement transaction underpriced")
}

// broadcastMayHaveLanded reports whether SendTransaction failed without the
// node saying it rejected the transaction. A timeout that is not a dial, a
// cancel, EOF, or a read or write error can mean the node already accepted
// it. A refused dial never opened a connection. An HTTP 5xx can arrive after
// the node has pooled the transaction. A TLS handshake timeout, and a remote
// or local TLS alert, are returned before the POST body is written. The same
// failure wrapped as a broken HTTP/1 transport can mean bytes were already
// written. HTTP/2 replaces a later read with "server sent GOAWAY and closed
// the connection" after the server has received the stream. A graceful
// shutdown GOAWAY means the server has not received it.
func broadcastMayHaveLanded(err error) bool {
	if err == nil {
		return false
	}
	// Go replaces the read error with this text once the server has received
	// the stream. It is not a net.Error and does not unwrap to that read.
	// Checked on the outer error so a *url.Error wrap is included.
	if http2ServerClosedConnection(err) {
		return true
	}
	// A dial timeout is still a dial: the signed transaction was not written.
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var httpErr rpc.HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode >= 500
	}
	// *url.Error implements net.Error, so it has to be judged by its cause
	// before the net.Error check. A handshake failure would otherwise look
	// like a broadcast that may have landed.
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return broadcastMayHaveLanded(urlErr.Err)
	}
	// addTLS returns these before the request body is written. A later alert
	// on a connection that already accepted bytes is wrapped by net/http.
	if !transportConnectionBroken(err) && tlsFailedBeforeBody(err) {
		return false
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

// transportConnectionBroken reports net/http's wrap once a broken HTTP/1
// connection may already have written the request.
func transportConnectionBroken(err error) bool {
	return err != nil && strings.Contains(err.Error(), "net/http: HTTP/1.x transport connection broken")
}

// http2ServerClosedConnection reports Go's HTTP/2 error after the server has
// received the stream and then sent GOAWAY. The graceful-shutdown text is a
// different error: the server has not received the stream, and the HTTP/2
// client retries that request itself.
func http2ServerClosedConnection(err error) bool {
	return err != nil && strings.Contains(err.Error(), "http2: server sent GOAWAY and closed the connection")
}

// tlsFailedBeforeBody reports a TLS handshake timeout or alert. The timeout is
// net/http's unexported net.Error with this string. crypto/tls reports an alert
// as a net.OpError with Op "remote error" or "local error", often wrapped in
// another net.Error.
func tlsFailedBeforeBody(err error) bool {
	for cur := err; cur != nil; {
		if cur.Error() == "net/http: TLS handshake timeout" {
			return true
		}
		next := errors.Unwrap(cur)
		if next == nil || next == cur {
			break
		}
		cur = next
	}
	var opErr *net.OpError
	return errors.As(err, &opErr) && (opErr.Op == "remote error" || opErr.Op == "local error")
}

func (c *TxSubmitter) prepareTransaction(
	ctx context.Context,
	destination string,
	data []byte,
	nonce uint64,
	fee, tip *big.Int,
) (*types.DynamicFeeTx, error) {
	if !common.IsHexAddress(destination) {
		return nil, errors.New("invalid transaction destination")
	}
	to := common.HexToAddress(destination)
	code, err := c.eth.PendingCodeAt(ctx, to)
	if err != nil {
		return nil, errors.Wrapf(err, "getting code at %s", to)
	}
	if len(code) == 0 {
		return nil, errors.Errorf("no contract code at %s", to)
	}
	// Pending state may already include the transaction this submission replaces.
	gas, err := c.eth.EstimateGasAtBlock(
		ctx,
		ethereum.CallMsg{From: c.address, To: &to, Data: data, GasFeeCap: fee, GasTipCap: tip},
		nil,
	)
	if err != nil {
		return nil, errors.Wrap(chainsevm.ExplainRevert(err), "estimating gas")
	}
	return &types.DynamicFeeTx{
		ChainID:   c.ethSigner.ChainID(),
		Nonce:     nonce,
		To:        &to,
		Data:      data,
		Gas:       gas,
		GasFeeCap: fee,
		GasTipCap: tip,
	}, nil
}

// Round up by 20%, including when the previous tip was zero.
func bump(v *big.Int) *big.Int {
	out := new(big.Int).Mul(v, big.NewInt(120))
	out.Add(out, big.NewInt(99))
	out.Div(out, big.NewInt(100))
	return maxBig(out, new(big.Int).Add(v, big.NewInt(1)))
}

func maxBig(a, b *big.Int) *big.Int {
	if a.Cmp(b) >= 0 {
		return a
	}
	return b
}
