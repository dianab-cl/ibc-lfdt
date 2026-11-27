// SPDX-License-Identifier: Apache-2.0

// Package evm signs and broadcasts EVM transactions.
package evm

import (
	"context"
	"log/slog"
	"math/big"
	"slices"
	"sync"
	"time"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/pkg/errors"

	chainsevm "github.com/lfdt-ibc/ibc/cli/internal/chains/evm"
	"github.com/lfdt-ibc/ibc/cli/internal/service/signer"
	v2 "github.com/lfdt-ibc/ibc/cli/internal/types/v2"
)

// Transaction limits.
const (
	DefaultPendingTxTimeout = 2 * time.Minute
	// requestTimeout bounds Submit and ShouldRetry.
	requestTimeout = 10 * time.Second
	// maxInFlight limits outstanding nonces per sender; stalled nonces can still be reused.
	maxInFlight = 16
	// maxBroadcastAttempts limits broadcasts per Submit, including fee replacements.
	maxBroadcastAttempts = 8
	// feeCeilingMultiplier caps fees relative to the fresh estimate for each Submit.
	feeCeilingMultiplier = 4
	// receiptPollInterval is how often Confirm checks for a mined receipt.
	receiptPollInterval = 200 * time.Millisecond
)

// ChainOptions controls transaction retry timing.
type ChainOptions struct {
	// PendingTxTimeout sets the wait before retrying a missing transaction or reusing a stalled nonce.
	PendingTxTimeout time.Duration
}

// ETHClient go-ethereum methods used by the EVM tx submitter.
type ETHClient interface {
	BalanceAt(context.Context, common.Address, *big.Int) (*big.Int, error)
	HeaderByNumber(context.Context, *big.Int) (*types.Header, error)
	SuggestGasTipCap(context.Context) (*big.Int, error)
	PendingCodeAt(context.Context, common.Address) ([]byte, error)
	CallContract(context.Context, ethereum.CallMsg, *big.Int) ([]byte, error)
	EstimateGasAtBlock(context.Context, ethereum.CallMsg, *big.Int) (uint64, error)
	NonceAt(context.Context, common.Address, *big.Int) (uint64, error)
	PendingNonceAt(context.Context, common.Address) (uint64, error)
	SendTransaction(context.Context, *types.Transaction) error
	TransactionReceipt(context.Context, common.Hash) (*types.Receipt, error)
}

// TxSubmitter simulates, signs, and broadcasts transactions on one EVM chain.
type TxSubmitter struct {
	chainID                        string
	eth                            ETHClient
	signer                         signer.Signer
	address                        common.Address
	ethSigner                      types.Signer
	opts                           ChainOptions
	mu                             sync.Mutex
	nextNonce                      uint64
	lowestUnminedNonceHeight       uint64
	lowestUnminedNonce             uint64
	lowestUnminedNoncePendingSince time.Time
	lastAttemptedFeeCap            *big.Int
	lastAttemptedTipCap            *big.Int
	// confirmReserved nonces were broadcast by Confirm. selectNonce does not
	// hand one to a later intent while PendingNonceAt is still above it. Once
	// the lowest one has stalled, submitLocked re-signs its intent at higher fees.
	confirmReserved map[uint64]*reservation
}

// reservation is the transfer Confirm broadcast at one nonce.
type reservation struct {
	intent v2.TxIntent
	// hashes are the transactions signed for intent at this nonce, oldest first.
	// A stall replacement appends one.
	hashes []string
	// onReplace, when set, is told each replacement hash. It runs on its own
	// goroutine, without c.mu, and can fire after Confirm has returned.
	onReplace ReplaceFunc
	// waiting is set while Confirm waits for a receipt. The gas metrics keep
	// the hashes until that wait ends, so the mined one is still counted when
	// selectNonce drops the reservation first.
	waiting bool
}

// ReplaceFunc receives the previous and the new hash when a Confirm transfer
// is re-signed at the same nonce. Only one of them can be mined.
type ReplaceFunc func(previous, replacement string)

// ErrBusy leaves the packet available for a later dispatch.
var ErrBusy = errors.New("transaction submission capacity reached")

func NewFromRPC(chainID, rpcURL string, chainSigner signer.Signer, opts ChainOptions) (*TxSubmitter, error) {
	eth, err := chainsevm.Dial(chainID, rpcURL)
	if err != nil {
		return nil, err
	}
	return New(chainID, eth, chainSigner, opts)
}

func New(chainID string, eth ETHClient, chainSigner signer.Signer, opts ChainOptions) (*TxSubmitter, error) {
	if opts.PendingTxTimeout == 0 {
		opts.PendingTxTimeout = DefaultPendingTxTimeout
	}
	if opts.PendingTxTimeout <= 0 {
		return nil, errors.New("invalid pending transaction timeout")
	}
	id, ok := new(big.Int).SetString(chainID, 10)
	if !ok || id.Sign() <= 0 {
		return nil, errors.Errorf("invalid EVM chain ID %q", chainID)
	}
	if chainSigner.Type() != signer.ECDSA {
		return nil, errors.New("EVM transactions require an ECDSA signer")
	}
	pub, err := crypto.DecompressPubkey(chainSigner.PublicKey())
	if err != nil {
		return nil, errors.Wrapf(err, "decompressing signer public key for chain %q", chainID)
	}
	c := &TxSubmitter{
		chainID: chainID, eth: eth, signer: chainSigner,
		address: crypto.PubkeyToAddress(*pub), ethSigner: types.LatestSignerForChainID(id), opts: opts,
	}
	metrics.setWallet(chainID, c.address.Hex(), eth)
	return c, nil
}

func (c *TxSubmitter) Submit(ctx context.Context, intent v2.TxIntent) (_ *v2.Submission, err error) {
	defer func() {
		metrics.attempt(ctx, c.chainID, c.address.Hex(), err)
		if err != nil {
			level := slog.LevelWarn
			if errors.Is(err, ErrBusy) {
				level = slog.LevelInfo
			}
			slog.Log(ctx, level, "Transaction submission unsuccessful",
				"chainID", c.chainID, "sender", c.address.Hex(), "err", err.Error())
		}
	}()
	c.mu.Lock()
	defer c.mu.Unlock()
	// The relayer retries a lost broadcast itself. Do not reserve the nonce here:
	// the next attempt reuses it unless the node already reports it pending.
	sub, _, err := c.submitLocked(ctx, intent, false)
	if err != nil {
		return nil, err
	}
	return sub, nil
}

// Confirm signs and broadcasts intent with this submitter's nonce and signer,
// then waits for the receipt. The nonce is reserved before that wait and is
// not given to a later intent while the node still reports it pending, so the
// signer lock is not held during the wait. Other nonces on this account can
// proceed. If the transfer is still pending after PendingTxTimeout, the next
// submission on this account re-signs the same intent at that nonce with
// higher fees, which can happen after Confirm returns; onReplace, when not
// nil, is told each such hash. The returned hash is the one that was mined
// or, when none was, the latest one signed.
//
// A non-empty hash with a non-nil error means the transaction was signed and
// the broadcast may have reached the node. The nonce is reserved in that case.
// Callers must report that hash and must not treat it as a failure with no
// transaction.
func (c *TxSubmitter) Confirm(
	ctx context.Context, intent v2.TxIntent, confirmTimeout time.Duration, onReplace ReplaceFunc,
) (hash string, receipt *types.Receipt, err error) {
	defer func() {
		metrics.attempt(ctx, c.chainID, c.address.Hex(), err)
		if err != nil && hash == "" {
			slog.WarnContext(ctx, "Transaction submission unsuccessful",
				"chainID", c.chainID, "sender", c.address.Hex(), "err", err.Error())
		}
	}()
	c.mu.Lock()

	// If this broadcast is the lowest unmined nonce, noteAccepted moves this
	// timestamp. A failed wait puts it back to now so the next submission does
	// not immediately replace a transfer that may still be in flight.
	pendingSince := c.lowestUnminedNoncePendingSince
	sub, nonce, submitErr := c.submitLocked(ctx, intent, true)
	if sub == nil {
		c.mu.Unlock()
		return "", nil, submitErr
	}
	hash = sub.TxHash
	// Hold the reservation itself: selectNonce deletes the map entry once the
	// nonce is mined, which can happen before this wait sees the replacement
	// that was mined.
	reserved := c.confirmReserved[nonce]
	if reserved != nil {
		reserved.onReplace = onReplace
		reserved.waiting = true
	}
	restartStallTimer := nonce == c.lowestUnminedNonce && !c.lowestUnminedNoncePendingSince.Equal(pendingSince)
	if confirmTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, confirmTimeout)
		defer cancel()
	}
	// Other submissions can use later nonces while this receipt is outstanding.
	// confirmReserved keeps them off this nonce until the node reports it free.
	c.mu.Unlock()
	hash, receipt, err = c.waitReceipt(ctx, reserved, hash)
	c.mu.Lock()
	defer c.mu.Unlock()
	// Count the mined hash, then forget the rest of the group. A transfer that
	// mines after this wait is not counted.
	if err == nil {
		metrics.endTx(c.chainID, receipt)
	}
	c.forgetWaitedHashes(reserved, hash)

	if err != nil {
		if restartStallTimer && c.lowestUnminedNonce == nonce {
			c.lowestUnminedNoncePendingSince = time.Now()
		}
		if submitErr != nil {
			err = errors.Wrap(submitErr, err.Error())
		}
		slog.WarnContext(ctx, "Transaction broadcast but not confirmed",
			"chainID", c.chainID, "sender", c.address.Hex(), "txHash", hash)
		return hash, nil, err
	}
	return hash, receipt, nil
}

// submitLocked signs and broadcasts. The caller holds c.mu.
// When reserveUnconfirmed is set and the node may have accepted the
// transaction, the nonce is recorded the way a successful broadcast is and
// the submission is returned together with the broadcast error.
func (c *TxSubmitter) submitLocked(
	ctx context.Context, intent v2.TxIntent, reserveUnconfirmed bool,
) (*v2.Submission, uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	nonce, head, err := c.selectNonce(ctx, time.Now())
	if err != nil {
		return nil, 0, err
	}
	if r, ok := c.confirmReserved[nonce]; ok {
		// selectNonce only returns a reserved nonce when that transfer has
		// stalled. Replace it, then give intent the next nonce. A transfer that
		// now reverts cannot be re-signed and would revert if mined, so intent
		// takes its nonce instead of queueing behind it.
		if c.replaceStalledLocked(ctx, nonce, head, r) {
			if nonce, head, err = c.selectNonce(ctx, time.Now()); err != nil {
				return nil, 0, err
			}
		}
	}
	return c.broadcastLocked(ctx, intent, nonce, head, reserveUnconfirmed)
}

// replaceStalledLocked re-signs the Confirm transfer at nonce, pending past
// PendingTxTimeout, with the same intent and higher fees. Without this a fee
// spike leaves the transfer, and every later nonce from this signer, stuck
// until the node evicts it. It returns false when the transfer now reverts in
// simulation: the reservation is dropped and the caller may use nonce. The
// caller holds c.mu.
func (c *TxSubmitter) replaceStalledLocked(
	ctx context.Context, nonce uint64, head *types.Header, r *reservation,
) (keepNonce bool) {
	previous := r.hashes[len(r.hashes)-1]
	sub, _, err := c.broadcastLocked(ctx, r.intent, nonce, head, true)
	if sub == nil && errors.Is(err, ErrEstimationReverted) {
		c.dropReservation(nonce)
		slog.WarnContext(ctx, "Stalled confirmed transaction now reverts; its nonce goes to the next transaction",
			"chainID", c.chainID, "sender", c.address.Hex(), "nonce", nonce, "txHash", previous, "err", err)
		return false
	}
	if sub == nil {
		// Try again after another PendingTxTimeout. The pending transfer keeps
		// the nonce meanwhile.
		c.lowestUnminedNoncePendingSince = time.Now()
		slog.WarnContext(ctx, "Stalled confirmed transaction not replaced",
			"chainID", c.chainID, "sender", c.address.Hex(), "nonce", nonce, "txHash", previous, "err", err)
		return true
	}
	metrics.forgetTx(c.chainID, previous)
	slog.WarnContext(ctx, "Stalled confirmed transaction replaced with higher fees",
		"chainID", c.chainID, "sender", c.address.Hex(), "nonce", nonce,
		"replacedTxHash", previous, "txHash", sub.TxHash)
	return true
}

// broadcastLocked signs intent at nonce and broadcasts it, raising fees while
// the node reports the replacement underpriced. The caller holds c.mu.
func (c *TxSubmitter) broadcastLocked(
	ctx context.Context, intent v2.TxIntent, nonce uint64, head *types.Header, reserveUnconfirmed bool,
) (*v2.Submission, uint64, error) {
	fee, tip, err := estimateFees(ctx, c.eth, head.BaseFee)
	if err != nil {
		return nil, 0, err
	}
	feeCeiling := new(big.Int).Mul(fee, big.NewInt(feeCeilingMultiplier))
	if nonce == c.lowestUnminedNonce && c.lastAttemptedFeeCap != nil {
		fee, tip = maxBig(fee, bump(c.lastAttemptedFeeCap)), maxBig(tip, bump(c.lastAttemptedTipCap))
	}
	fee, tip = capFees(fee, tip, feeCeiling)
	txData, err := c.prepareTransaction(ctx, intent.To, intent.Data, nonce, fee, tip)
	if err != nil {
		return nil, 0, err
	}
	for range maxBroadcastAttempts {
		txData.GasFeeCap, txData.GasTipCap = fee, tip
		tx, err := signTransaction(ctx, types.NewTx(txData), c.signer, c.ethSigner, c.address)
		if err != nil {
			return nil, 0, err
		}
		submittedAt := time.Now().UTC()
		err = broadcast(ctx, c.eth, tx)
		if err == nil {
			c.noteAccepted(nonce, fee, tip, reserveFor(reserveUnconfirmed, intent, tx))
			metrics.startTx(c.chainID, c.address.Hex(), tx.Hash().Hex())
			slog.InfoContext(ctx, "Transaction submitted", c.transactionAttributes(tx, head.BaseFee)...)
			return &v2.Submission{
				TxHash:         tx.Hash().Hex(),
				SubmittedAt:    submittedAt,
				RelayerAddress: c.address.Hex(),
			}, nonce, nil
		}
		slog.WarnContext(ctx, "Transaction broadcast rejected or unconfirmed",
			append(c.transactionAttributes(tx, head.BaseFee), "err", chainsevm.ExplainRevert(err).Error())...)
		if !isReplacementUnderpriced(err) {
			wrapped := errors.Wrapf(err, "broadcasting transaction %s at nonce %d", tx.Hash(), nonce)
			if reserveUnconfirmed && broadcastMayHaveLanded(err) {
				c.noteAccepted(nonce, fee, tip, reserveFor(true, intent, tx))
				metrics.startTx(c.chainID, c.address.Hex(), tx.Hash().Hex())
				return &v2.Submission{
					TxHash:         tx.Hash().Hex(),
					SubmittedAt:    submittedAt,
					RelayerAddress: c.address.Hex(),
				}, nonce, wrapped
			}
			return nil, 0, wrapped
		}
		if nonce == c.lowestUnminedNonce {
			c.lastAttemptedFeeCap, c.lastAttemptedTipCap = fee, tip
		}
		nextFee, nextTip := capFees(bump(fee), bump(tip), feeCeiling)
		if nextFee.Cmp(fee) == 0 && nextTip.Cmp(tip) == 0 {
			slog.WarnContext(
				ctx,
				"Transaction replacement fee ceiling reached",
				append(
					c.transactionAttributes(tx, head.BaseFee),
					"feeCeilingWei",
					feeCeiling.String(),
					"err",
					err.Error(),
				)...)
			return nil, 0, errors.Wrapf(err,
				"fee ceiling %s wei (%dx fresh estimate) reached for nonce %d",
				feeCeiling, feeCeilingMultiplier, nonce)
		}
		fee, tip = nextFee, nextTip
	}
	return nil, 0, errors.Errorf(
		"replacement still underpriced after %d attempts at nonce %d",
		maxBroadcastAttempts,
		nonce,
	)
}

// reserveFor is the reservation noteAccepted records for tx, or nil when the
// nonce is not reserved.
func reserveFor(reserve bool, intent v2.TxIntent, tx *types.Transaction) *reservation {
	if !reserve {
		return nil
	}
	return &reservation{intent: intent, hashes: []string{tx.Hash().Hex()}}
}

// noteAccepted records a broadcast the node may now have. The caller holds c.mu.
// A non-nil reserve keeps the nonce for Confirm until the node reports it mined
// or free. A replacement at an already reserved nonce adds its hash.
func (c *TxSubmitter) noteAccepted(nonce uint64, fee, tip *big.Int, reserve *reservation) {
	c.nextNonce = max(c.nextNonce, nonce+1)
	if reserve != nil {
		if c.confirmReserved == nil {
			c.confirmReserved = make(map[uint64]*reservation)
		}
		if r, ok := c.confirmReserved[nonce]; ok {
			previous := r.hashes[len(r.hashes)-1]
			r.hashes = append(r.hashes, reserve.hashes...)
			if r.onReplace != nil {
				// Off the signer lock: the callback may write the audit log.
				go r.onReplace(previous, reserve.hashes[0])
			}
		} else {
			c.confirmReserved[nonce] = reserve
		}
	}
	if nonce == c.lowestUnminedNonce {
		c.lastAttemptedFeeCap, c.lastAttemptedTipCap = fee, tip
		c.lowestUnminedNoncePendingSince = time.Now()
	}
}

// dropReservation deletes the reservation at nonce and forgets its hashes in
// the gas metrics. While Confirm still waits on it, the hashes are left for
// Confirm, which counts the mined one and forgets the rest. The caller holds
// c.mu.
func (c *TxSubmitter) dropReservation(nonce uint64) {
	r, ok := c.confirmReserved[nonce]
	if !ok {
		return
	}
	if !r.waiting {
		for _, h := range r.hashes {
			metrics.forgetTx(c.chainID, h)
		}
	}
	delete(c.confirmReserved, nonce)
}

// forgetWaitedHashes ends Confirm's wait on r: every hash in its group is
// forgotten in the gas metrics, along with hash, the one Confirm reports.
// endTx has already removed a mined hash. Hashes signed later are forgotten
// when the reservation is dropped. The caller holds c.mu.
func (c *TxSubmitter) forgetWaitedHashes(r *reservation, hash string) {
	metrics.forgetTx(c.chainID, hash)
	if r == nil {
		return
	}
	r.waiting = false
	for _, h := range r.hashes {
		metrics.forgetTx(c.chainID, h)
	}
}

// confirmNonceInFlight reports whether Confirm reserved nonce and the node
// still has a transaction at or beyond it.
func (c *TxSubmitter) confirmNonceInFlight(nonce, pending uint64) bool {
	_, reserved := c.confirmReserved[nonce]
	return reserved && pending > nonce
}

// waitReceipt polls until first, or a stall replacement of it recorded on r,
// is mined or ctx ends. r may be nil. It returns the mined hash, or the latest
// signed hash when none was mined. Confirm releases the signer lock for the
// wait; this takes it only to read r's hashes.
func (c *TxSubmitter) waitReceipt(
	ctx context.Context, r *reservation, first string,
) (string, *types.Receipt, error) {
	var last error
	known := []string{first}
	ticker := time.NewTicker(receiptPollInterval)
	defer ticker.Stop()
	for {
		known = c.replacementHashes(r, known)
		for _, hash := range known {
			receipt, err := c.eth.TransactionReceipt(ctx, common.HexToHash(hash))
			switch {
			case err == nil && receipt != nil:
				return hash, receipt, nil
			case err == nil:
				last = errors.New("empty receipt")
			default:
				last = err
			}
		}
		select {
		case <-ctx.Done():
			latest := known[len(known)-1]
			if last != nil {
				return latest, nil, last
			}
			return latest, nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// replacementHashes returns r's hashes when stall replacements have added to
// known. The caller's pointer stays valid after selectNonce deletes the map
// entry. noteAccepted appends to r.hashes under c.mu.
func (c *TxSubmitter) replacementHashes(r *reservation, known []string) []string {
	if r == nil {
		return known
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(r.hashes) <= len(known) {
		return known
	}
	return slices.Clone(r.hashes)
}

func (c *TxSubmitter) transactionAttributes(tx *types.Transaction, baseFee *big.Int) []any {
	return []any{
		"chainID", c.chainID, "txHash", tx.Hash().Hex(), "sender", c.address.Hex(),
		"nonce", tx.Nonce(), "gasLimit", tx.Gas(),
		"maxFeePerGasWei", tx.GasFeeCap().String(), "maxPriorityFeePerGasWei", tx.GasTipCap().String(),
		"baseFeePerGasWei", baseFee.String(), "nextUnminedNonce", c.lowestUnminedNonce,
	}
}

func (c *TxSubmitter) selectNonce(ctx context.Context, now time.Time) (uint64, *types.Header, error) {
	head, err := c.eth.HeaderByNumber(ctx, nil)
	if err != nil {
		return 0, nil, errors.Wrap(err, "getting latest header for nonce selection")
	}
	height := head.Number.Uint64()
	if height < c.lowestUnminedNonceHeight {
		return 0, nil, errors.Errorf(
			"RPC block height %d is behind last nonce observation at %d",
			height,
			c.lowestUnminedNonceHeight,
		)
	}
	mined, err := c.eth.NonceAt(ctx, c.address, head.Number)
	if err != nil {
		return 0, nil, errors.Wrapf(err, "getting mined nonce for %s at block %s", c.address, head.Number)
	}
	pending, err := c.eth.PendingNonceAt(ctx, c.address)
	if err != nil {
		return 0, nil, errors.Wrapf(err, "getting pending nonce for %s", c.address)
	}
	c.lowestUnminedNonceHeight = height
	if mined != c.lowestUnminedNonce {
		c.lowestUnminedNonce, c.lowestUnminedNoncePendingSince = mined, time.Time{}
		c.lastAttemptedFeeCap, c.lastAttemptedTipCap = nil, nil
	}
	for reserved := range c.confirmReserved {
		if reserved < mined {
			c.dropReservation(reserved)
		}
	}
	c.nextNonce = max(c.nextNonce, mined, pending)
	if c.nextNonce > mined {
		if c.lowestUnminedNoncePendingSince.IsZero() {
			c.lowestUnminedNoncePendingSince = now
		}
		// After the stall timeout the lowest unmined nonce is replaced. A
		// Confirm transfer the node still has keeps its reservation, and
		// submitLocked re-signs that transfer. One the node no longer reports
		// gives the nonce to the next intent.
		if now.Sub(c.lowestUnminedNoncePendingSince) >= c.opts.PendingTxTimeout {
			if !c.confirmNonceInFlight(mined, pending) {
				c.dropReservation(mined)
			}
			return mined, head, nil
		}
	}
	if c.nextNonce-mined >= maxInFlight {
		return 0, nil, ErrBusy
	}
	return c.nextNonce, head, nil
}

// ShouldRetry leaves missing transactions pending until the retry interval expires.
func (c *TxSubmitter) ShouldRetry(ctx context.Context, txHash string, sentAt time.Time) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	receipt, err := c.eth.TransactionReceipt(ctx, common.HexToHash(txHash))
	switch {
	case errors.Is(err, ethereum.NotFound):
		if time.Since(sentAt) >= c.opts.PendingTxTimeout {
			metrics.forgetTx(c.chainID, txHash)
			return true, nil
		}
		return false, v2.ErrTxNotFound
	case err != nil:
		return false, errors.Wrapf(err, "getting receipt for tx %s", txHash)
	default:
		metrics.endTx(c.chainID, receipt)
		return receipt.Status != types.ReceiptStatusSuccessful, nil
	}
}
