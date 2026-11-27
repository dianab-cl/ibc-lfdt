// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/ecdsa"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pkg/errors"

	"github.com/cosmos/ibc-contracts/packages/go-abigen/ift"
	"github.com/lfdt-ibc/ibc/cli/internal/config"
)

// localECDSAKey loads a configured local ECDSA signer. Remote signers are refused.
func localECDSAKey(cfg config.Config, alias string) (*ecdsa.PrivateKey, error) {
	keyHex, err := deployerKeyHex(cfg, alias)
	if err != nil {
		return nil, err
	}
	key, err := crypto.HexToECDSA(strings.TrimPrefix(keyHex, "0x"))
	if err != nil {
		return nil, errors.New("invalid key")
	}
	return key, nil
}

// callAndMine signs and broadcasts one IFT call, then waits for the receipt.
// confirmTimeout bounds the wait; zero uses ctx alone. A non-nil transaction
// with a non-nil error means the tx was broadcast and did not mine.
func callAndMine(
	ctx context.Context,
	backend *ethclient.Client,
	key *ecdsa.PrivateKey,
	chainID *big.Int,
	iftAddr common.Address,
	confirmTimeout time.Duration,
	call func(*ift.Contract, *bind.TransactOpts) (*types.Transaction, error),
) (*types.Transaction, *types.Receipt, error) {
	contract, err := ift.NewContract(iftAddr, backend)
	if err != nil {
		return nil, nil, err
	}
	opts, err := bind.NewKeyedTransactorWithChainID(key, chainID)
	if err != nil {
		return nil, nil, err
	}
	opts.Context = ctx

	tx, err := call(contract, opts)
	if err != nil {
		return nil, nil, err
	}

	waitCtx, cancel := ctx, func() {}
	if confirmTimeout > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, confirmTimeout)
	}
	defer cancel()
	receipt, err := bind.WaitMined(waitCtx, backend, tx)
	if err != nil {
		return tx, nil, err
	}
	return tx, receipt, nil
}
