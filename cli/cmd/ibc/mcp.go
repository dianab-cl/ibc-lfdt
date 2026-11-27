// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	stdErrors "errors"
	"log/slog"
	"math/big"
	"net/url"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pkg/errors"

	"github.com/cosmos/ibc-contracts/packages/go-abigen/ift"
	"github.com/lfdt-ibc/ibc/cli/api/v2/relayer"
	"github.com/lfdt-ibc/ibc/cli/internal/config"
	"github.com/lfdt-ibc/ibc/cli/internal/mcpserver"
	"github.com/lfdt-ibc/ibc/cli/internal/otel"
	"github.com/lfdt-ibc/ibc/cli/internal/service/signer"
	"github.com/lfdt-ibc/ibc/cli/internal/txsubmitter"
	evmsub "github.com/lfdt-ibc/ibc/cli/internal/txsubmitter/evm"
	v2 "github.com/lfdt-ibc/ibc/cli/internal/types/v2"
)

// flags of `ibc relayer run` that start the MCP server beside the relayer
var (
	flagRelayerMCPListen string
)

// startRelayerMCP starts the MCP server inside `ibc relayer run`, on its own
// port, calling this relayer through api: the same handler its RelayerApiService
// serves. --mcp-listen overrides mcp.listenAddr for this run. It returns nil
// when both are empty.
func startRelayerMCP(
	cfg config.Config, api relayer.RelayerApiServiceClient, submitters *txsubmitter.Set,
) (stop func() error, _ error) {
	listen := flagRelayerMCPListen
	if listen == "" {
		listen = cfg.MCP.ListenAddr
	}
	if listen == "" {
		slog.Debug("MCP server not started: no listen address")
		return nil, nil
	}

	if err := mcpserver.RequireLoopback(listen); err != nil {
		return nil, err
	}
	auditPath, err := mcpHomePath(globalFlags.Home, cfg.MCP.AuditLog, mcpserver.AuditFileName)
	if err != nil {
		return nil, err
	}
	tokenPath, err := mcpHomePath(globalFlags.Home, cfg.MCP.TokenFile, mcpserver.TokenFileName)
	if err != nil {
		return nil, err
	}
	token, err := mcpserver.LoadOrCreateToken(tokenPath)
	if err != nil {
		return nil, err
	}
	audit, err := mcpserver.OpenAuditLog(auditPath)
	if err != nil {
		return nil, err
	}

	srv, err := mcpserver.New(
		&evmBackend{cfg: cfg, submitters: submitters},
		mcpserver.NewRelayerAPI(api),
		mcpserver.Options{
			Version: version,
			Audit:   audit,
			// Counts share the relayer meter. Observability already owns the
			// scrape listener; this does not open another.
			Meter: otel.Meter("relayer"),
		},
	)
	if err != nil {
		_ = audit.Close()
		return nil, err
	}
	h, err := srv.ListenHTTP(listen, token)
	if err != nil {
		_ = srv.Close()
		return nil, err
	}

	// stderr, via slog. The container image runs /opt/ibc with no log redirect,
	// so docker logs and kubectl logs are this stream. The audit file is not.
	slog.Info("MCP server listening", "url", h.URL(), "tokenFile", tokenPath)

	var once sync.Once
	var stopErr error
	return func() error {
		once.Do(func() {
			stopErr = stdErrors.Join(h.Stop(), srv.Close())
			if stopErr != nil {
				slog.Error("MCP server stopped", "err", stopErr)
				stopErr = errors.Wrap(stopErr, "stop MCP server")
				return
			}
			slog.Info("MCP server stopped")
		})
		return stopErr
	}, nil
}

// mcpHomePath resolves an MCP file setting such as mcp.auditLog. An empty
// setting uses defaultName in the IBC home. A relative path is joined to that home.
func mcpHomePath(home, configured, defaultName string) (string, error) {
	home, err := config.ExpandHome(home)
	if err != nil {
		return "", err
	}
	if configured == "" {
		return filepath.Join(home, defaultName), nil
	}
	path, err := config.ExpandHome(configured)
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(home, path)
	}
	return path, nil
}

// evmBackend implements mcpserver.ChainBackend over the configured EVM chains.
// A send uses the relayer's TxSubmitter for that (chain, signer): the same
// signer and the same nonce allocator the relayer submits with. Reads dial
// the chain RPCs already in the relayer config.
type evmBackend struct {
	cfg        config.Config
	submitters *txsubmitter.Set
}

var _ mcpserver.ChainBackend = (*evmBackend)(nil)

func (b *evmBackend) Chains(context.Context) ([]mcpserver.ChainInfo, error) {
	out := make([]mcpserver.ChainInfo, 0, len(b.cfg.Chains))
	for _, c := range b.cfg.Chains {
		out = append(out, mcpserver.ChainInfo{ChainID: c.ChainID, Type: string(c.Type())})
	}
	return out, nil
}

func (b *evmBackend) Connections(context.Context) ([]mcpserver.ConnectionInfo, error) {
	end := func(e config.ClientEnd) mcpserver.ClientEndInfo {
		return mcpserver.ClientEndInfo{
			ChainID: e.ChainID, ClientID: e.ClientID, Signer: e.Signer,
			AutoRelay: e.AutoRelay.Enabled != nil && *e.AutoRelay.Enabled,
		}
	}
	out := make([]mcpserver.ConnectionInfo, 0, len(b.cfg.Relayer.Connections))
	for _, c := range b.cfg.Relayer.Connections {
		out = append(out, mcpserver.ConnectionInfo{Alias: c.Alias, ClientA: end(c.ClientA), ClientB: end(c.ClientB)})
	}
	return out, nil
}

func (b *evmBackend) Signers(context.Context) ([]mcpserver.SignerInfo, error) {
	out := make([]mcpserver.SignerInfo, 0, len(b.cfg.Signers))
	for _, s := range b.cfg.Signers {
		info := mcpserver.SignerInfo{Alias: s.Alias}
		if addr, err := signer.EVMAddressOf(s); err == nil {
			info.Address = addr
		}
		out = append(out, info)
	}
	return out, nil
}

func (b *evmBackend) ResolveAddress(_ context.Context, value string) (string, error) {
	addr, err := resolveAddress(b.cfg, value)
	if err != nil {
		return "", mcpserver.NewError(mcpserver.CodeInvalidArgument, "%s", err.Error())
	}
	return common.HexToAddress(addr).Hex(), nil
}

func (b *evmBackend) TokenInfo(ctx context.Context, chainID, iftAddr string) (mcpserver.TokenInfo, error) {
	contract, closeClient, err := b.openIFT(ctx, chainID, iftAddr)
	if err != nil {
		return mcpserver.TokenInfo{}, err
	}
	defer closeClient()

	opts := &bind.CallOpts{Context: ctx}
	symbol, err := contract.Symbol(opts)
	if err != nil {
		return mcpserver.TokenInfo{}, rpcError(err, "symbol")
	}
	decimals, err := contract.Decimals(opts)
	if err != nil {
		return mcpserver.TokenInfo{}, rpcError(err, "decimals")
	}
	return mcpserver.TokenInfo{Symbol: symbol, Decimals: decimals}, nil
}

func (b *evmBackend) IFTBalance(ctx context.Context, chainID, iftAddr, address string) (*big.Int, error) {
	contract, closeClient, err := b.openIFT(ctx, chainID, iftAddr)
	if err != nil {
		return nil, err
	}
	defer closeClient()

	bal, err := contract.BalanceOf(&bind.CallOpts{Context: ctx}, common.HexToAddress(address))
	if err != nil {
		return nil, rpcError(err, "balanceOf")
	}
	return bal, nil
}

func (b *evmBackend) IFTSend(ctx context.Context, req mcpserver.SendRequest) (mcpserver.TxRef, error) {
	submitter, err := b.evmSubmitter(req.ChainID, req.From)
	if err != nil {
		return mcpserver.TxRef{}, err
	}
	timeout := uint64(time.Now().Add(req.Timeout).Unix())
	data, err := packIFTTransfer(req.ClientID, req.To, req.Amount, timeout)
	if err != nil {
		return mcpserver.TxRef{}, mcpserver.NewError(mcpserver.CodeConfigError, "encode transfer: %s", redactRPC(err))
	}
	var onReplace evmsub.ReplaceFunc
	if req.OnReplaced != nil {
		onReplace = func(previous, replacement string) {
			slog.Warn("MCP tx re-signed at the same nonce", "chain", req.ChainID,
				"previousTx", previous, "tx", replacement, "from", req.From)
			req.OnReplaced(previous, replacement)
		}
	}
	hash, receipt, err := submitter.Confirm(ctx, v2.TxIntent{To: req.IFT, Data: data}, req.ConfirmTimeout, onReplace)
	if hash == "" {
		if err == nil {
			err = stdErrors.New("no transaction")
		}
		// Nothing was broadcast. A transfer that reverts in simulation reverts
		// again until chain state changes, so it is not retryable.
		if stdErrors.Is(err, evmsub.ErrEstimationReverted) {
			return mcpserver.TxRef{}, mcpserver.NewError(mcpserver.CodeTxReverted,
				"transfer reverts in simulation and was not broadcast: %s", redactRPC(err))
		}
		// Any other failure before broadcast may be transient. The agent may retry.
		return mcpserver.TxRef{}, mcpserver.NewError(mcpserver.CodeChainRPCError, "submit: %s", redactRPC(err))
	}
	ref := mcpserver.TxRef{ChainID: req.ChainID, TxHash: hash}
	slog.Info("MCP tx broadcast", "chain", req.ChainID, "tx", ref.TxHash, "from", req.From)
	if err != nil {
		return mcpserver.TxRef{}, mcpserver.NewTxError(mcpserver.CodeTxNotConfirmed, ref,
			"tx broadcast but not mined within %s: %s; check the hash before resending",
			req.ConfirmTimeout, redactRPC(err))
	}
	if receipt == nil || receipt.Status != types.ReceiptStatusSuccessful {
		return mcpserver.TxRef{}, mcpserver.NewTxError(mcpserver.CodeTxReverted, ref, "tx %s reverted", ref.TxHash)
	}
	return ref, nil
}

// evmSubmitter is the relayer's nonce allocator for this chain and signer.
// There is no second key and no second pending-nonce read beside it.
func (b *evmBackend) evmSubmitter(chainID, signerAlias string) (*evmsub.TxSubmitter, error) {
	if b.submitters == nil {
		return nil, mcpserver.NewError(mcpserver.CodeConfigError, "relayer transaction submitters are not available")
	}
	s, ok := b.submitters.Get(chainID, signerAlias)
	if !ok {
		return nil, mcpserver.NewError(mcpserver.CodePolicyDenied,
			"signer %q is not configured to submit on chain %q", signerAlias, chainID)
	}
	concrete, ok := s.(*evmsub.TxSubmitter)
	if !ok {
		return nil, mcpserver.NewError(mcpserver.CodeConfigError,
			"signer %q on chain %q does not use the EVM submitter", signerAlias, chainID)
	}
	return concrete, nil
}

func packIFTTransfer(clientID, receiver string, amount *big.Int, timeout uint64) ([]byte, error) {
	parsed, err := ift.ContractMetaData.GetAbi()
	if err != nil {
		return nil, err
	}
	return parsed.Pack("iftTransfer", clientID, receiver, amount, timeout)
}

// openIFT dials chainID and binds the IFT contract. The caller closes the client.
func (b *evmBackend) openIFT(ctx context.Context, chainID, iftAddr string) (*ift.Contract, func(), error) {
	client, err := b.dial(ctx, chainID)
	if err != nil {
		return nil, nil, err
	}
	contract, err := ift.NewContract(common.HexToAddress(iftAddr), client)
	if err != nil {
		client.Close()
		return nil, nil, errors.Wrap(err, "opening IFT contract")
	}
	return contract, client.Close, nil
}

func (b *evmBackend) dial(ctx context.Context, chainID string) (*ethclient.Client, error) {
	chain, ok := b.cfg.Chain(chainID)
	if !ok {
		return nil, mcpserver.NewError(mcpserver.CodeConfigError, "chain %q not declared in config", chainID)
	}
	if chain.Type() != config.ChainTypeEVM {
		return nil, mcpserver.NewError(mcpserver.CodeConfigError, "chain %q is not an EVM chain", chainID)
	}
	client, err := ethclient.DialContext(ctx, chain.EVM.RPC)
	if err != nil {
		// the RPC URL may hold credentials, so it is not echoed to the agent
		return nil, mcpserver.NewError(mcpserver.CodeChainRPCError, "dial chain %q failed", chainID)
	}
	return client, nil
}

var rpcURLPattern = regexp.MustCompile(`https?://\S+`)

// redactRPC drops endpoint URLs. Dial failures are already generic; call and
// submit errors from the HTTP client include the request URL, which may hold
// credentials.
func redactRPC(err error) string {
	if err == nil {
		return ""
	}
	var ue *url.Error
	if stdErrors.As(err, &ue) && ue.Err != nil {
		return redactRPC(ue.Err)
	}
	return rpcURLPattern.ReplaceAllString(err.Error(), "[redacted-url]")
}

func rpcError(err error, what string) error {
	if errors.Is(err, ethereum.NotFound) {
		return mcpserver.NewError(mcpserver.CodeInvalidArgument, "%s: not found", what)
	}
	return mcpserver.NewError(mcpserver.CodeChainRPCError, "%s: %s", what, redactRPC(err))
}
