<!-- SPDX-License-Identifier: Apache-2.0 -->

# IBC MCP server

`ibc relayer run` serves IBC CLI operations as
[Model Context Protocol](https://modelcontextprotocol.io) tools when `ibc.yml` sets
`mcp.listenAddr`, so an AI agent can send IFT tokens and follow the packets through that relayer.
The server runs in the relayer's process, on its own port, and calls the relayer directly. It is
an API over what that relayer already does. It does not mint and it does not cap amounts: mint
stays on `ibc tx ift mint`, and a send is whatever that client's signer can already submit. A
send uses the signer already configured on that client in `ibc.yml`.

```yaml
mcp:
  listenAddr: 127.0.0.1:3001
  auditLog: ~/.ibc/mcp-audit.jsonl # optional; empty uses mcp-audit.jsonl in the IBC home
```

## Tools

| Tool | What it does | Backed by |
| --- | --- | --- |
| `ibc_get_context` | This relayer's chains, connections (client IDs and signers), and whether packet queries work | `ibc.yml`, `Packets` |
| `ibc_ift_balance` | IFT balance of an address or signer alias, with symbol and decimals | IFT contract |
| `ibc_ift_send` | Send IFT over a client with that client's signer. Waits until the user accepts or declines, then up to 5 minutes for mining. `timeout_seconds` is the packet lifetime (default 900), not the mining wait. Follow `next` | IFT `iftTransfer`, `Relay` |
| `ibc_relay_tx` | Ask the relayer to deliver a tx's packets on a chain it connects; idempotent. `next` names `ibc_wait_for_packets` only when a packet was selected; `not_selected` or no packets means call `ibc_relay_tx` again | `Relay` |
| `ibc_get_packets` | One page of packet statuses with send/recv/ack/timeout hashes | `Packets` |
| `ibc_wait_for_packets` | Wait until a tx's packets are final (or received), with progress notifications. A `not_selected` packet returns immediately with a hint to call `ibc_relay_tx`, then wait again. Default 600 seconds, maximum 1800. Cancelling is a reason to wait again, not to send again | `Packets`, polled |

The resource template `ibc://packets/{chain_id}/{tx_hash}` returns every packet of that transaction as JSON. `ibc_get_packets` returns one page.

Failures come back as error tool results whose text is JSON:
`{"code", "message", "retryable", "tx"?}`. Codes: `INVALID_ARGUMENT`, `POLICY_DENIED`,
`USER_DECLINED`, `CONFIG_ERROR`, `CHAIN_RPC_ERROR`, `TX_REVERTED`, `TX_NOT_CONFIRMED`,
`RELAYER_UNAVAILABLE`, `RELAYER_ERROR`. A write that reached the chain always carries its `tx`.
`TX_NOT_CONFIRMED` is not retryable. Check that hash and do not sign again. `CHAIN_RPC_ERROR`
and `RELAYER_UNAVAILABLE` are retryable. A relay that reports no packets found is
`INVALID_ARGUMENT` and retryable: retry `ibc_relay_tx`. Other `INVALID_ARGUMENT` results are not.

## Running it

```text
$ ibc relayer run
level=INFO msg="MCP server listening" url=http://127.0.0.1:3001
level=INFO msg="MCP audit" module=mcp tool=ibc_ift_send outcome=mined chain=41001 code="" amount=100 client=base-0 tx=0xabc
level=INFO msg=Readiness module=bootstrap readiness="{Event:ready ChainsConnected:[41001 41002] HTTP:0.0.0.0:3000}"
```

Those lines are the process log, written to stderr. A container runtime collects that stream (`docker logs`, `kubectl logs`). Startup, a failure to start, shutdown, and each audit event are on it. Bytes the server writes to stdout, including the Go SDK stdio transport, are copied onto that same logger, so they are on the stream too. Panics recovered by the HTTP server and SDK session logs are on it as well. The audit file stays in the IBC home and is not part of that stream.

`--mcp-listen 127.0.0.1:3001` overrides `mcp.listenAddr` for that process.

Point the MCP client at that URL:

```bash
# Claude Code
claude mcp add --transport http ibc http://127.0.0.1:3001
```

The MCP server starts and stops with the relayer. A taken port stops the relayer from starting.
Without `mcp.listenAddr` and without `--mcp-listen`, the relayer runs exactly as before. The
address must be loopback (`127.0.0.1`, `::1`, or `localhost`), because the endpoint has no
authentication and can sign with the relayer's keys.

## What a caller may do

A send is allowed when `(chain_id, client_id)` is one end of a relayer connection and `from` is
the signer configured on that end. Any IFT on that chain can be sent, to any receiver. The user
approves the exact transfer through MCP elicitation before anything is signed. The call does not
return until the user accepts or declines. A client without elicitation is refused with
`POLICY_DENIED`. The approval is bound to the chain, token, signer, receiver, amount, client,
timeout and relay mode by a single-use HMAC token, so it cannot be replayed or applied to a
different write. The token is redeemed before broadcast. A mismatch or an expiry was not spent;
call the tool again. A token that was already used means the transfer may already be broadcast.
If the error includes `tx`, call `ibc_get_packets` with that hash as `source_tx_hash` and do not
call `ibc_ift_send` again. If no hash was returned, ask the user and do not call `ibc_ift_send`.
If a send is cancelled or the client times out and a hash was returned, check that hash and do not
call `ibc_ift_send` again. If no hash was returned, ask the user and do not call `ibc_ift_send`.
The signer is the
one the relayer already submits with for that client, including a remote signer.

`ibc_relay_tx` does not sign. It is allowed when `chain_id` is on a relayer connection, because
the relayer spends its own gas to deliver. Reads stay open on every chain in `ibc.yml`.

Every send and relay is appended to the audit log before it is submitted, and a second line
records how it ended (`mined`, `not_confirmed`, `reverted`, `relayed`, or `failed`). A refusal
or a declined approval is one line (`denied` or `declined`). The same line is written to the
process log with the tool, outcome, chain, and code. If the first line cannot be synced,
the call does not proceed. Reads are not logged. The file is owner-readable only and remains
the durable record.

## Layout

- `server.go`: tool registration and handlers, write authorization
- `audit.go`: append-only JSONL audit log
- `wait.go`: `ibc_wait_for_packets` and progress
- `confirm.go`: approval tokens for multi round-trip confirmation
- `config.go`: amount parsing
- `relayer.go`: adapter over the relayer's `RelayerApiService` handler, called in-process
- `http.go`: streamable HTTP listener
- `backend.go`: the chain and relayer interfaces. The EVM backend and the startup hook for
  `ibc relayer run` live in `cmd/ibc/mcp.go`. A send goes through the relayer's transaction
  submitter for that chain and signer. `ibc tx ift` keeps its own sign-and-mine path.
