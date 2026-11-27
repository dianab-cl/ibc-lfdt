// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"errors"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.opentelemetry.io/otel/metric"

	"github.com/lfdt-ibc/ibc/cli/internal/otel"
)

// methodToolsCall is the MCP method for a tool call. The SDK constant is unexported.
const methodToolsCall = "tools/call"

const (
	callResultOK    = "ok"
	callResultError = "error"
	// callCodeOther is any failure that is not one of the tool error codes.
	callCodeOther = "other"
	// callToolUnknown keeps a client-supplied name off the label.
	callToolUnknown = "unknown"
)

func newToolCallCounter(m metric.Meter) (metric.Int64Counter, error) {
	if m == nil {
		return nil, nil
	}
	return m.Int64Counter("mcp_tool_calls_total")
}

// trackTool keeps name as a metric label. Names this server did not register
// are recorded as unknown, so a client cannot grow the series.
func (s *Server) trackTool(tool *mcp.Tool) *mcp.Tool {
	s.knownTools[tool.Name] = struct{}{}
	return tool
}

func (s *Server) observeToolCall(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
		result, err := next(ctx, method, req)
		s.recordToolCall(ctx, method, req, result, err)
		return result, err
	}
}

func (s *Server) recordToolCall(
	ctx context.Context,
	method string,
	req mcp.Request,
	result mcp.Result,
	callErr error,
) {
	if s.toolCalls == nil || method != methodToolsCall {
		return
	}
	call, ok := req.(*mcp.CallToolRequest)
	if !ok || call == nil || call.Params == nil {
		return
	}
	outcome, code := toolCallOutcome(result, callErr)
	s.toolCalls.Add(context.WithoutCancel(ctx), 1, metric.WithAttributes(
		otel.AttrTool.String(s.metricTool(call.Params.Name)),
		otel.AttrResult.String(outcome),
		otel.AttrCode.String(code),
	))
}

func (s *Server) metricTool(name string) string {
	if _, ok := s.knownTools[name]; ok {
		return name
	}
	return callToolUnknown
}

// toolCallOutcome reports success or error, and the tool error code when the
// call returned one. An empty code is success. Any other failure is "other".
func toolCallOutcome(result mcp.Result, callErr error) (string, string) {
	if callErr != nil {
		return callResultError, toolErrorCode(callErr)
	}
	res, ok := result.(*mcp.CallToolResult)
	if ok && res != nil && res.IsError {
		return callResultError, toolErrorCode(res.GetError())
	}
	return callResultOK, ""
}

func toolErrorCode(err error) string {
	var te *ToolError
	if errors.As(err, &te) && knownToolCode(te.Code) {
		return string(te.Code)
	}
	return callCodeOther
}

// knownToolCode is every ErrorCode. An unrecognized code is not used as a
// label: that would let a backend grow the series.
func knownToolCode(code ErrorCode) bool {
	switch code {
	case CodeInvalidArgument, CodePolicyDenied, CodeUserDeclined, CodeConfigError,
		CodeChainRPCError, CodeTxReverted, CodeTxNotConfirmed,
		CodeRelayerUnavailable, CodeRelayerError:
		return true
	default:
		return false
	}
}
