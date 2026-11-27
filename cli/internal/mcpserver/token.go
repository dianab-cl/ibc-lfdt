// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"strings"
	"unicode"

	"github.com/modelcontextprotocol/go-sdk/auth"
	pkgerrors "github.com/pkg/errors"
)

// TokenFileName is the bearer token file in the IBC home when mcp.tokenFile is empty.
const TokenFileName = "mcp-token"

// minTokenLength rejects a token file an operator filled with something guessable.
// A generated token is 64 hex characters.
const minTokenLength = 32

// LoadOrCreateToken returns the bearer token MCP clients must send. It reads
// path, or creates it with a random token and mode 0600 when it does not exist.
// An existing file that group or others can access is set to 0600 first.
// Any process that can reach the loopback port can call the tools, and the
// elicitation that approves a send is answered by that same caller, so the
// token is what keeps other local users and processes out.
func LoadOrCreateToken(path string) (string, error) {
	if f, err := os.Open(path); err == nil {
		defer f.Close()
		if err := restrictToOwner(f, path); err != nil {
			return "", pkgerrors.Wrap(err, "MCP token file")
		}
		data, err := io.ReadAll(f)
		if err != nil {
			return "", pkgerrors.Wrapf(err, "read MCP token file %s", path)
		}
		token := strings.TrimSpace(string(data))
		if len(token) < minTokenLength {
			return "", pkgerrors.Errorf(
				"MCP token file %s holds fewer than %d characters", path, minTokenLength)
		}
		// The Authorization header is split on whitespace, so such a token
		// could never be sent.
		if strings.IndexFunc(token, unicode.IsSpace) >= 0 {
			return "", pkgerrors.Errorf("MCP token file %s holds whitespace inside the token", path)
		}
		return token, nil
	} else if !os.IsNotExist(err) {
		return "", pkgerrors.Wrapf(err, "read MCP token file %s", path)
	}

	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", pkgerrors.Wrap(err, "generate MCP token")
	}
	token := hex.EncodeToString(buf)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", pkgerrors.Wrapf(err, "create MCP token file %s", path)
	}
	if _, err := f.WriteString(token + "\n"); err != nil {
		_ = f.Close()
		return "", pkgerrors.Wrapf(err, "write MCP token file %s", path)
	}
	if err := f.Close(); err != nil {
		return "", pkgerrors.Wrapf(err, "write MCP token file %s", path)
	}
	return token, nil
}

// requireBearer rejects a request whose Authorization header is not
// "Bearer <token>" with 401, before the MCP handler sees it.
func requireBearer(token string, next http.Handler) http.Handler {
	want := []byte(token)
	verify := func(_ context.Context, got string, _ *http.Request) (*auth.TokenInfo, error) {
		if subtle.ConstantTimeCompare([]byte(got), want) != 1 {
			return nil, auth.ErrInvalidToken
		}
		return &auth.TokenInfo{}, nil
	}
	// The token does not expire; it lives as long as the file.
	return auth.RequireBearerToken(verify, &auth.RequireBearerTokenOptions{AllowMissingExpiration: true})(next)
}
