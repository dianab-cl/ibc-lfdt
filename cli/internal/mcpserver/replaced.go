// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"strings"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// replacements groups the hashes signed for one transfer at one nonce. The
// relayer re-signs a stalled send with higher fees, which can happen after
// ibc_ift_send returned, so the hash the agent holds may never be mined. Only
// one hash of a group can be mined, and it is not always the newest.
type replacements struct {
	mu     sync.Mutex
	groups map[string]*[]string // lower-case hash → its group, oldest first
}

func (r *replacements) add(previous, replacement string) {
	previous, replacement = strings.ToLower(previous), strings.ToLower(replacement)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.groups == nil {
		r.groups = map[string]*[]string{}
	}
	g, ok := r.groups[previous]
	if !ok {
		g = &[]string{previous}
		r.groups[previous] = g
	}
	*g = append(*g, replacement)
	r.groups[replacement] = g
}

// family returns every hash signed for the same transfer as hash, newest
// first. A hash with no replacement is returned alone.
func (r *replacements) family(hash string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[strings.ToLower(hash)]
	if !ok {
		return []string{hash}
	}
	out := make([]string, 0, len(*g))
	for i := len(*g) - 1; i >= 0; i-- {
		out = append(out, (*g)[i])
	}
	return out
}

// noteReplaced records that the relayer re-signed this send's transfer. The
// tools that take a tx hash then also look at the other hashes, the audit log
// gets a line under the send's id, and a replay of the spent approval returns
// the new hash.
func (s *Server) noteReplaced(rec auditRecord, req *mcp.CallToolRequest, chainID, previous, replacement string) {
	s.replaced.add(previous, replacement)
	s.noteBroadcast(req, TxRef{ChainID: chainID, TxHash: replacement})
	_ = s.record(auditRecord{
		ID: rec.ID, Tool: rec.Tool, ChainID: chainID, Outcome: outcomeReplaced, TxHash: replacement,
		Message: "re-signed at the same nonce with higher fees; replaces " + previous +
			"; only one of them can be mined",
	})
}

// replacedNote is the warning when a lookup used another hash of the same
// transfer than the one the caller passed.
func replacedNote(asked, used string) string {
	if strings.EqualFold(asked, used) {
		return ""
	}
	return "The relayer re-signed this transfer at the same nonce; " + used +
		" is the transaction that carries its packets, not " + asked + "."
}
