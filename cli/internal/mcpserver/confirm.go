// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"sync"
	"time"
)

const (
	confirmInputKey = "confirm"
	confirmStateTTL = 10 * time.Minute
)

// confirmations issues and checks the RequestState that ties a user's
// approval to one exact write. A tool call that needs approval returns an
// elicitation input request plus a state token; the client retries with the
// user's answer and echoes the token. The token is an HMAC over a nonce and
// the write's resolved fields, so approving one transfer cannot authorize a
// different one, and each nonce is accepted once.
type confirmations struct {
	secret []byte
	now    func() time.Time

	mu sync.Mutex
	// used records a nonce that was accepted. tx is set once broadcast returns
	// a hash. It stays empty when the approval was spent and broadcast has not
	// returned one.
	used map[string]spentConfirm
}

// spentConfirm is one redeemed approval. expiry is the token's own expiry.
type spentConfirm struct {
	expiry time.Time
	tx     *TxRef
}

func newConfirmations(now func() time.Time) *confirmations {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		panic(err)
	}
	return &confirmations{secret: secret, now: now, used: map[string]spentConfirm{}}
}

// issue returns a state token for the write described by fields.
func (c *confirmations) issue(fields ...string) string {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		panic(err)
	}
	n := hex.EncodeToString(nonce)
	exp := c.now().Add(confirmStateTTL).UTC().Format(time.RFC3339)
	return n + "." + exp + "." + c.mac(n, exp, fields)
}

// confirmStatus is the result of redeem. Mismatch and expiry do not spend the
// nonce. A nonce already in the used set was spent on an earlier redeem, which
// happens before broadcast.
type confirmStatus int

const (
	confirmAccepted confirmStatus = iota
	confirmMismatch
	confirmExpired
	confirmUsed
)

// redeem checks a state token against the write's fields and spends it.
// A mismatch or an expiry leaves the nonce unspent. A nonce that was already
// accepted stays spent.
func (c *confirmations) redeem(state string, fields ...string) confirmStatus {
	nonce, exp, mac, ok := splitConfirmState(state)
	if !ok {
		return confirmMismatch
	}
	if !hmac.Equal([]byte(mac), []byte(c.mac(nonce, exp, fields))) {
		return confirmMismatch
	}
	expiry, err := time.Parse(time.RFC3339, exp)
	if err != nil || c.now().After(expiry) {
		return confirmExpired
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	for n, rec := range c.used {
		if now.After(rec.expiry) {
			delete(c.used, n)
		}
	}
	if _, spent := c.used[nonce]; spent {
		return confirmUsed
	}
	c.used[nonce] = spentConfirm{expiry: expiry}
	return confirmAccepted
}

// noteTx records the hash broadcast returned for an approval that is already
// spent. It does not store a hash the broadcast has not returned.
func (c *confirmations) noteTx(state string, tx TxRef) {
	if tx.TxHash == "" {
		return
	}
	nonce, _, _, ok := splitConfirmState(state)
	if !ok {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	rec, spent := c.used[nonce]
	if !spent {
		return
	}
	copied := tx
	rec.tx = &copied
	c.used[nonce] = rec
}

// spentTx returns the hash stored for a spent approval, or nil when broadcast
// has not returned one.
func (c *confirmations) spentTx(state string) *TxRef {
	nonce, _, _, ok := splitConfirmState(state)
	if !ok {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	rec, spent := c.used[nonce]
	if !spent || rec.tx == nil || rec.tx.TxHash == "" {
		return nil
	}
	copied := *rec.tx
	return &copied
}

func splitConfirmState(state string) (nonce, exp, mac string, ok bool) {
	parts := strings.Split(state, ".")
	if len(parts) != 3 || parts[0] == "" {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

func (c *confirmations) mac(nonce, exp string, fields []string) string {
	h := hmac.New(sha256.New, c.secret)
	h.Write([]byte(nonce + "\x00" + exp))
	for _, f := range fields {
		h.Write([]byte{0})
		h.Write([]byte(f))
	}
	return base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}
