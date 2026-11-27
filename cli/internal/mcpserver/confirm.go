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
	// spentRetention keeps a spent approval past expiry so a replay reports used,
	// not expired, and the agent is not told to send again.
	spentRetention = 24 * time.Hour
)

const (
	confirmAccepted confirmStatus = iota
	confirmMismatch
	// confirmExpired is a token that expired unspent.
	confirmExpired
	confirmUsed
	// confirmStale expired over spentRetention ago, so whether it was spent is unknown.
	confirmStale
	// confirmOtherRun was issued before a restart, so whether it was spent is unknown.
	confirmOtherRun
)

// confirmations ties a user's approval to one exact write: the state token is an
// HMAC over a single-use nonce and the write's resolved fields.
type confirmations struct {
	secret []byte
	// run tags this process's tokens; state is in memory, so redeem reports an
	// earlier run's token as such rather than as a mismatch.
	run string
	now func() time.Time

	mu sync.Mutex
	// used holds accepted nonces; tx is set once broadcast returns a hash.
	used map[string]spentConfirm
}

// spentConfirm is one redeemed approval. expiry is the token's own expiry.
// The record is kept until spentRetention after it.
type spentConfirm struct {
	expiry time.Time
	tx     *TxRef
}

// confirmStatus is the result of redeem. Only acceptance spends the nonce,
// and that happens before broadcast.
type confirmStatus int

func newConfirmations(now func() time.Time) *confirmations {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		panic(err)
	}
	run := make([]byte, 8)
	if _, err := rand.Read(run); err != nil {
		panic(err)
	}
	return &confirmations{
		secret: secret, run: hex.EncodeToString(run), now: now, used: map[string]spentConfirm{},
	}
}

// issue returns a state token for the write described by fields.
func (c *confirmations) issue(fields ...string) string {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		panic(err)
	}
	n := hex.EncodeToString(nonce)
	exp := c.now().Add(confirmStateTTL).UTC().Format(time.RFC3339)
	return c.run + "." + n + "." + exp + "." + c.mac(n, exp, fields)
}

// redeem checks a state token against fields and spends it. Mismatch or expiry
// leaves it unspent; a spent nonce reports used even after expiry.
func (c *confirmations) redeem(state string, fields ...string) confirmStatus {
	run, nonce, exp, mac, ok := splitConfirmState(state)
	if !ok {
		return confirmMismatch
	}
	if run != c.run {
		return confirmOtherRun
	}
	if !hmac.Equal([]byte(mac), []byte(c.mac(nonce, exp, fields))) {
		return confirmMismatch
	}
	expiry, err := time.Parse(time.RFC3339, exp)
	if err != nil {
		return confirmMismatch
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	for n, rec := range c.used {
		if now.After(rec.expiry.Add(spentRetention)) {
			delete(c.used, n)
		}
	}
	// Spent is checked before expiry: an expired token that was spent may
	// already have been broadcast.
	if _, spent := c.used[nonce]; spent {
		return confirmUsed
	}
	if now.After(expiry.Add(spentRetention)) {
		return confirmStale
	}
	if now.After(expiry) {
		return confirmExpired
	}
	c.used[nonce] = spentConfirm{expiry: expiry}
	return confirmAccepted
}

// noteTx records the broadcast hash for an already spent approval.
func (c *confirmations) noteTx(state string, tx TxRef) {
	nonce, ok := c.ownNonce(state)
	if tx.TxHash == "" || !ok {
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

// spentTx returns the hash stored for a spent approval, or nil.
func (c *confirmations) spentTx(state string) *TxRef {
	nonce, ok := c.ownNonce(state)
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

// ownNonce is the nonce of a state token this run issued.
func (c *confirmations) ownNonce(state string) (string, bool) {
	run, nonce, _, _, ok := splitConfirmState(state)
	return nonce, ok && run == c.run
}

func splitConfirmState(state string) (run, nonce, exp, mac string, ok bool) {
	parts := strings.Split(state, ".")
	if len(parts) != 4 || parts[0] == "" || parts[1] == "" {
		return "", "", "", "", false
	}
	return parts[0], parts[1], parts[2], parts[3], true
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
