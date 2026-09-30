package handlers

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// JWKSSource supplies the JWK public keys the site's auth layer publishes
// (better-auth's jwks table). Implemented by *store.PostgresStore.
type JWKSSource interface {
	JWKSPublicKeys(ctx context.Context) ([]string, error)
}

// The keys rotate rarely; cache them briefly so the middleware does not hit
// the database on every admin request.
const jwksCacheTTL = 5 * time.Minute

type jwksCache struct {
	mu      sync.Mutex
	keys    []ed25519.PublicKey
	fetched time.Time
}

var adminKeys jwksCache

func (c *jwksCache) get(ctx context.Context, src JWKSSource) []ed25519.PublicKey {
	c.mu.Lock()
	defer c.mu.Unlock()
	if time.Since(c.fetched) < jwksCacheTTL && len(c.keys) > 0 {
		return c.keys
	}
	raw, err := src.JWKSPublicKeys(ctx)
	if err != nil {
		slog.Error("admin jwt: loading jwks", "error", err)
		return c.keys // stale keys beat no keys during a DB blip
	}
	var keys []ed25519.PublicKey
	for _, j := range raw {
		if k := parseEd25519JWK(j); k != nil {
			keys = append(keys, k)
		}
	}
	c.keys, c.fetched = keys, time.Now()
	return c.keys
}

// parseEd25519JWK extracts the raw public key from a JWK JSON document.
// better-auth stores OKP/Ed25519 keys; anything else is ignored.
func parseEd25519JWK(jwkJSON string) ed25519.PublicKey {
	var jwk struct {
		Kty string `json:"kty"`
		Crv string `json:"crv"`
		X   string `json:"x"`
	}
	if err := json.Unmarshal([]byte(jwkJSON), &jwk); err != nil {
		return nil
	}
	if jwk.Kty != "OKP" || jwk.Crv != "Ed25519" {
		return nil
	}
	b, err := base64.RawURLEncoding.DecodeString(jwk.X)
	if err != nil || len(b) != ed25519.PublicKeySize {
		return nil
	}
	return ed25519.PublicKey(b)
}

// verifyAdminJWT accepts a token only when its EdDSA signature checks out
// against one of the published keys AND the claims say role=admin and the
// token has not expired. This replaces the old decode-only check, which
// never looked at the signature.
func verifyAdminJWT(ctx context.Context, src JWKSSource, tokenStr string) bool {
	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return false
	}

	headerB, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return false
	}
	var header struct {
		Alg string `json:"alg"`
	}
	// The algorithm must be pinned. Honouring whatever the token names would
	// let a caller downgrade to "none".
	if err := json.Unmarshal(headerB, &header); err != nil || header.Alg != "EdDSA" {
		return false
	}

	payloadB, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	var claims struct {
		Role string `json:"role"`
		Exp  int64  `json:"exp"`
	}
	if err := json.Unmarshal(payloadB, &claims); err != nil {
		return false
	}
	if claims.Role != "admin" || time.Now().Unix() >= claims.Exp {
		return false
	}

	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return false
	}
	signed := []byte(parts[0] + "." + parts[1])
	for _, key := range adminKeys.get(ctx, src) {
		if ed25519.Verify(key, signed, sig) {
			return true
		}
	}
	return false
}
