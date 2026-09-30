package handlers

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"
)

// fakeJWKS serves a single locally generated test key in better-auth's
// stored-JWK shape.
type fakeJWKS struct{ jwk string }

func (f fakeJWKS) JWKSPublicKeys(context.Context) ([]string, error) {
	return []string{f.jwk}, nil
}

func testKeypair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey, fakeJWKS) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	jwk, _ := json.Marshal(map[string]string{
		"kty": "OKP", "crv": "Ed25519",
		"x": base64.RawURLEncoding.EncodeToString(pub),
	})
	return pub, priv, fakeJWKS{jwk: string(jwk)}
}

func mintToken(t *testing.T, priv ed25519.PrivateKey, alg, role string, exp int64, sign bool) string {
	t.Helper()
	enc := func(v any) string {
		b, _ := json.Marshal(v)
		return base64.RawURLEncoding.EncodeToString(b)
	}
	head := enc(map[string]string{"alg": alg, "typ": "JWT"})
	body := enc(map[string]any{"role": role, "exp": exp})
	sig := ""
	if sign {
		sig = base64.RawURLEncoding.EncodeToString(ed25519.Sign(priv, []byte(head+"."+body)))
	} else {
		sig = base64.RawURLEncoding.EncodeToString([]byte("no-signature"))
	}
	return head + "." + body + "." + sig
}

func TestVerifyAdminJWT(t *testing.T) {
	_, priv, src := testKeypair(t)
	_, otherPriv, _ := testKeypair(t)
	future := time.Now().Add(time.Hour).Unix()
	past := time.Now().Add(-time.Hour).Unix()

	cases := []struct {
		name  string
		token string
		want  bool
	}{
		{"signed admin token passes", mintToken(t, priv, "EdDSA", "admin", future, true), true},
		{"unsigned token is rejected", mintToken(t, priv, "EdDSA", "admin", future, false), false},
		{"token signed by a different key is rejected", mintToken(t, otherPriv, "EdDSA", "admin", future, true), false},
		{"non-admin role is rejected", mintToken(t, priv, "EdDSA", "user", future, true), false},
		{"expired token is rejected", mintToken(t, priv, "EdDSA", "admin", past, true), false},
		{"alg none is rejected", mintToken(t, priv, "none", "admin", future, true), false},
		{"garbage is rejected", "not.a.jwt", false},
		{"empty string is rejected", "", false},
	}
	for _, c := range cases {
		adminKeys = jwksCache{} // reset the cache between cases
		if got := verifyAdminJWT(context.Background(), src, c.token); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

func TestParseEd25519JWKRejectsOtherKeyTypes(t *testing.T) {
	if parseEd25519JWK(`{"kty":"RSA","n":"abc","e":"AQAB"}`) != nil {
		t.Error("RSA JWK should be ignored")
	}
	if parseEd25519JWK(`not json`) != nil {
		t.Error("invalid JSON should be ignored")
	}
	if parseEd25519JWK(`{"kty":"OKP","crv":"Ed25519","x":"dG9vc2hvcnQ"}`) != nil {
		t.Error("wrong-length key should be ignored")
	}
}
