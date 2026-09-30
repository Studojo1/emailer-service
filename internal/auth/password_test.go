package auth

import (
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

// Made by better-auth 1.4.18's own hashPassword (better-auth/crypto) for the
// fixture password below. The fullwidth first letter checks the NFKC step.
const (
	fixturePassword = "Ｆixture-pässword-1"
	betterAuthHash  = "a94352fd1219ccc7efbc0ac53d5544bd:5ad3962cf525d30db5b614ae6c0073d5e689d5f748bea36a4feefd410acb4fff6b0047046f958cc854f1d3f8db2c219df7e3c6001edb85e630178f4561e7e0ec"
)

// AS-N04 follow-up: the emailer checks and writes passwords in Better Auth's
// format itself, so it never needs the site's hash-password endpoint, and the
// settings page's "current password" is checked against real account hashes.
func TestCheckPasswordAcceptsBetterAuthHash(t *testing.T) {
	if !CheckPassword(betterAuthHash, fixturePassword) {
		t.Fatal("a Better Auth hash of the right password was rejected")
	}
	if !CheckPassword(betterAuthHash, "Fixture-pässword-1") {
		t.Fatal("NFKC-equivalent password was rejected")
	}
	if CheckPassword(betterAuthHash, "wrong-password") {
		t.Fatal("wrong password accepted")
	}
}

func TestHashPasswordRoundTripsInBetterAuthFormat(t *testing.T) {
	h, err := HashPassword(fixturePassword)
	if err != nil {
		t.Fatal(err)
	}
	salt, key, ok := strings.Cut(h, ":")
	if !ok || len(salt) != 32 || len(key) != 128 {
		t.Fatalf("hash %q is not <32 hex salt>:<128 hex key>", h)
	}
	if !CheckPassword(h, fixturePassword) || CheckPassword(h, "wrong-password") {
		t.Fatal("round trip failed")
	}
	if h2, _ := HashPassword(fixturePassword); h2 == h {
		t.Fatal("salt is not random")
	}
}

func TestCheckPasswordLegacyBcryptAndGarbage(t *testing.T) {
	b, _ := bcrypt.GenerateFromPassword([]byte("pw-123456"), bcrypt.MinCost)
	if !CheckPassword(string(b), "pw-123456") || CheckPassword(string(b), "nope") {
		t.Fatal("legacy bcrypt check wrong")
	}
	for _, bad := range []string{"", ":", "abc", "zz:zz", "abcd:"} {
		if CheckPassword(bad, "pw") {
			t.Fatalf("malformed hash %q accepted", bad)
		}
	}
}
