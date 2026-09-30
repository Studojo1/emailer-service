package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"strings"

	"golang.org/x/crypto/bcrypt"
	"golang.org/x/crypto/scrypt"
	"golang.org/x/text/unicode/norm"
)

// Better Auth (the site's auth layer) stores credential passwords as
// "<salt hex>:<key hex>", where key = scrypt(NFKC(password), salt hex string,
// N=16384, r=16, p=1, 64 bytes). See better-auth/dist/crypto/password.mjs.
//
// This service used to write and check bcrypt hashes, which Better Auth never
// accepts: every account holds a scrypt hash, so "current password" never
// matched and a password written here could never sign in. The hash is now
// made here in Better Auth's own format, so the service no longer calls the
// site's /api/auth/hash-password endpoint (audit AS-N04).
const (
	scryptN      = 16384
	scryptR      = 16
	scryptP      = 1
	scryptKeyLen = 64
	saltBytes    = 16
)

func scryptKey(password, saltHex string) ([]byte, error) {
	return scrypt.Key([]byte(norm.NFKC.String(password)), []byte(saltHex), scryptN, scryptR, scryptP, scryptKeyLen)
}

// HashPassword returns a Better Auth compatible hash of password.
func HashPassword(password string) (string, error) {
	salt := make([]byte, saltBytes)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("salt: %w", err)
	}
	saltHex := hex.EncodeToString(salt)
	key, err := scryptKey(password, saltHex)
	if err != nil {
		return "", fmt.Errorf("scrypt: %w", err)
	}
	return saltHex + ":" + hex.EncodeToString(key), nil
}

// CheckPassword reports whether password matches a stored hash: Better Auth's
// scrypt format, or a legacy bcrypt hash.
func CheckPassword(hash, password string) bool {
	if strings.HasPrefix(hash, "$2") {
		return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
	}
	saltHex, keyHex, ok := strings.Cut(hash, ":")
	if !ok || saltHex == "" || keyHex == "" {
		return false
	}
	want, err := hex.DecodeString(keyHex)
	if err != nil {
		return false
	}
	got, err := scryptKey(password, saltHex)
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare(got, want) == 1
}
