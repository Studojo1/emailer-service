package email

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"strings"
)

// Signed one-click unsubscribe links.
//
// Two forms, both HMAC-SHA256 with UNSUBSCRIBE_SECRET, hex encoded:
//
//	/v1/email/unsubscribe?uid=<user id>&t=hex(HMAC(uid))
//	/v1/email/unsubscribe?e=<email>&t=hex(HMAC("email:" + lower(trim(email))))
//
// The uid form is the one we have always sent, so links already in inboxes keep
// working. The email form covers marketing sent to someone we only know by
// address (webinar registrants, admin one-off sends). The "email:" prefix keeps
// the two message spaces apart.

// UnsubscribePath is the public route that serves the confirm page (GET) and
// records the opt-out (POST, including RFC 8058 one-click).
const UnsubscribePath = "/v1/email/unsubscribe"

func unsubscribeMAC(secret, msg string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(msg))
	return hex.EncodeToString(mac.Sum(nil))
}

// NormalizeEmail lower-cases and trims an address for opt-out keys.
func NormalizeEmail(addr string) string {
	return strings.ToLower(strings.TrimSpace(addr))
}

// UnsubscribeURL builds a signed unsubscribe link. It uses the user id when
// known, otherwise the address. Returns "" if the secret or both keys are
// missing: an unsigned link would let anyone opt anyone out.
func UnsubscribeURL(baseURL, secret, userID, addr string) string {
	if secret == "" {
		return ""
	}
	base := strings.TrimSuffix(baseURL, "/") + UnsubscribePath
	if userID != "" {
		return base + "?uid=" + url.QueryEscape(userID) + "&t=" + unsubscribeMAC(secret, userID)
	}
	if e := NormalizeEmail(addr); e != "" {
		return base + "?e=" + url.QueryEscape(e) + "&t=" + unsubscribeMAC(secret, "email:"+e)
	}
	return ""
}

// VerifyUnsubscribeToken checks a token from an unsubscribe link in constant
// time. Exactly one of userID or addr should be set (whichever the link
// carried). An empty secret never verifies.
func VerifyUnsubscribeToken(secret, userID, addr, token string) bool {
	if secret == "" || token == "" {
		return false
	}
	var expected string
	switch {
	case userID != "" && addr == "":
		expected = unsubscribeMAC(secret, userID)
	case userID == "" && NormalizeEmail(addr) != "":
		expected = unsubscribeMAC(secret, "email:"+NormalizeEmail(addr))
	default:
		return false
	}
	return hmac.Equal([]byte(token), []byte(expected))
}
