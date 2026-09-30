package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// change-password is reached only through the control-plane gateway, which
// checks the user's token and adds X-Internal-Secret. A direct call without
// the secret must not get as far as checking a password.
func TestChangePasswordRequiresInternalSecret(t *testing.T) {
	t.Setenv("INTERNAL_SECRET", "test-internal-secret")
	h := &Handler{}
	body := `{"user_id":"user-a","current_password":"old-password","new_password":"new-password-1"}`
	for _, secret := range []string{"", "wrong-secret"} {
		req := httptest.NewRequest(http.MethodPost, "/v1/email/change-password", strings.NewReader(body))
		if secret != "" {
			req.Header.Set("X-Internal-Secret", secret)
		}
		rec := httptest.NewRecorder()
		h.HandleChangePassword(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("secret %q: got %d, want 401", secret, rec.Code)
		}
	}
}
