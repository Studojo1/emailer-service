package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// login-code mails a sign-in code, so the endpoint must refuse anything that
// isn't exactly six digits: no text, no links, nothing a caller could smuggle
// into an email that looks like it came from Studojo's security flow.
func TestLoginCodeRejectsNonCodes(t *testing.T) {
	t.Setenv("INTERNAL_SECRET", "s3cret")
	h := &Handler{} // no sender: a valid request would fail later, a bad one must stop here
	for _, code := range []string{"", "12345", "1234567", "12a456", "<b>1</b>", "123 45", "١٢٣٤٥٦"} {
		body := `{"to":"student@example.com","template":"login-code","code":` + quote(code) + `}`
		req := httptest.NewRequest(http.MethodPost, "/v1/email/send-template", strings.NewReader(body))
		req.Header.Set("X-Internal-Secret", "s3cret")
		w := httptest.NewRecorder()
		h.HandleSendTemplate(w, req)
		if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "six digits") {
			t.Errorf("code %q: got %d %s, want 400 six digits", code, w.Code, w.Body.String())
		}
	}
}

func TestIsSixDigits(t *testing.T) {
	if !isSixDigits("042917") {
		t.Error("042917 should be a code")
	}
	for _, s := range []string{"", "4291", "0429170", "04291x"} {
		if isSixDigits(s) {
			t.Errorf("%q should not be a code", s)
		}
	}
}

func quote(s string) string { return `"` + strings.ReplaceAll(s, `"`, `\"`) + `"` }
