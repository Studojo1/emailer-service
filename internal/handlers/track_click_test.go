package handlers

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// The click tracker on email.studojo.com must only ever send people to our own
// sites. A bare suffix check let lookalike hosts such as evilstudojo.com through.
func TestTrackClickRedirectsOnlyToOwnHosts(t *testing.T) {
	const home = "https://studojo.com"
	cases := []struct{ dest, want string }{
		{"https://studojo.com/outreach/results?utm_source=email", "https://studojo.com/outreach/results?utm_source=email"},
		{"https://www.studojo.com/cc", "https://www.studojo.com/cc"},
		{"https://studojo.pro/outreach/results", "https://studojo.pro/outreach/results"},
		{"https://app.studojo.pro/", "https://app.studojo.pro/"},
		{"https://evilstudojo.com/login", home},
		{"https://evilstudojo.pro/login", home},
		{"https://studojo.com.evil.com/", home},
		{"https://evil.com/?next=studojo.com", home},
		{"javascript:alert(1)//studojo.com", home},
		{"//evilstudojo.com/", home},
		{"", home},
	}
	for _, c := range cases {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/v1/email/click/?u="+url.QueryEscape(c.dest), nil)
		(&Handler{EmailFrontendURL: home}).HandleTrackClick(w, r)
		if w.Code != http.StatusFound {
			t.Fatalf("%q: status %d, want 302", c.dest, w.Code)
		}
		if got := w.Header().Get("Location"); got != c.want {
			t.Errorf("%q: redirected to %q, want %q", c.dest, got, c.want)
		}
	}
}
