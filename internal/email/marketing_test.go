package email

import (
	"context"
	"encoding/json"
	"errors"
	"html"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

const (
	footerSentence = "You're getting this because you have a Studojo account."
	footerLink     = "Unsubscribe from tips and offers"
	footerCompany  = "Studojo Labs Private Limited, Bengaluru, India"
	testSecret     = "test-unsubscribe-secret"
	testBaseURL    = "https://email.studojo.com"
)

// templateFiles lists every template name in templates/.
func templateFiles(t *testing.T) []string {
	t.Helper()
	paths, err := filepath.Glob("../../templates/*.html")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no templates found: %v", err)
	}
	var names []string
	for _, p := range paths {
		names = append(names, strings.TrimSuffix(filepath.Base(p), ".html"))
	}
	return names
}

// Every template file is in exactly one class, every classified name has a
// file, and every template loaded at startup is classified.
func TestEveryTemplateIsClassified(t *testing.T) {
	files := map[string]bool{}
	for _, n := range templateFiles(t) {
		files[n] = true
		count := 0
		for _, set := range []map[string]bool{serviceTemplates, marketingTemplates, templateFragments} {
			if set[n] {
				count++
			}
		}
		if count != 1 {
			t.Errorf("template %s is in %d classes, want exactly 1 (add it to classification.go)", n, count)
		}
	}
	for _, set := range []map[string]bool{serviceTemplates, marketingTemplates, templateFragments} {
		for n := range set {
			if !files[n] {
				t.Errorf("classified template %s has no file", n)
			}
		}
	}
	for _, n := range RegisteredTemplates {
		if !files[n] {
			t.Errorf("registered template %s has no file", n)
		}
	}
}

// fakeLogger is the store as the Sender sees it.
type fakeLogger struct {
	mu        sync.Mutex
	optedOut  map[string]bool // user id or address
	optErr    error
	optChecks int
}

func (f *fakeLogger) LogEmailSent(ctx context.Context, userID, userName, emailTo, templateName, fromAddress string) error {
	return nil
}
func (f *fakeLogger) IsEmailSuppressed(ctx context.Context, email string) (bool, error) {
	return false, nil
}
func (f *fakeLogger) IsMarketingOptedOut(ctx context.Context, userID, email string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.optChecks++
	if f.optErr != nil {
		return false, f.optErr
	}
	return f.optedOut[userID] || f.optedOut[NormalizeEmail(email)], nil
}

// captured is one provider request.
type captured struct {
	Headers map[string]string
	HTML    string
}

// newACSSender returns a Sender whose ACS endpoint is a test server that
// records every send.
func newACSSender(t *testing.T) (*Sender, *[]captured, *sync.Mutex) {
	t.Helper()
	var mu sync.Mutex
	var sends []captured
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var p struct {
			Headers map[string]string `json:"headers"`
			Content struct {
				HTML string `json:"html"`
			} `json:"content"`
		}
		_ = json.Unmarshal(body, &p)
		mu.Lock()
		sends = append(sends, captured{Headers: p.Headers, HTML: p.Content.HTML})
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(srv.Close)
	client, err := NewClient("endpoint="+srv.URL+";accesskey=a2V5", "no-reply@studojo.com")
	if err != nil {
		t.Fatal(err)
	}
	return newTestSender(t, client), &sends, &mu
}

func newTestSender(t *testing.T, client *Client) *Sender {
	t.Helper()
	tr, err := NewTemplateRenderer("../../templates")
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.LoadAllTemplates(); err != nil {
		t.Fatal(err)
	}
	s := NewSender(client, tr)
	s.SetUnsubscribeSecret(testSecret, testBaseURL)
	return s
}

func userCtx(uid string) context.Context {
	return context.WithValue(context.Background(), UserIDKey, uid)
}

func sampleData() map[string]interface{} {
	return map[string]interface{}{
		"UserName": "Asha Rao", "FirstName": "Asha", "DashboardURL": "https://studojo.com/",
		"OutreachURL": "https://studojo.com/outreach", "ActionURL": "https://studojo.com/x",
		"EffectiveDate": "1 November 2026", "Credits": 5, "Delivered": 1, "Total": 2, "Replied": 0,
		"Subject": "s", "Message": "m", "Name": "Asha", "Email": "a@example.com",
	}
}

// Table-driven: every marketing template renders the unsubscribe footer with
// the signed link; every service template renders no unsubscribe at all.
func TestFooterMatchesClassification(t *testing.T) {
	tr, err := NewTemplateRenderer("../../templates")
	if err != nil {
		t.Fatal(err)
	}
	s := NewSender(nil, tr)
	s.SetUnsubscribeSecret(testSecret, testBaseURL)

	for _, name := range templateFiles(t) {
		if templateFragments[name] {
			continue
		}
		name := name
		t.Run(name, func(t *testing.T) {
			if err := tr.LoadTemplate(name); err != nil {
				t.Fatalf("load: %v", err)
			}
			// Callers sometimes pass a stale UnsubscribeURL; service sends must drop it.
			data := sampleData()
			data["UnsubscribeURL"] = "https://stale.example/unsub"
			dm, headers := s.prepareSend("asha@example.com", "user-1", name, data)
			out, err := tr.Render(name, dm)
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			out = html.UnescapeString(out)
			if marketingTemplates[name] {
				want := UnsubscribeURL(testBaseURL, testSecret, "user-1", "")
				for _, w := range []string{footerSentence, footerLink, footerCompany, want} {
					if !strings.Contains(out, w) {
						t.Errorf("marketing template missing %q", w)
					}
				}
				if headers["List-Unsubscribe"] != "<"+want+">" {
					t.Errorf("List-Unsubscribe = %q", headers["List-Unsubscribe"])
				}
			} else {
				if strings.Contains(strings.ToLower(out), "unsubscribe") {
					t.Errorf("service template %s contains an unsubscribe link", name)
				}
				if headers != nil {
					t.Errorf("service template got headers %v", headers)
				}
			}
		})
	}
}

// The provider request for a marketing send carries both RFC 8058 headers; a
// service send carries neither. Checked on the ACS path and the Resend path.
func TestMarketingSendHeaders(t *testing.T) {
	s, sends, mu := newACSSender(t)
	ctx := userCtx("user-1")
	if err := s.SendTemplateEmail(ctx, "asha@example.com", "cc-nudge-1", sampleData()); err != nil {
		t.Fatal(err)
	}
	if err := s.SendTemplateEmail(ctx, "asha@example.com", "payment-thankyou", sampleData()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(*sends) != 2 {
		t.Fatalf("want 2 sends, got %d", len(*sends))
	}
	m := (*sends)[0].Headers
	if !strings.HasPrefix(m["List-Unsubscribe"], "<"+testBaseURL+UnsubscribePath+"?uid=user-1&t=") {
		t.Errorf("ACS marketing List-Unsubscribe = %q", m["List-Unsubscribe"])
	}
	if m["List-Unsubscribe-Post"] != "List-Unsubscribe=One-Click" {
		t.Errorf("ACS marketing List-Unsubscribe-Post = %q", m["List-Unsubscribe-Post"])
	}
	if len((*sends)[1].Headers) != 0 {
		t.Errorf("ACS service send carried headers %v", (*sends)[1].Headers)
	}

	// Resend: same headers in its JSON payload.
	var got map[string]interface{}
	client, _ := NewClient("re_test", "no-reply@studojo.com")
	client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &got)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"id":"x"}`)), Header: http.Header{}}, nil
	})}
	rs := newTestSender(t, client)
	if err := rs.SendTemplateEmail(context.Background(), "reg@example.com", "cc-webinar-toolkit-recap", sampleData()); err != nil {
		t.Fatal(err)
	}
	h, _ := got["headers"].(map[string]interface{})
	lu, _ := h["List-Unsubscribe"].(string)
	// No user id: the link is keyed by address instead.
	if !strings.Contains(lu, UnsubscribePath+"?e=reg%40example.com&t=") || h["List-Unsubscribe-Post"] != "List-Unsubscribe=One-Click" {
		t.Errorf("Resend headers = %v", h)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// An opted-out user gets no marketing but still gets every service email; a
// failed opt-out lookup blocks marketing (fail closed) but never service.
func TestOptedOutSkipsMarketingOnly(t *testing.T) {
	s, sends, mu := newACSSender(t)
	fl := &fakeLogger{optedOut: map[string]bool{"user-1": true, "gone@example.com": true}}
	s.SetLogger(fl)

	count := func() int { mu.Lock(); defer mu.Unlock(); return len(*sends) }

	for _, tmpl := range []string{"cc-nudge-1", "cc-outreach-coupon", "welcome", "checkin-reminder"} {
		if err := s.SendTemplateEmail(userCtx("user-1"), "asha@example.com", tmpl, sampleData()); err != nil {
			t.Fatalf("%s: %v", tmpl, err)
		}
	}
	// Opted out by address only (no account): also skipped.
	if err := s.SendTemplateEmail(context.Background(), "Gone@Example.com", "cc-webinar-toolkit-recap", sampleData()); err != nil {
		t.Fatal(err)
	}
	if n := count(); n != 0 {
		t.Fatalf("opted-out user got %d marketing emails", n)
	}

	for _, tmpl := range []string{"payment-thankyou", "forgot-password", "outreach-gmail-reconnect", "policy-update", "cc-webinar-toolkit"} {
		if err := s.SendTemplateEmail(userCtx("user-1"), "asha@example.com", tmpl, sampleData()); err != nil {
			t.Fatalf("%s: %v", tmpl, err)
		}
	}
	if n := count(); n != 5 {
		t.Fatalf("opted-out user got %d of 5 service emails", n)
	}

	fl.optErr = errors.New("db down")
	if err := s.SendTemplateEmail(userCtx("user-2"), "b@example.com", "cc-nudge-2", sampleData()); err == nil {
		t.Error("marketing send with a failed opt-out check should return an error")
	}
	checks := fl.optChecks
	if err := s.SendTemplateEmail(userCtx("user-2"), "b@example.com", "verify-email", sampleData()); err != nil {
		t.Errorf("service send must not depend on the opt-out check: %v", err)
	}
	if fl.optChecks != checks {
		t.Error("service send consulted the opt-out table")
	}
	if n := count(); n != 6 {
		t.Fatalf("want 6 sends, got %d", n)
	}
}

func TestUnsubscribeTokenSignature(t *testing.T) {
	uidURL := UnsubscribeURL(testBaseURL, testSecret, "user-1", "asha@example.com")
	tok := uidURL[strings.Index(uidURL, "&t=")+3:]
	if !VerifyUnsubscribeToken(testSecret, "user-1", "", tok) {
		t.Fatal("valid uid token rejected")
	}
	cases := map[string]bool{
		"other user":     VerifyUnsubscribeToken(testSecret, "user-2", "", tok),
		"wrong secret":   VerifyUnsubscribeToken("other", "user-1", "", tok),
		"empty secret":   VerifyUnsubscribeToken("", "user-1", "", unsubscribeMAC("", "user-1")),
		"tampered":       VerifyUnsubscribeToken(testSecret, "user-1", "", strings.Repeat("0", len(tok))),
		"empty token":    VerifyUnsubscribeToken(testSecret, "user-1", "", ""),
		"uid as email":   VerifyUnsubscribeToken(testSecret, "", "user-1", tok),
		"both keys sent": VerifyUnsubscribeToken(testSecret, "user-1", "asha@example.com", tok),
	}
	for name, ok := range cases {
		if ok {
			t.Errorf("%s: token accepted", name)
		}
	}

	emailURL := UnsubscribeURL(testBaseURL, testSecret, "", " Asha@Example.com ")
	etok := emailURL[strings.Index(emailURL, "&t=")+3:]
	if !strings.Contains(emailURL, "?e=asha%40example.com&") {
		t.Errorf("email URL not normalised: %s", emailURL)
	}
	if !VerifyUnsubscribeToken(testSecret, "", "ASHA@example.com", etok) {
		t.Error("valid email token rejected")
	}
	if VerifyUnsubscribeToken(testSecret, "", "other@example.com", etok) {
		t.Error("email token accepted for another address")
	}
	if UnsubscribeURL(testBaseURL, "", "user-1", "") != "" {
		t.Error("unsigned link built with an empty secret")
	}
}

// The policy-update notice renders the approved copy and subject.
func TestPolicyUpdateTemplate(t *testing.T) {
	tr, _ := NewTemplateRenderer("../../templates")
	if err := tr.LoadTemplate("policy-update"); err != nil {
		t.Fatal(err)
	}
	s := NewSender(nil, tr)
	out, err := tr.Render("policy-update", sampleData())
	if err != nil {
		t.Fatal(err)
	}
	out = html.UnescapeString(out)
	for _, want := range []string{
		"Hi Asha,",
		"New versions of our Terms of Service, Privacy Policy and Refund Policy take effect on 1 November 2026.",
		"Purchases are final, except for payment errors, things we never delivered, and campaigns that stop completely because of us. Purchases made before 1 November 2026 keep the policy you bought under.",
		"Studojo is for people aged 18 and over.",
		"We now list every provider we use, and explain how LinkedIn automation, AutoApply and AI-written emails work.",
		"If you keep using Studojo after that date, the new versions apply. If you don't agree, you can delete your account in Settings.",
		"https://studojo.com/terms", "https://studojo.com/privacy", "https://studojo.com/refund-policy",
		footerCompany,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("policy-update missing %q", want)
		}
	}
	subj, _ := s.getSubject("policy-update", sampleData())
	if subj != "We're updating our policies on 1 November 2026" {
		t.Errorf("subject = %q", subj)
	}
}
