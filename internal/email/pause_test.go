package email

import (
	"context"
	"testing"
)

// With the email pause on, only the account emails reach the provider: marketing, receipts, campaign notices,
// confirmations, the contact form and the internal ops alerts are all held, and each held send returns nil so
// the scheduler drains its row instead of retrying it later.
func TestPauseHoldsEverythingButAccountEmails(t *testing.T) {
	s, sends, mu := newACSSender(t)
	s.SetPaused(true)
	count := func() int { mu.Lock(); defer mu.Unlock(); return len(*sends) }

	held := []string{
		"cc-welcome-new-user", "cc-outreach-nudge-d1", "cc-outreach-push1", "cc-outreach-convert1", "checkin-reminder",
		"leads-ready", "payment-thankyou", "internship-applied", "cc-dna-ready", "outreach-launch-nudge",
		"outreach-campaign-finished", "outreach-gmail-reconnect", "contact-form", "ops-alert",
	}
	for _, tmpl := range held {
		if err := s.SendTemplateEmail(userCtx("user-1"), "asha@example.com", tmpl, sampleData()); err != nil {
			t.Fatalf("%s: a held send must return nil, got %v", tmpl, err)
		}
	}
	if n := count(); n != 0 {
		t.Fatalf("held templates reached the provider: %d sends", n)
	}
	if err := s.SendOpsAlert(context.Background(), "ops@example.com", "alert", "<p>alert</p>"); err != nil {
		t.Fatalf("a held ops alert must return nil, got %v", err)
	}
	if n := count(); n != 0 {
		t.Fatalf("the ops alert reached the provider while paused")
	}

	for _, tmpl := range []string{"forgot-password", "verify-email", "login-code", "password-changed"} {
		if err := s.SendTemplateEmail(userCtx("user-1"), "asha@example.com", tmpl, sampleData()); err != nil {
			t.Fatalf("%s: %v", tmpl, err)
		}
	}
	if n := count(); n != 4 {
		t.Fatalf("want the 4 account emails sent while paused, got %d", n)
	}

	// Lifted: everything sends again.
	s.SetPaused(false)
	if err := s.SendTemplateEmail(userCtx("user-1"), "asha@example.com", "payment-thankyou", sampleData()); err != nil {
		t.Fatal(err)
	}
	if n := count(); n != 5 {
		t.Fatalf("want a send after the pause is lifted, got %d", n)
	}
}

func TestPausedFromEnv(t *testing.T) {
	for v, want := range map[string]bool{"1": true, "true": true, "TRUE": true, " 1 ": true, "0": false, "": false, "no": false} {
		t.Setenv("EMAILS_PAUSED", v)
		if got := PausedFromEnv(); got != want {
			t.Errorf("EMAILS_PAUSED=%q: got %v, want %v", v, got, want)
		}
	}
}
