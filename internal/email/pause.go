package email

import (
	"os"
	"strings"
)

// Email pause (the owner, 29 Oct 2026): "kill all the emails being sent by us to a user now, and the internal team
// emails as well; the only emails should be the ones the students send to hiring managers". With EMAILS_PAUSED=1
// this service sends nothing, the internal ops alerts included, except the account emails below, which the owner
// kept so nobody is locked out of their account. The campaign emails students send to hiring managers go from the
// student's own Gmail through job-outreach-worker and never pass through this service.
//
// A paused send returns nil, as an opted-out or suppressed one does: the scheduler marks its row sent, so nothing
// queued during the pause goes out later when it is lifted. Lift it by setting EMAILS_PAUSED to 0 in the deploy
// workflow.

// pauseExempt are the emails that still go out while paused: sign-in and account security.
var pauseExempt = map[string]bool{
	"forgot-password":  true,
	"verify-email":     true,
	"login-code":       true,
	"password-changed": true,
}

// PausedFromEnv reads EMAILS_PAUSED: "1" or "true" pauses.
func PausedFromEnv() bool {
	v := strings.TrimSpace(os.Getenv("EMAILS_PAUSED"))
	return v == "1" || strings.EqualFold(v, "true")
}

// SetPaused turns the email pause on or off.
func (s *Sender) SetPaused(paused bool) {
	s.paused = paused
}

// Paused reports whether the email pause is on.
func (s *Sender) Paused() bool {
	return s.paused
}

// pausedFor reports whether a template is held back by the pause.
func (s *Sender) pausedFor(templateName string) bool {
	return s.paused && !pauseExempt[templateName]
}
