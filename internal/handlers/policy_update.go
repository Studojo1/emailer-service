package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/studojo/emailer-service/internal/store"
)

// Policy-update notice (Terms §22 / Privacy §20: "We will email you at least 7
// days before a material change takes effect").
//
// POST /v1/email/policy-update queues the policy-update service email to every
// verified user for one effective date. Each recipient gets a scheduled_emails
// row of type "policy-update:<YYYY-MM-DD>"; the (user_id, email_type) unique
// index plus the scheduler's sent check mean re-running the trigger for the same
// date never sends anyone a second copy. The scheduler drains the rows at the
// normal paced rate. It is a service email: no unsubscribe, never skipped for
// marketing opt-outs.

// PolicyUpdateTypePrefix prefixes the scheduled_emails type for policy notices.
const PolicyUpdateTypePrefix = "policy-update:"

// PolicyUpdateMinNotice is the shortest notice the trigger accepts.
const PolicyUpdateMinNotice = 7 * 24 * time.Hour

// PolicyUpdateEmailType is the scheduled_emails type for an effective date.
func PolicyUpdateEmailType(effective time.Time) string {
	return PolicyUpdateTypePrefix + effective.Format("2006-01-02")
}

// ParsePolicyUpdateEmailType returns the effective date carried by a
// policy-update scheduled_emails type.
func ParsePolicyUpdateEmailType(emailType string) (time.Time, bool) {
	if !strings.HasPrefix(emailType, PolicyUpdateTypePrefix) {
		return time.Time{}, false
	}
	d, err := time.Parse("2006-01-02", strings.TrimPrefix(emailType, PolicyUpdateTypePrefix))
	return d, err == nil
}

// FormatEffectiveDate renders a date the way the email shows it: "1 November 2026".
func FormatEffectiveDate(d time.Time) string {
	return d.Format("2 January 2006")
}

// FirstName returns the first word of a display name, or "there".
func FirstName(name string) string {
	if f := strings.Fields(name); len(f) > 0 {
		return f[0]
	}
	return "there"
}

// policyUpdateStore is the part of the store the trigger needs (a fake in tests).
type policyUpdateStore interface {
	ListVerifiedUsers(ctx context.Context) ([]store.User, error)
	HasScheduledOrReceivedEmail(ctx context.Context, userID, emailType string) (bool, error)
	CreateScheduledEmail(ctx context.Context, userID, emailType string, scheduledAt time.Time) error
}

// QueuePolicyUpdate queues one policy-update row per verified user for the
// effective date, skipping anyone who already has one (queued or sent). Rows are
// staggered like bulk-send (10s apart, 2 min pause every 20).
func QueuePolicyUpdate(ctx context.Context, st policyUpdateStore, effective, now time.Time) (queued, skipped, failed int, err error) {
	users, err := st.ListVerifiedUsers(ctx)
	if err != nil {
		return 0, 0, 0, err
	}
	emailType := PolicyUpdateEmailType(effective)
	delay := time.Duration(0)
	for _, u := range users {
		exists, derr := st.HasScheduledOrReceivedEmail(ctx, u.ID, emailType)
		if derr != nil {
			// Unknown state: don't queue. The unique index would stop a duplicate
			// row anyway, but a re-run is the safe way to pick these up.
			slog.Error("policy-update: dedup check failed", "user_id", u.ID, "error", derr)
			failed++
			continue
		}
		if exists {
			skipped++
			continue
		}
		if queued > 0 && queued%20 == 0 {
			delay += 2 * time.Minute
		}
		if cerr := st.CreateScheduledEmail(ctx, u.ID, emailType, now.Add(delay)); cerr != nil {
			slog.Error("policy-update: queue failed", "user_id", u.ID, "error", cerr)
			failed++
			continue
		}
		delay += 10 * time.Second
		queued++
	}
	return queued, skipped, failed, nil
}

// PolicyUpdateRequest is the body for POST /v1/email/policy-update.
type PolicyUpdateRequest struct {
	EffectiveDate string `json:"effective_date"` // YYYY-MM-DD
}

// HandlePolicyUpdate handles POST /v1/email/policy-update. Admin-only in the
// same way as bulk-send: it requires X-Internal-Secret, which only the
// control-plane admin proxy (after authenticating an admin) and ops hold. The
// public email.studojo.com ingress carries no secret.
func (h *Handler) HandlePolicyUpdate(w http.ResponseWriter, r *http.Request) {
	if !requireInternalSecret(w, r) {
		return
	}
	var req PolicyUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "invalid request body", http.StatusBadRequest)
		return
	}
	effective, err := time.Parse("2006-01-02", strings.TrimSpace(req.EffectiveDate))
	if err != nil {
		writeError(w, "effective_date must be YYYY-MM-DD", http.StatusBadRequest)
		return
	}
	now := time.Now().UTC()
	if effective.Sub(now) < PolicyUpdateMinNotice {
		writeError(w, "effective_date must be at least 7 days from now (Terms §22)", http.StatusBadRequest)
		return
	}
	queued, skipped, failed, err := QueuePolicyUpdate(r.Context(), h.Store, effective, now)
	if err != nil {
		slog.Error("policy-update: list verified users", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}
	slog.Info("policy-update queued", "effective_date", req.EffectiveDate, "queued", queued, "skipped", skipped, "failed", failed)
	writeJSON(w, map[string]interface{}{
		"message":        fmt.Sprintf("queued policy-update for %d users (skipped %d already queued or sent, %d failed)", queued, skipped, failed),
		"email_type":     PolicyUpdateEmailType(effective),
		"effective_date": FormatEffectiveDate(effective),
		"queued":         queued,
		"skipped":        skipped,
		"failed":         failed,
	}, http.StatusOK)
}
