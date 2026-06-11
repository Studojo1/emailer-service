package scheduler

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/studojo/emailer-service/internal/email"
	"github.com/studojo/emailer-service/internal/store"
)

// Scheduler polls for due scheduled emails and sends them
type Scheduler struct {
	Store          *store.PostgresStore
	Sender         *email.Sender
	FrontendURL    string
	rateLimitUntil time.Time // circuit breaker: stop sending until quota resets
}

// NewScheduler creates a new Scheduler
func NewScheduler(s *store.PostgresStore, sender *email.Sender, frontendURL string) *Scheduler {
	return &Scheduler{Store: s, Sender: sender, FrontendURL: frontendURL}
}

// Run starts the scheduler loop. Call in a goroutine.
func (sc *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	catchupTicker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	defer catchupTicker.Stop()

	// Process immediately on start
	sc.processDue(ctx)
	sc.runCatchup(ctx)

	for {
		select {
		case <-ticker.C:
			sc.processDue(ctx)
		case <-catchupTicker.C:
			sc.runCatchup(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (sc *Scheduler) processDue(ctx context.Context) {
	// Circuit breaker: if ACS rate-limited us recently, wait until quota resets
	if time.Now().Before(sc.rateLimitUntil) {
		remaining := time.Until(sc.rateLimitUntil).Round(time.Minute)
		slog.Info("scheduler: rate limit backoff active, skipping tick", "resume_in", remaining)
		return
	}

	emails, err := sc.Store.GetDueScheduledEmails(ctx)
	if err != nil {
		slog.Error("scheduler: failed to fetch due emails", "error", err)
		return
	}
	for i, e := range emails {
		if i > 0 {
			// 20s between sends = 3/min = 180/hr — just under the 200/hr ACS free-tier cap
			time.Sleep(20 * time.Second)
		}
		if rateLimited := sc.send(ctx, e); rateLimited {
			// Both ACS resources exhausted — stop the batch and wait 65 min for quota reset
			sc.rateLimitUntil = time.Now().Add(65 * time.Minute)
			slog.Warn("scheduler: ACS rate limit hit, pausing sends for 65 minutes")
			return
		}
	}
}

// runCatchup re-queues platform-owned new-flow emails for users who crossed a
// trigger window while the service was down. Coach-owned sequences (dormancy,
// returning, old-user) are driven by the coach backend cron, not caught up here.
func (sc *Scheduler) runCatchup(ctx context.Context) {
	sc.catchupCC(ctx)
}

// catchupCC queues the first email of a platform-owned cc sequence for users who
// passed its entry window but have no row of that type yet. Only the first step is
// caught up; the rest chain off it in send().
func (sc *Scheduler) catchupCC(ctx context.Context) {
	type ccCatchupSpec struct {
		emailType  string
		minMinutes int
		baseOffset time.Duration
	}
	specs := []ccCatchupSpec{
		{"cc_outreach_nudge_d1", 7 * 60, 0}, // outreach not-used, 7h after signup
	}
	now := time.Now().UTC()
	for _, spec := range specs {
		users, err := sc.Store.ListUsersWithoutEmail(ctx, spec.emailType, spec.minMinutes)
		if err != nil {
			slog.Error("catchup cc: list failed", "type", spec.emailType, "error", err)
			continue
		}
		queued := 0
		for i, u := range users {
			sendAt := now.Add(spec.baseOffset + time.Duration(i)*time.Second)
			if err := sc.Store.CreateScheduledEmail(ctx, u.ID, spec.emailType, sendAt); err != nil {
				slog.Error("catchup cc: schedule failed", "type", spec.emailType, "user_id", u.ID, "error", err)
				continue
			}
			queued++
		}
		if queued > 0 {
			slog.Info("catchup cc: queued missed emails", "type", spec.emailType, "count", queued)
		}
	}
}

// isCCMarketingType returns true for cc sequence emails suppressed for paying
// Outreach customers.
func isCCMarketingType(emailType string) bool {
	return strings.HasPrefix(emailType, "cc_")
}

// send attempts to send a single scheduled email.
// Returns true if ACS rate-limited us (caller should pause the batch).
func (sc *Scheduler) send(ctx context.Context, e store.ScheduledEmail) (rateLimited bool) {
	user, err := sc.Store.GetUserByID(ctx, e.UserID)
	if err != nil || user == nil {
		slog.Error("scheduler: user not found, discarding", "user_id", e.UserID, "email_type", e.EmailType)
		_ = sc.Store.MarkScheduledEmailSent(ctx, e.ID)
		return false
	}

	prefs, err := sc.Store.GetEmailPreferences(ctx, e.UserID)
	if err != nil {
		slog.Error("scheduler: failed to get preferences", "user_id", e.UserID)
		return false
	}
	if !prefs.ProductEmails {
		slog.Info("scheduler: skipping email, product emails disabled", "user_id", e.UserID, "type", e.EmailType)
		_ = sc.Store.MarkScheduledEmailSent(ctx, e.ID)
		return false
	}

	// Suppress cc marketing sequences for paid users — silently drain the row
	if isCCMarketingType(e.EmailType) {
		paid, err := sc.Store.IsUserPaid(ctx, e.UserID)
		if err != nil {
			slog.Warn("scheduler: paid check failed, sending cc email anyway", "user_id", e.UserID, "err", err)
		} else if paid {
			slog.Info("scheduler: suppressing cc marketing for paid user", "user_id", e.UserID, "type", e.EmailType)
			_ = sc.Store.MarkScheduledEmailSent(ctx, e.ID)
			return false
		}
	}

	already, err := sc.Store.HasReceivedEmail(ctx, e.UserID, e.EmailType)
	if err != nil {
		slog.Error("scheduler: dedup check failed", "user_id", e.UserID, "type", e.EmailType, "error", err)
	} else if already {
		slog.Info("scheduler: already sent, marking done", "user_id", e.UserID, "type", e.EmailType)
		_ = sc.Store.MarkScheduledEmailSent(ctx, e.ID)
		return false
	}

	var templateData map[string]interface{}
	switch e.EmailType {
	case "leads_ready":
		templateData = map[string]interface{}{"UserName": user.Name, "OutreachURL": sc.FrontendURL + "/outreach"}
	case "welcome":
		templateData = map[string]interface{}{"UserName": user.Name, "DashboardURL": sc.FrontendURL + "/"}
	default:
		if strings.HasPrefix(e.EmailType, "cc_") {
			// cc sequence emails are self-contained (links hardcoded). The two
			// coupon-bearing types get a default code from env (DEFAULT_COUPON_CODE)
			// when scheduled (e.g. the abandoned-checkout coupon); instant coupon
			// events carry their own code on the payload instead.
			templateData = map[string]interface{}{
				"UserName":     user.Name,
				"DashboardURL": sc.FrontendURL + "/",
			}
			if e.EmailType == "cc_outreach_coupon" || e.EmailType == "cc_coupon_unlock" {
				code := os.Getenv("DEFAULT_COUPON_CODE")
				if code == "" {
					code = "STUDOJO20"
				}
				templateData["CouponCode"] = code
			}
		} else {
			slog.Warn("scheduler: unknown email type, skipping", "type", e.EmailType)
			_ = sc.Store.MarkScheduledEmailSent(ctx, e.ID)
			return false
		}
	}

	templateName := emailTypeToTemplate(e.EmailType)
	ctx = context.WithValue(ctx, email.UserIDKey, user.ID)
	ctx = context.WithValue(ctx, email.UserNameKey, user.Name)

	if err := sc.Sender.SendTemplateEmail(ctx, user.Email, templateName, templateData); err != nil {
		if strings.Contains(err.Error(), "429") || strings.Contains(err.Error(), "TooManyRequests") || strings.Contains(err.Error(), "exhausted") {
			return true // signal circuit breaker
		}
		slog.Error("scheduler: failed to send email", "error", err, "user_id", e.UserID, "type", e.EmailType)
		return false
	}

	if err := sc.Store.MarkScheduledEmailSent(ctx, e.ID); err != nil {
		slog.Error("scheduler: failed to mark sent", "error", err, "id", e.ID)
	}
	slog.Info("scheduler: email sent", "user_id", e.UserID, "type", e.EmailType, "email", user.Email)

	// cc sequences are scheduled in full up front by ScheduleCCSequence, so there
	// is no mid-stream chaining to do here.
	return false
}

func emailTypeToTemplate(emailType string) string {
	switch emailType {
	case "leads_ready":
		return "leads-ready"
	default:
		// cc_* sequence types map 1:1 to cc-* templates (underscore -> hyphen).
		if strings.HasPrefix(emailType, "cc_") {
			return strings.ReplaceAll(emailType, "_", "-")
		}
		return emailType
	}
}
