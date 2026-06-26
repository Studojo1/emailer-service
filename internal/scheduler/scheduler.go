package scheduler

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/studojo/emailer-service/internal/email"
	"github.com/studojo/emailer-service/internal/handlers"
	"github.com/studojo/emailer-service/internal/store"
)

// Scheduler polls for due scheduled emails and sends them
type Scheduler struct {
	Store          *store.PostgresStore
	Sender         *email.Sender
	FrontendURL    string
	rateLimitUntil time.Time     // circuit breaker: stop sending until quota resets
	sendInterval   time.Duration // gap between sends, derived from the per-hour cap
	backoff        time.Duration // how long to pause the batch on a hard 429
}

// couponFallbackCode is the blanket coupon used only when minting a unique
// per-recipient code fails. Preserves the historical DEFAULT_COUPON_CODE behavior.
func couponFallbackCode() string {
	if code := os.Getenv("DEFAULT_COUPON_CODE"); code != "" {
		return code
	}
	return "STUDOJO20"
}

// ratePerHour returns the configured provider send cap per hour. Defaults to 180
// (just under the Azure free-tier 200/hr), override with EMAIL_RATE_PER_HOUR.
func ratePerHour() int {
	if v := os.Getenv("EMAIL_RATE_PER_HOUR"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 180
}

// maxMarketingPerWeek caps marketing emails per user per rolling 7 days, across
// ALL flows, to prevent fatigue. Default 3, override with EMAIL_MAX_PER_USER_PER_WEEK.
func maxMarketingPerWeek() int {
	if v := os.Getenv("EMAIL_MAX_PER_USER_PER_WEEK"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 3
}

// NewScheduler creates a new Scheduler. Pacing is derived from EMAIL_RATE_PER_HOUR
// so the same binary serves a free tier or a higher paid quota with no code change.
func NewScheduler(s *store.PostgresStore, sender *email.Sender, frontendURL string) *Scheduler {
	rph := ratePerHour()
	interval := time.Duration(float64(time.Hour) / float64(rph))
	if interval < time.Second {
		interval = time.Second // never busier than 1/s regardless of config
	}
	// On a hard rate-limit with no Retry-After, pause ~one hour for the quota window.
	return &Scheduler{
		Store:        s,
		Sender:       sender,
		FrontendURL:  frontendURL,
		sendInterval: interval,
		backoff:      65 * time.Minute,
	}
}

// Run starts the scheduler loop. Call in a goroutine.
func (sc *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	catchupTicker := time.NewTicker(1 * time.Hour)
	// Behavioral routing (Idea D) runs on a slow tick and is a no-op unless
	// BEHAVIORAL_ROUTING_PCT > 0. Slow + cohort-gated + shadow-by-default so it
	// can never mis-send at scale.
	behavioralTicker := time.NewTicker(30 * time.Minute)
	// Apollo credit-burn tripwire — cheap DB read, paged at most once/day.
	burnTicker := time.NewTicker(15 * time.Minute)
	defer ticker.Stop()
	defer catchupTicker.Stop()
	defer behavioralTicker.Stop()
	defer burnTicker.Stop()

	// Process immediately on start
	sc.processDue(ctx)
	sc.runCatchup(ctx)
	sc.checkApolloBurn(ctx)

	for {
		select {
		case <-ticker.C:
			sc.processDue(ctx)
		case <-catchupTicker.C:
			sc.runCatchup(ctx)
		case <-behavioralTicker.C:
			sc.runBehavioral(ctx)
		case <-burnTicker.C:
			sc.checkApolloBurn(ctx)
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

	// Priority-ordered: new/active-user emails always drain before old-user
	// re-engagement, so high-intent students are never starved by a big dormant
	// batch. The DB query orders by priority then scheduled_at (see the store).
	emails, err := sc.Store.GetDueScheduledEmails(ctx)
	if err != nil {
		slog.Error("scheduler: failed to fetch due emails", "error", err)
		return
	}
	for i, e := range emails {
		if i > 0 {
			// Pace to stay under the provider cap (EMAIL_RATE_PER_HOUR).
			time.Sleep(sc.sendInterval)
		}
		if rateLimited := sc.send(ctx, e); rateLimited {
			// Provider rate-limited us. Honour Retry-After if the sender surfaced
			// one; otherwise pause ~one hour for the quota window.
			pause := sc.backoff
			if ra := sc.Sender.LastRetryAfter(); ra > 0 {
				pause = ra + 30*time.Second
			}
			sc.rateLimitUntil = time.Now().Add(pause)
			slog.Warn("scheduler: provider rate limit hit, pausing sends", "pause", pause.Round(time.Second))
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

	// Engagement gate: this row sends no email. When it comes due, enrol the
	// flow's chase ONLY if the user hasn't opened/clicked the welcome (and, for
	// outreach, hasn't used the tool — that cancels the gate in the handler).
	// Works for every gated flow via the gate registry.
	if _, welcomeType, _, chase, ok := handlers.GateByType(e.EmailType); ok {
		engaged, err := sc.Store.HasEngagedWithWelcome(ctx, user.Email, welcomeType)
		if err != nil {
			// A transient DB error is NOT a signal that the user engaged. Marking
			// the gate sent here drops the user out of the chase forever on a hiccup
			// (audit B1). Leave the row pending so the next tick re-checks.
			slog.Warn("scheduler: gate engagement check failed, leaving gate pending for retry", "user_id", e.UserID, "err", err)
			return false
		}
		if engaged {
			slog.Info("scheduler: user engaged with welcome, not enrolling chase", "user_id", e.UserID, "gate", e.EmailType)
			_ = sc.Store.MarkScheduledEmailSent(ctx, e.ID)
			return false
		}
		// Not engaged: enrol the chase. Only consume the gate row if enrolment
		// actually landed — otherwise leave it pending so the next tick retries,
		// rather than silently dropping the user out of the chase forever.
		if err := handlers.ScheduleCCChase(ctx, sc.Store, e.UserID, time.Now().UTC(), chase); err != nil {
			slog.Error("scheduler: chase enrolment failed, leaving gate pending for retry", "user_id", e.UserID, "gate", e.EmailType, "err", err)
			return false
		}
		slog.Info("scheduler: no engagement, enrolled chase", "user_id", e.UserID, "gate", e.EmailType)
		_ = sc.Store.MarkScheduledEmailSent(ctx, e.ID)
		return false
	}

	// Per-step re-check: before sending any gated chase email, drop the rest if
	// the user has since engaged with that flow's welcome. Each gate's chase has
	// a known prefix and welcome template.
	if prefix, welcomeType, found := handlers.ChaseFor(e.EmailType); found {
		engaged, err := sc.Store.HasEngagedWithWelcome(ctx, user.Email, welcomeType)
		if err == nil && engaged {
			slog.Info("scheduler: user engaged since enrol, cancelling remaining chase", "user_id", e.UserID, "prefix", prefix)
			_, _ = sc.Store.CancelPendingEmailsByPrefix(ctx, e.UserID, prefix)
			_ = sc.Store.MarkScheduledEmailSent(ctx, e.ID)
			return false
		}
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

	// Per-user frequency cap (fatigue): no inbox should get more than N marketing
	// emails in a rolling week, however many flows the user is enrolled in.
	// DEFER (push the row forward), never drop — the email still goes, just later.
	// Transactional emails are exempt (only cc marketing is capped).
	if isCCMarketingType(e.EmailType) {
		recent, ferr := sc.Store.CountMarketingSentSince(ctx, user.Email, 7*24*time.Hour)
		if ferr != nil {
			slog.Warn("scheduler: frequency check failed, sending anyway", "user_id", e.UserID, "err", ferr)
		} else if recent >= maxMarketingPerWeek() {
			next := time.Now().UTC().Add(24 * time.Hour)
			if rerr := sc.Store.RescheduleEmail(ctx, e.ID, next); rerr != nil {
				slog.Error("scheduler: frequency defer failed", "id", e.ID, "err", rerr)
			} else {
				slog.Info("scheduler: frequency cap hit, deferred", "user_id", e.UserID, "type", e.EmailType, "recent", recent, "next", next)
			}
			return false
		}
	}

	var templateData map[string]interface{}
	switch e.EmailType {
	case "leads_ready":
		templateData = map[string]interface{}{"UserName": user.Name, "OutreachURL": sc.FrontendURL + "/outreach"}
	case "welcome":
		templateData = map[string]interface{}{"UserName": user.Name, "DashboardURL": sc.FrontendURL + "/"}
	default:
		// Accept both cc_ (sequence types) and cc- (template names queued by
		// bulk-send, e.g. cc-outreach-pricing) so any cc email renders with the
		// standard self-contained data instead of being dropped as "unknown".
		if strings.HasPrefix(e.EmailType, "cc_") || strings.HasPrefix(e.EmailType, "cc-") {
			templateData = map[string]interface{}{
				"UserName":     user.Name,
				"DashboardURL": sc.FrontendURL + "/",
			}
			// Coupon emails that render a real code get a UNIQUE, single-use,
			// user-bound code whose 10h clock starts on first open. The static
			// DEFAULT_COUPON_CODE is only a fallback if minting fails.
			tmpl := emailTypeToTemplate(e.EmailType)
			if handlers.PerRecipientCouponTemplate(tmpl) {
				code, err := sc.Store.CreatePerRecipientCoupon(ctx, "JEREMY", user.ID, user.Email, tmpl, handlers.FounderCouponPercent())
				if err != nil {
					slog.Error("scheduler: per-recipient coupon mint failed, using fallback", "type", e.EmailType, "user_id", e.UserID, "err", err)
					code = couponFallbackCode()
				}
				templateData["CouponCode"] = code
			} else if e.EmailType == "cc_coupon_unlock" {
				// cc-coupon-unlock does not render a code, but keep the historical
				// injection so any variant template that references it still works.
				templateData["CouponCode"] = couponFallbackCode()
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
