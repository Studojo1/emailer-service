package scheduler

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/studojo/emailer-service/internal/email"
	"github.com/studojo/emailer-service/internal/store"
)

// Scheduler polls for due scheduled emails and sends them
type Scheduler struct {
	Store            *store.PostgresStore
	Sender           *email.Sender
	FrontendURL      string
	rateLimitUntil   time.Time // circuit breaker: stop sending until quota resets
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

// runCatchup finds users who missed emails because the service wasn't running
// when they signed up or completed the quiz, and queues them now.
// Welcome emails are NOT caught up here — they are already scheduled for all
// users via the initial backfill and sent exclusively via the signup handler
// going forward.
func (sc *Scheduler) runCatchup(ctx context.Context) {
	sc.catchupSegmentation(ctx)
	sc.catchupNurture(ctx)
}

// catchupNurture queues missing nurture emails for users who signed up before
// the emailer service was running and never had their sequence scheduled.
// Each nurture type is offset by one day so a user who missed all four never
// receives more than one nurture email per day.
func (sc *Scheduler) catchupNurture(ctx context.Context) {
	type nurtureSpec struct {
		emailType  string
		minDays    int
		baseOffset time.Duration // delay from now before the first send of this type
	}
	specs := []nurtureSpec{
		{"nurture_day3", 3, 0},
		{"nurture_day7", 7, 24 * time.Hour},
		{"nurture_day14", 14, 48 * time.Hour},
		{"nurture_day30", 30, 72 * time.Hour},
	}

	now := time.Now().UTC()
	for _, spec := range specs {
		users, err := sc.Store.ListUsersWithoutEmail(ctx, spec.emailType, spec.minDays*24*60)
		if err != nil {
			slog.Error("catchup nurture: list failed", "type", spec.emailType, "error", err)
			continue
		}
		queued := 0
		for i, u := range users {
			// 1s apart so scheduled_at is unique per user; baseOffset separates types by a day
			sendAt := now.Add(spec.baseOffset + time.Duration(i)*time.Second)
			if err := sc.Store.CreateScheduledEmail(ctx, u.ID, spec.emailType, sendAt); err != nil {
				slog.Error("catchup nurture: schedule failed", "type", spec.emailType, "user_id", u.ID, "error", err)
				continue
			}
			queued++
		}
		if queued > 0 {
			slog.Info("catchup nurture: queued missed emails", "type", spec.emailType, "count", queued)
		}
	}
}

// catchupSegmentation queues a segmentation email for any user who completed
// the quiz (has target_roles set) but never received a segmentation email.
func (sc *Scheduler) catchupSegmentation(ctx context.Context) {
	users, err := sc.Store.ListQuizCompletedWithoutEmail(ctx, "funnel-segmentation-v1")
	if err != nil {
		slog.Error("catchup: failed to list quiz-completed users without segmentation email", "error", err)
		return
	}
	queued := 0
	now := time.Now().UTC()
	for i, u := range users {
		delay := time.Duration(i) * 10 * time.Second
		if i > 0 && i%20 == 0 {
			delay += time.Duration(i/20) * 120 * time.Second
		}
		if err := sc.Store.CreateScheduledEmail(ctx, u.ID, "funnel-segmentation-v1", now.Add(delay)); err != nil {
			slog.Error("catchup: failed to queue segmentation", "user_id", u.ID, "error", err)
			continue
		}
		queued++
	}
	if queued > 0 {
		slog.Info("catchup: queued missed segmentation emails", "count", queued)
	}
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
	case "nurture_day3":
		templateData = map[string]interface{}{"UserName": user.Name, "InternshipURL": sc.FrontendURL + "/outreach"}
	case "nurture_day7":
		templateData = map[string]interface{}{"UserName": user.Name, "OutreachURL": sc.FrontendURL + "/outreach"}
	case "nurture_day14":
		templateData = map[string]interface{}{"UserName": user.Name, "OutreachURL": sc.FrontendURL + "/outreach"}
	case "nurture_day30":
		templateData = map[string]interface{}{"UserName": user.Name, "DashboardURL": sc.FrontendURL + "/"}
	default:
		if strings.HasPrefix(e.EmailType, "funnel-") {
			templateData = map[string]interface{}{"UserName": user.Name}
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
	return false
}

func emailTypeToTemplate(emailType string) string {
	switch emailType {
	case "nurture_day3":
		return "nurture-day3"
	case "nurture_day7":
		return "nurture-day7"
	case "nurture_day14":
		return "nurture-day14"
	case "nurture_day30":
		return "nurture-day30"
	case "leads_ready":
		return "leads-ready"
	default:
		// funnel-* types already have the right hyphenated name
		return emailType
	}
}
