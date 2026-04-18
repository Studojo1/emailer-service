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
	Store       *store.PostgresStore
	Sender      *email.Sender
	FrontendURL string
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
	emails, err := sc.Store.GetDueScheduledEmails(ctx)
	if err != nil {
		slog.Error("scheduler: failed to fetch due emails", "error", err)
		return
	}
	for i, e := range emails {
		if i > 0 {
			time.Sleep(3 * time.Second) // ~20 emails/min, well under ACS rate limit
		}
		sc.send(ctx, e)
	}
}

// runCatchup finds users who should have gotten welcome or segmentation emails
// but didn't (e.g. RabbitMQ event dropped, pod restart during signup, etc.)
// and queues them for sending. Safe to run repeatedly — dedup is built in.
func (sc *Scheduler) runCatchup(ctx context.Context) {
	sc.catchupWelcome(ctx)
	sc.catchupSegmentation(ctx)
}

// catchupWelcome queues a welcome email for any user who signed up more than
// 5 minutes ago but has never received one and isn't already queued.
func (sc *Scheduler) catchupWelcome(ctx context.Context) {
	users, err := sc.Store.ListUsersWithoutEmail(ctx, "welcome", 5)
	if err != nil {
		slog.Error("catchup: failed to list users without welcome email", "error", err)
		return
	}
	queued := 0
	now := time.Now().UTC()
	for i, u := range users {
		delay := time.Duration(i) * 10 * time.Second
		if i > 0 && i%20 == 0 {
			delay += time.Duration(i/20) * 120 * time.Second
		}
		if err := sc.Store.CreateScheduledEmail(ctx, u.ID, "welcome", now.Add(delay)); err != nil {
			slog.Error("catchup: failed to queue welcome", "user_id", u.ID, "error", err)
			continue
		}
		queued++
	}
	if queued > 0 {
		slog.Info("catchup: queued missed welcome emails", "count", queued)
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

func (sc *Scheduler) send(ctx context.Context, e store.ScheduledEmail) {
	// Get user
	user, err := sc.Store.GetUserByID(ctx, e.UserID)
	if err != nil || user == nil {
		slog.Error("scheduler: user not found", "user_id", e.UserID, "email_type", e.EmailType)
		return
	}

	// Check preferences
	prefs, err := sc.Store.GetEmailPreferences(ctx, e.UserID)
	if err != nil {
		slog.Error("scheduler: failed to get preferences", "user_id", e.UserID)
		return
	}
	if !prefs.ProductEmails {
		slog.Info("scheduler: skipping email, product emails disabled", "user_id", e.UserID, "type", e.EmailType)
		_ = sc.Store.MarkScheduledEmailSent(ctx, e.ID)
		return
	}

	// Dedup: if this email type was already sent (e.g. event handler sent it,
	// or a previous scheduler run sent it but MarkScheduledEmailSent failed),
	// mark the row done and skip to avoid a duplicate send.
	already, err := sc.Store.HasReceivedEmail(ctx, e.UserID, e.EmailType)
	if err != nil {
		slog.Error("scheduler: dedup check failed", "user_id", e.UserID, "type", e.EmailType, "error", err)
	} else if already {
		slog.Info("scheduler: already sent, marking done", "user_id", e.UserID, "type", e.EmailType)
		_ = sc.Store.MarkScheduledEmailSent(ctx, e.ID)
		return
	}

	// Build template data
	var templateData map[string]interface{}
	switch e.EmailType {
	case "leads_ready":
		templateData = map[string]interface{}{
			"UserName":    user.Name,
			"OutreachURL": sc.FrontendURL + "/outreach",
		}
	case "welcome":
		templateData = map[string]interface{}{
			"UserName":     user.Name,
			"DashboardURL": sc.FrontendURL + "/",
		}
	case "nurture_day3":
		templateData = map[string]interface{}{
			"UserName":      user.Name,
			"InternshipURL": sc.FrontendURL + "/outreach",
		}
	case "nurture_day7":
		templateData = map[string]interface{}{
			"UserName":    user.Name,
			"OutreachURL": sc.FrontendURL + "/outreach",
		}
	case "nurture_day14":
		templateData = map[string]interface{}{
			"UserName":    user.Name,
			"OutreachURL": sc.FrontendURL + "/outreach",
		}
	case "nurture_day30":
		templateData = map[string]interface{}{
			"UserName":     user.Name,
			"DashboardURL": sc.FrontendURL + "/",
		}
	default:
		// Handle all funnel-* types generically
		if strings.HasPrefix(e.EmailType, "funnel-") {
			templateData = map[string]interface{}{
				"UserName": user.Name,
			}
		} else {
			slog.Warn("scheduler: unknown email type, skipping", "type", e.EmailType)
			_ = sc.Store.MarkScheduledEmailSent(ctx, e.ID)
			return
		}
	}

	templateName := emailTypeToTemplate(e.EmailType)

	if err := sc.Sender.SendTemplateEmail(ctx, user.Email, templateName, templateData); err != nil {
		slog.Error("scheduler: failed to send email", "error", err, "user_id", e.UserID, "type", e.EmailType)
		return
	}

	if err := sc.Store.MarkScheduledEmailSent(ctx, e.ID); err != nil {
		slog.Error("scheduler: failed to mark sent", "error", err, "id", e.ID)
	}

	slog.Info("scheduler: email sent", "user_id", e.UserID, "type", e.EmailType, "email", user.Email)
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
