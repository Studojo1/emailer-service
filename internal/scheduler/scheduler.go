package scheduler

import (
	"context"
	"log/slog"
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
	defer ticker.Stop()

	// Process immediately on start, then on each tick
	sc.processDue(ctx)
	for {
		select {
		case <-ticker.C:
			sc.processDue(ctx)
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
	for _, e := range emails {
		sc.send(ctx, e)
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
		// Mark sent so we don't retry
		_ = sc.Store.MarkScheduledEmailSent(ctx, e.ID)
		return
	}

	// Build template data
	var templateData map[string]interface{}
	switch e.EmailType {
	case "leads_ready":
		templateData = map[string]interface{}{
			"UserName":     user.Name,
			"OutreachURL":  sc.FrontendURL + "/outreach",
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
		slog.Warn("scheduler: unknown email type, skipping", "type", e.EmailType)
		_ = sc.Store.MarkScheduledEmailSent(ctx, e.ID) // don't retry unknowns
		return
	}

	// Map email_type to template name (underscore → hyphen)
	templateName := emailTypeToTemplate(e.EmailType)

	if err := sc.Sender.SendTemplateEmail(ctx, user.Email, templateName, templateData); err != nil {
		slog.Error("scheduler: failed to send email", "error", err, "user_id", e.UserID, "type", e.EmailType)
		return
	}

	if err := sc.Store.MarkScheduledEmailSent(ctx, e.ID); err != nil {
		slog.Error("scheduler: failed to mark sent", "error", err, "id", e.ID)
	}

	slog.Info("scheduler: nurture email sent", "user_id", e.UserID, "type", e.EmailType, "email", user.Email)
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
		return emailType
	}
}
