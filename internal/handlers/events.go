package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/studojo/emailer-service/internal/email"
	"github.com/studojo/emailer-service/internal/store"
)

// EventHandler handles email events from RabbitMQ
type EventHandler struct {
	Store       *store.PostgresStore
	Sender      *email.Sender
	FrontendURL string
}

// NewEventHandler creates a new event handler
func NewEventHandler(store *store.PostgresStore, sender *email.Sender, frontendURL string) *EventHandler {
	return &EventHandler{
		Store:       store,
		Sender:      sender,
		FrontendURL: frontendURL,
	}
}

// UserSignupEvent represents a user signup event
type UserSignupEvent struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
	Name   string `json:"name"`
}

// ResumeOptimizedEvent represents a resume optimization complete event
type ResumeOptimizedEvent struct {
	UserID            string `json:"user_id"`
	JobID             string `json:"job_id"`
	ResumeName        string `json:"resume_name"`
	ImprovementsSummary string `json:"improvements_summary"`
}

// InternshipAppliedEvent represents an internship application event
type InternshipAppliedEvent struct {
	UserID         string `json:"user_id"`
	InternshipID   string `json:"internship_id"`
	InternshipTitle string `json:"internship_title"`
	CompanyName    string `json:"company_name"`
	ResumeID       string `json:"resume_id"`
	Timestamp      string `json:"timestamp"`
}

// HandleUserSignup handles user signup events
func (h *EventHandler) HandleUserSignup(ctx context.Context, event *UserSignupEvent) error {
	// Check preferences - welcome email is a product email
	prefs, err := h.Store.GetEmailPreferences(ctx, event.UserID)
	if err != nil {
		return err
	}

	if !prefs.ProductEmails {
		slog.Info("skipping welcome email - product emails disabled", "user_id", event.UserID)
		return nil
	}

	// Send welcome email
	err = h.Sender.SendTemplateEmail(ctx, event.Email, "welcome", map[string]interface{}{
		"UserName":     event.Name,
		"DashboardURL": h.FrontendURL + "/",
	})
	if err != nil {
		slog.Error("failed to send welcome email", "error", err, "user_id", event.UserID)
		return err
	}

	slog.Info("welcome email sent", "user_id", event.UserID, "email", event.Email)

	// Schedule nurture sequence
	now := time.Now().UTC()
	nurture := []struct {
		emailType string
		delay     time.Duration
	}{
		{"nurture_day3", 3 * 24 * time.Hour},
		{"nurture_day7", 7 * 24 * time.Hour},
		{"nurture_day14", 14 * 24 * time.Hour},
		{"nurture_day30", 30 * 24 * time.Hour},
	}
	for _, n := range nurture {
		if err := h.Store.CreateScheduledEmail(ctx, event.UserID, n.emailType, now.Add(n.delay)); err != nil {
			slog.Error("failed to schedule nurture email", "type", n.emailType, "user_id", event.UserID, "error", err)
		}
	}

	return nil
}

// HandleResumeOptimized handles resume optimization complete events
func (h *EventHandler) HandleResumeOptimized(ctx context.Context, event *ResumeOptimizedEvent) error {
	// Check preferences
	prefs, err := h.Store.GetEmailPreferences(ctx, event.UserID)
	if err != nil {
		return err
	}

	if !prefs.ResumeEmails {
		slog.Info("skipping resume optimized email - resume emails disabled", "user_id", event.UserID)
		return nil
	}

	// Get user email
	user, err := h.Store.GetUserByID(ctx, event.UserID)
	if err != nil || user == nil {
		return err
	}

	// Send resume optimized email
	err = h.Sender.SendTemplateEmail(ctx, user.Email, "resume-optimized", map[string]interface{}{
		"UserName":            user.Name,
		"ResumeName":          event.ResumeName,
		"ImprovementsSummary": event.ImprovementsSummary,
		"ViewResumeURL":       h.FrontendURL + "/resumes",
	})
	if err != nil {
		slog.Error("failed to send resume optimized email", "error", err, "user_id", event.UserID)
		return err
	}

	slog.Info("resume optimized email sent", "user_id", event.UserID, "job_id", event.JobID)
	return nil
}

// HandleInternshipApplied handles internship application events
func (h *EventHandler) HandleInternshipApplied(ctx context.Context, event *InternshipAppliedEvent) error {
	// Check preferences
	prefs, err := h.Store.GetEmailPreferences(ctx, event.UserID)
	if err != nil {
		return err
	}

	if !prefs.InternshipEmails {
		slog.Info("skipping internship applied email - internship emails disabled", "user_id", event.UserID)
		return nil
	}

	// Get user email
	user, err := h.Store.GetUserByID(ctx, event.UserID)
	if err != nil || user == nil {
		return err
	}

	// Send internship application confirmation email
	err = h.Sender.SendTemplateEmail(ctx, user.Email, "internship-applied", map[string]interface{}{
		"UserName":           user.Name,
		"InternshipTitle":     event.InternshipTitle,
		"CompanyName":         event.CompanyName,
		"ResumeID":            event.ResumeID,
		"Timestamp":           event.Timestamp,
		"ViewApplicationURL":  h.FrontendURL + "/my-applications",
	})
	if err != nil {
		slog.Error("failed to send internship applied email", "error", err, "user_id", event.UserID)
		return err
	}

	slog.Info("internship applied email sent", "user_id", event.UserID, "internship_id", event.InternshipID)
	return nil
}

// ProcessEvent processes an event based on routing key
func (h *EventHandler) ProcessEvent(ctx context.Context, routingKey string, body []byte) error {
	switch routingKey {
	case "event.user.signup":
		var event UserSignupEvent
		if err := json.Unmarshal(body, &event); err != nil {
			return err
		}
		return h.HandleUserSignup(ctx, &event)

	case "event.resume.optimized":
		var event ResumeOptimizedEvent
		if err := json.Unmarshal(body, &event); err != nil {
			return err
		}
		return h.HandleResumeOptimized(ctx, &event)

	case "event.internship.applied":
		var event InternshipAppliedEvent
		if err := json.Unmarshal(body, &event); err != nil {
			return err
		}
		return h.HandleInternshipApplied(ctx, &event)

	default:
		slog.Warn("unknown event type", "routing_key", routingKey)
		return nil
	}
}

