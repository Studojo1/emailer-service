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

// FunnelEmailEvent represents a manually triggered funnel email
type FunnelEmailEvent struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
	Name   string `json:"name"`
}

// PaymentEvent represents a successful payment
type PaymentEvent struct {
	UserID  string `json:"user_id"`
	Email   string `json:"email"`
	Name    string `json:"name"`
	PlanName string `json:"plan_name"`
	Amount  string `json:"amount"`
	OrderID string `json:"order_id"`
}

// funnelRoutingKeyToTemplate maps event.funnel.* routing keys to template names
var funnelRoutingKeyToTemplate = map[string]string{
	"event.funnel.welcome_new":          "funnel-welcome-new",
	"event.funnel.welcome_existing":     "funnel-welcome-existing",
	"event.funnel.followup_v1":          "funnel-followup-v1",
	"event.funnel.followup_v2":          "funnel-followup-v2",
	"event.funnel.followup_v3":          "funnel-followup-v3",
	"event.funnel.segmentation_v1":      "funnel-segmentation-v1",
	"event.funnel.segmentation_v2":      "funnel-segmentation-v2",
	"event.funnel.exploration_v1":       "funnel-exploration-v1",
	"event.funnel.exploration_v2":       "funnel-exploration-v2",
	"event.funnel.congratulations":      "funnel-congratulations",
	"event.funnel.comparison":           "funnel-comparison",
	"event.funnel.pitching_v1":          "funnel-pitching-v1",
	"event.funnel.pitching_v2":          "funnel-pitching-v2",
	"event.funnel.pitching_v3":          "funnel-pitching-v3",
	"event.funnel.honest_question_v1":   "funnel-honest-question-v1",
	"event.funnel.honest_question_v2":   "funnel-honest-question-v2",
	"event.funnel.honest_question_v3":   "funnel-honest-question-v3",
	"event.funnel.onboarding":           "funnel-onboarding",
	"event.funnel.recognition_v1":       "funnel-recognition-v1",
	"event.funnel.recognition_v2":       "funnel-recognition-v2",
	"event.funnel.recognition_v3":       "funnel-recognition-v3",
	"event.funnel.recognition_v4":       "funnel-recognition-v4",
	"event.funnel.testimonial":          "funnel-testimonial",
	"event.funnel.pricing":              "funnel-pricing",
	"event.funnel.case_study":           "funnel-case-study",
	"event.funnel.walkthrough":          "funnel-walkthrough",
	"event.funnel.educational":          "funnel-educational",
	"event.funnel.winback":              "funnel-winback",
	"event.funnel.signup_thankyou":      "signup-thankyou",
	"event.funnel.signup_followup":      "signup-followup",
	"event.funnel.signup_welcome_v1":    "signup-welcome-v1",
	"event.funnel.signup_welcome_v2":    "signup-welcome-v2",
	"event.funnel.signup_welcome_v3":    "signup-welcome-v3",
	"event.funnel.signup_welcome_v4":    "signup-welcome-v4",
	"event.funnel.signup_welcome_v5":    "signup-welcome-v5",
}

// HandleFunnelEmail handles all event.funnel.* events
func (h *EventHandler) HandleFunnelEmail(ctx context.Context, routingKey string, event *FunnelEmailEvent) error {
	templateName, ok := funnelRoutingKeyToTemplate[routingKey]
	if !ok {
		slog.Warn("unknown funnel routing key", "routing_key", routingKey)
		return nil
	}

	// Dedup: skip if this user has already received this template.
	// Record uses templateName (not routingKey) so the scheduler catchup
	// dedup query also finds it correctly.
	if event.UserID != "" {
		already, err := h.Store.HasReceivedEmail(ctx, event.UserID, templateName)
		if err != nil {
			slog.Error("funnel email: dedup check failed", "user_id", event.UserID, "template", templateName, "error", err)
		} else if already {
			slog.Info("funnel email: already sent, skipping", "user_id", event.UserID, "template", templateName)
			return nil
		}
	}

	// Resolve email/name from user ID if not provided directly
	recipientEmail := event.Email
	recipientName := event.Name
	if recipientEmail == "" && event.UserID != "" {
		user, err := h.Store.GetUserByID(ctx, event.UserID)
		if err != nil || user == nil {
			slog.Error("funnel email: failed to get user", "user_id", event.UserID, "error", err)
			return err
		}
		recipientEmail = user.Email
		recipientName = user.Name
	}
	if recipientName == "" {
		recipientName = "there"
	}

	ctx = context.WithValue(ctx, email.UserIDKey, event.UserID)
	ctx = context.WithValue(ctx, email.UserNameKey, recipientName)
	if err := h.Sender.SendTemplateEmail(ctx, recipientEmail, templateName, map[string]interface{}{
		"UserName": recipientName,
	}); err != nil {
		slog.Error("funnel email: failed to send", "routing_key", routingKey, "template", templateName, "error", err)
		return err
	}

	slog.Info("funnel email sent", "routing_key", routingKey, "template", templateName, "email", recipientEmail)

	if event.UserID != "" {
		// Record with templateName so HasReceivedEmail and the catchup scheduler
		// both find it with the same key they query.
		if err := h.Store.RecordSentEmail(ctx, event.UserID, templateName); err != nil {
			slog.Error("funnel email: failed to record", "template", templateName, "error", err)
		}
	}

	return nil
}

// ContactFormEvent represents a contact form submission
type ContactFormEvent struct {
	Name    string `json:"name"`
	Email   string `json:"email"`
	Subject string `json:"subject"`
	Message string `json:"message"`
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

	// Dedup: skip if already sent
	already, err := h.Store.HasReceivedEmail(ctx, event.UserID, "welcome")
	if err != nil {
		slog.Error("welcome email: dedup check failed", "user_id", event.UserID, "error", err)
	} else if already {
		slog.Info("welcome email: already sent, skipping", "user_id", event.UserID)
		return nil
	}

	// Send welcome email
	ctx = context.WithValue(ctx, email.UserIDKey, event.UserID)
	ctx = context.WithValue(ctx, email.UserNameKey, event.Name)
	err = h.Sender.SendTemplateEmail(ctx, event.Email, "welcome", map[string]interface{}{
		"UserName":     event.Name,
		"DashboardURL": h.FrontendURL + "/",
	})
	if err != nil {
		slog.Error("failed to send welcome email", "error", err, "user_id", event.UserID)
		return err
	}

	slog.Info("welcome email sent", "user_id", event.UserID, "email", event.Email)

	// Record the sent welcome email for admin tracking
	if err := h.Store.RecordSentEmail(ctx, event.UserID, "welcome"); err != nil {
		slog.Error("failed to record welcome email", "error", err)
	}

	// Schedule nurture sequence — guard against duplicate scheduling on event replays
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
		exists, err := h.Store.HasScheduledOrReceivedEmail(ctx, event.UserID, n.emailType)
		if err != nil {
			slog.Error("nurture dedup check failed", "type", n.emailType, "user_id", event.UserID, "error", err)
			continue
		}
		if exists {
			continue
		}
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

	// Dedup: skip if already sent for this job
	emailKey := "resume-optimized-" + event.JobID
	already, err := h.Store.HasReceivedEmail(ctx, event.UserID, emailKey)
	if err != nil {
		slog.Error("resume optimized email: dedup check failed", "user_id", event.UserID, "job_id", event.JobID, "error", err)
	} else if already {
		slog.Info("resume optimized email: already sent, skipping", "user_id", event.UserID, "job_id", event.JobID)
		return nil
	}

	// Get user email
	user, err := h.Store.GetUserByID(ctx, event.UserID)
	if err != nil || user == nil {
		return err
	}

	// Send resume optimized email
	ctx = context.WithValue(ctx, email.UserIDKey, user.ID)
	ctx = context.WithValue(ctx, email.UserNameKey, user.Name)
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
	if err := h.Store.RecordSentEmail(ctx, event.UserID, emailKey); err != nil {
		slog.Error("failed to record resume optimized email", "error", err)
	}
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

	// Dedup: one confirmation email per internship application
	emailKey := "internship-applied-" + event.InternshipID
	already, err := h.Store.HasReceivedEmail(ctx, event.UserID, emailKey)
	if err != nil {
		slog.Error("internship applied email: dedup check failed", "user_id", event.UserID, "internship_id", event.InternshipID, "error", err)
	} else if already {
		slog.Info("internship applied email: already sent, skipping", "user_id", event.UserID, "internship_id", event.InternshipID)
		return nil
	}

	// Get user email
	user, err := h.Store.GetUserByID(ctx, event.UserID)
	if err != nil || user == nil {
		return err
	}

	// Send internship application confirmation email
	ctx = context.WithValue(ctx, email.UserIDKey, user.ID)
	ctx = context.WithValue(ctx, email.UserNameKey, user.Name)
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
	if err := h.Store.RecordSentEmail(ctx, event.UserID, emailKey); err != nil {
		slog.Error("failed to record internship applied email", "error", err)
	}
	return nil
}

// HandleContactForm handles contact form submission events
func (h *EventHandler) HandleContactForm(ctx context.Context, event *ContactFormEvent) error {
	adminEmail := "admin@studojo.com"

	err := h.Sender.SendTemplateEmail(ctx, adminEmail, "contact-form", map[string]interface{}{
		"Name":    event.Name,
		"Email":   event.Email,
		"Subject": event.Subject,
		"Message": event.Message,
	})
	if err != nil {
		slog.Error("failed to send contact form email", "error", err, "from", event.Email)
		return err
	}

	slog.Info("contact form email sent", "from", event.Email, "subject", event.Subject)
	return nil
}

// HandlePayment handles payment confirmation events
func (h *EventHandler) HandlePayment(ctx context.Context, event *PaymentEvent) error {
	// Dedup per order — one confirmation per OrderID
	emailKey := "payment-thankyou-" + event.OrderID
	if event.UserID != "" {
		already, err := h.Store.HasReceivedEmail(ctx, event.UserID, emailKey)
		if err != nil {
			slog.Error("payment email: dedup check failed", "order_id", event.OrderID, "error", err)
		} else if already {
			slog.Info("payment email: already sent, skipping", "order_id", event.OrderID)
			return nil
		}
	}

	recipientEmail := event.Email
	recipientName := event.Name
	if recipientEmail == "" && event.UserID != "" {
		user, err := h.Store.GetUserByID(ctx, event.UserID)
		if err != nil || user == nil {
			slog.Error("payment email: failed to get user", "user_id", event.UserID, "error", err)
			return err
		}
		recipientEmail = user.Email
		recipientName = user.Name
	}
	if recipientName == "" {
		recipientName = "there"
	}

	ctx = context.WithValue(ctx, email.UserIDKey, event.UserID)
	ctx = context.WithValue(ctx, email.UserNameKey, recipientName)
	err := h.Sender.SendTemplateEmail(ctx, recipientEmail, "payment-thankyou", map[string]interface{}{
		"UserName": recipientName,
		"PlanName": event.PlanName,
		"Amount":   event.Amount,
		"OrderID":  event.OrderID,
	})
	if err != nil {
		slog.Error("payment email: failed to send", "error", err, "email", recipientEmail)
		return err
	}
	slog.Info("payment confirmation email sent", "email", recipientEmail, "order_id", event.OrderID)

	if event.UserID != "" {
		if err := h.Store.RecordSentEmail(ctx, event.UserID, emailKey); err != nil {
			slog.Error("payment email: failed to record", "order_id", event.OrderID, "error", err)
		}
	}
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

	case "event.contact.form-submitted":
		var event ContactFormEvent
		if err := json.Unmarshal(body, &event); err != nil {
			return err
		}
		return h.HandleContactForm(ctx, &event)

	case "event.payment.success":
		var event PaymentEvent
		if err := json.Unmarshal(body, &event); err != nil {
			return err
		}
		return h.HandlePayment(ctx, &event)

	default:
		// Handle all event.funnel.* routing keys generically
		if _, ok := funnelRoutingKeyToTemplate[routingKey]; ok {
			var event FunnelEmailEvent
			if err := json.Unmarshal(body, &event); err != nil {
				return err
			}
			return h.HandleFunnelEmail(ctx, routingKey, &event)
		}
		slog.Warn("unknown event type", "routing_key", routingKey)
		return nil
	}
}

