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
	UserID              string `json:"user_id"`
	JobID               string `json:"job_id"`
	ResumeName          string `json:"resume_name"`
	ImprovementsSummary string `json:"improvements_summary"`
}

// CCEmailEvent represents a new-flow (career-coach / efficient flow) email
// trigger. CTAVariant (when set) selects the closing CTA block for old-user
// stage emails: "outreach" | "coach" | "two-tool". CouponCode is passed through
// for coupon emails.
type CCEmailEvent struct {
	UserID     string `json:"user_id"`
	Email      string `json:"email"`
	Name       string `json:"name"`
	CTAVariant string `json:"cta_variant,omitempty"`
	CouponCode string `json:"coupon_code,omitempty"`
}

// PaymentEvent represents a successful payment
type PaymentEvent struct {
	UserID   string `json:"user_id"`
	Email    string `json:"email"`
	Name     string `json:"name"`
	PlanName string `json:"plan_name"`
	Amount   string `json:"amount"`
	OrderID  string `json:"order_id"`
}

// ccRoutingKeyToTemplate maps event.cc.* routing keys to template names for the
// instant-send emails (the first email of a flow, or a standalone trigger). Each
// of these may also START a scheduled sequence; see ccSequenceStarters below.
var ccRoutingKeyToTemplate = map[string]string{
	// Outreach Dojo
	"event.cc.welcome_new_user":      "cc-welcome-new-user",
	"event.cc.outreach_payment_page": "cc-outreach-payment-page",
	// Career Coach
	"event.cc.welcome":           "cc-welcome",
	"event.cc.dna_ready":         "cc-dna-ready",
	"event.cc.roadmap_delivered": "cc-roadmap-delivered",
	// Resume Maker
	"event.cc.resume_strong": "cc-rm-strong-1",
	"event.cc.resume_weak":   "cc-rm-weak-1",
	// Internship Dojo
	"event.cc.id_two_tools": "cc-id-two-tools",
	// Old / dormant user (intent stage; deepest-tool CTA chosen via CTAVariant)
	"event.cc.old_s1": "cc-old-s1-1",
	"event.cc.old_s2": "cc-old-s2-1",
	"event.cc.old_s3": "cc-old-s3-1",
}

// ccSequence is one step of a scheduled cc sequence.
type ccSequence struct {
	emailType string
	delay     time.Duration
}

// ScheduleCCSequence queues a list of cc sequence steps into scheduled_emails with
// per-type dedup. Mirrors ScheduleFunnelSequence. Safe to call repeatedly.
func ScheduleCCSequence(ctx context.Context, s *store.PostgresStore, userID string, after time.Time, steps []ccSequence) {
	for _, step := range steps {
		exists, err := s.HasScheduledOrReceivedEmail(ctx, userID, step.emailType)
		if err != nil {
			slog.Error("cc sequence: dedup check failed", "type", step.emailType, "user_id", userID, "error", err)
			continue
		}
		if exists {
			continue
		}
		if err := s.CreateScheduledEmail(ctx, userID, step.emailType, after.Add(step.delay)); err != nil {
			slog.Error("cc sequence: schedule failed", "type", step.emailType, "user_id", userID, "error", err)
		}
	}
}

const hour = time.Hour
const day = 24 * time.Hour

// ccSequenceStarters maps a sequence-starting routing key to the follow-up steps
// scheduled after its instant email is sent. Delays come straight from the flow spec.
var ccSequenceStarters = map[string][]ccSequence{
	// Outreach NOT used: scheduled off the welcome (the welcome itself is event.cc.welcome_new_user)
	"event.cc.welcome_new_user": {
		{"cc_outreach_nudge_d1", 7 * hour},
		{"cc_outreach_nudge_d2", 31 * hour}, // 7h + 24h
		{"cc_outreach_nudge_d3", 63 * hour}, // + 32h
		{"cc_outreach_nudge_d4", 96 * hour}, // day 4
	},
	// Career Coach NOT started: off cc-welcome
	"event.cc.welcome": {
		{"cc_nudge_1", 8 * hour},
		{"cc_nudge_2", 32 * hour},
		{"cc_nudge_3", 56 * hour},
	},
	// Post-DNA sequence: off dna_ready
	"event.cc.dna_ready": {
		{"cc_dna_confirm_nudge", 2 * day},
		{"cc_checkin_1", 4 * day},
		{"cc_checkin_2", 7 * day},
		{"cc_checkin_3", 10 * day},
	},
	// Roadmap sequence: off roadmap_delivered
	"event.cc.roadmap_delivered": {
		{"cc_upskill_nudge", 7 * day},
		{"cc_coupon_unlock", 9 * day},
		{"cc_dormant", 11 * day},
		{"cc_to_outreach", 14 * day},
	},
	// Resume strong -> outreach lean
	"event.cc.resume_strong": {
		{"cc_rm_strong_2", 2 * day},
		{"cc_rm_strong_3", 3 * day},
	},
	// Resume weak -> coach lean
	"event.cc.resume_weak": {
		{"cc_rm_weak_2", 2 * day},
		{"cc_rm_weak_3", 3 * day},
	},
	// Internship two-tool offer -> re-engage if no click
	"event.cc.id_two_tools": {
		{"cc_id_reengage_1", 3 * day},
		{"cc_id_reengage_2", 7 * day},
	},
	// Old user stages (tool-neutral). CTA on the closing email is chosen at send
	// time from the user's stored deepest-tool signal.
	"event.cc.old_s1": {
		{"cc_old_s1_2", 5 * day},
		{"cc_old_s1_3", 9 * day},
	},
	"event.cc.old_s2": {
		{"cc_old_s2_2", 6 * day},
		{"cc_old_s2_3", 9 * day},
	},
	"event.cc.old_s3": {
		{"cc_old_s3_2", 7 * day},
		{"cc_old_s3_3", 14 * day},
	},
}

// HandleCCEmail handles all event.cc.* events: sends the instant email and, if the
// key starts a sequence, schedules the follow-up steps.
func (h *EventHandler) HandleCCEmail(ctx context.Context, routingKey string, event *CCEmailEvent) error {
	templateName, ok := ccRoutingKeyToTemplate[routingKey]
	if !ok {
		slog.Warn("unknown cc routing key", "routing_key", routingKey)
		return nil
	}

	// Dedup on the template name (matches what RecordSentEmail stores).
	if event.UserID != "" {
		already, err := h.Store.HasReceivedEmail(ctx, event.UserID, templateName)
		if err != nil {
			slog.Error("cc email: dedup check failed", "user_id", event.UserID, "template", templateName, "error", err)
		} else if already {
			slog.Info("cc email: already sent, skipping", "user_id", event.UserID, "template", templateName)
			return nil
		}
	}

	recipientEmail := event.Email
	recipientName := event.Name
	if recipientEmail == "" && event.UserID != "" {
		user, err := h.Store.GetUserByID(ctx, event.UserID)
		if err != nil || user == nil {
			slog.Error("cc email: failed to get user", "user_id", event.UserID, "error", err)
			return err
		}
		recipientEmail = user.Email
		recipientName = user.Name
	}
	if recipientName == "" {
		recipientName = "there"
	}

	data := map[string]interface{}{
		"UserName":     recipientName,
		"DashboardURL": h.FrontendURL + "/",
		"CouponCode":   event.CouponCode,
	}

	ctx = context.WithValue(ctx, email.UserIDKey, event.UserID)
	ctx = context.WithValue(ctx, email.UserNameKey, recipientName)
	if err := h.Sender.SendTemplateEmail(ctx, recipientEmail, templateName, data); err != nil {
		slog.Error("cc email: failed to send", "routing_key", routingKey, "template", templateName, "error", err)
		return err
	}
	slog.Info("cc email sent", "routing_key", routingKey, "template", templateName, "email", recipientEmail)

	if event.UserID != "" {
		if err := h.Store.RecordSentEmail(ctx, event.UserID, templateName); err != nil {
			slog.Error("cc email: failed to record", "template", templateName, "error", err)
		}
		if steps, ok := ccSequenceStarters[routingKey]; ok {
			ScheduleCCSequence(ctx, h.Store, event.UserID, time.Now().UTC(), steps)
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
	UserID          string `json:"user_id"`
	InternshipID    string `json:"internship_id"`
	InternshipTitle string `json:"internship_title"`
	CompanyName     string `json:"company_name"`
	ResumeID        string `json:"resume_id"`
	Timestamp       string `json:"timestamp"`
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

	// The old nurture sequence is retired. The new efficient flow's engagement
	// sequences are started by their own event.cc.* triggers (welcome_new_user,
	// welcome, dna_ready, etc.), not from this transactional account-welcome.

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
		"UserName":          user.Name,
		"InternshipTitle":   event.InternshipTitle,
		"CompanyName":       event.CompanyName,
		"ResumeID":          event.ResumeID,
		"Timestamp":         event.Timestamp,
		"ViewApplicationURL": h.FrontendURL + "/my-applications",
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
		// Cancel any pending cc marketing sequences — paid users don't need conversion nudges
		if n, err := h.Store.CancelPendingCCMarketingEmails(ctx, event.UserID); err != nil {
			slog.Error("payment: failed to cancel cc marketing emails", "user_id", event.UserID, "error", err)
		} else if n > 0 {
			slog.Info("payment: cancelled pending cc marketing emails", "user_id", event.UserID, "count", n)
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

	case "event.payment.success", "event.payment.completed":
		var event PaymentEvent
		if err := json.Unmarshal(body, &event); err != nil {
			return err
		}
		return h.HandlePayment(ctx, &event)

	default:
		// Handle all event.cc.* routing keys generically (the new efficient flow)
		if _, ok := ccRoutingKeyToTemplate[routingKey]; ok {
			var event CCEmailEvent
			if err := json.Unmarshal(body, &event); err != nil {
				return err
			}
			return h.HandleCCEmail(ctx, routingKey, &event)
		}
		slog.Warn("unknown event type", "routing_key", routingKey)
		return nil
	}
}
