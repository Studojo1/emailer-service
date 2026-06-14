package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strconv"
	"strings"
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
	"event.cc.welcome_new_user": "cc-welcome-new-user",
	"event.cc.outreach_used":    "cc-outreach-push1",
	"event.cc.outreach_coupon":  "cc-outreach-coupon",
	// One-shot abandoned-cart blast: GOAT10 (10% off). Dedicated template so the
	// shared cc-outreach-coupon (automated flow, different code) is untouched.
	"event.cc.cart_goat": "cc-cart-goat",
	// Career Coach
	"event.cc.welcome":           "cc-welcome",
	"event.cc.dna_ready":         "cc-dna-ready",
	"event.cc.roadmap_delivered": "cc-roadmap-delivered",
	"event.cc.coupon_unlock":     "cc-coupon-unlock",
	// Resume Maker
	"event.cc.resume_strong": "cc-rm-strong-1",
	"event.cc.resume_weak":   "cc-rm-weak-1",
	// Internship Dojo
	"event.cc.id_two_tools": "cc-id-two-tools",
	// Old / dormant user keys are handled by ccSpreadStarters (see HandleCCEmail),
	// not here, so they are scheduled with a per-day spread instead of sent now.
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

// CCGateOutreachNotUsed is kept for backwards-compat with any in-flight rows
// scheduled under the old single-gate name; it maps to the outreach gate below.
const CCGateOutreachNotUsed = "cc_gate_outreach_notused"

// OutreachWelcomeType is the welcome template whose open/click counts as engagement
// for the outreach not-used gate (exported for the scheduler's legacy path).
const OutreachWelcomeType = "cc-welcome-new-user"

// ccGate describes one engagement-gated chase: after WelcomeType is sent, a gate
// row (GateType) is scheduled +7h. When it comes due the scheduler enrols Chase
// ONLY if the user has not opened/clicked the welcome and has not used the tool.
// Each chase email also re-checks engagement (by ChasePrefix) before sending.
type ccGate struct {
	GateType    string       // email_type of the placeholder gate row (no email sent)
	WelcomeType string       // template whose open/click counts as engagement
	ChasePrefix string       // prefix of the chase emails (for cancel/re-check)
	Chase       []ccSequence // chase steps, delays measured from the gate firing
}

// ccGates: every engagement-gated flow. The starter routing key triggers the
// welcome instantly and schedules the gate; the gate enrols the chase if no
// engagement. Add a flow here to make it gated — no scheduler change needed.
var ccGates = map[string]ccGate{
	// Outreach Dojo — not used
	"event.cc.welcome_new_user": {
		GateType:    "cc_gate_outreach_notused",
		WelcomeType: "cc-welcome-new-user",
		ChasePrefix: "cc_outreach_nudge",
		Chase: []ccSequence{
			{"cc_outreach_nudge_d1", 0},
			{"cc_outreach_nudge_d2", 24 * hour},
			{"cc_outreach_nudge_d3", 56 * hour},
			{"cc_outreach_nudge_d4", 89 * hour},
		},
	},
	// Career Coach — not started
	"event.cc.welcome": {
		GateType:    "cc_gate_coach_notstarted",
		WelcomeType: "cc-welcome",
		ChasePrefix: "cc_nudge",
		Chase: []ccSequence{
			{"cc_nudge_1", 0},
			{"cc_nudge_2", 24 * hour},
			{"cc_nudge_3", 48 * hour},
		},
	},
}

// gateByType lets the scheduler resolve a due gate row back to its config.
var gateByType = func() map[string]ccGate {
	m := map[string]ccGate{}
	for _, g := range ccGates {
		m[g.GateType] = g
	}
	return m
}()

// OutreachNotUsedNudges kept exported for the scheduler's legacy gate path.
var OutreachNotUsedNudges = ccGates["event.cc.welcome_new_user"].Chase

// GateByType is the exported resolver used by the scheduler.
func GateByType(t string) (gateType, welcomeType, chasePrefix string, chase []ccSequence, ok bool) {
	g, found := gateByType[t]
	if !found {
		return "", "", "", nil, false
	}
	return g.GateType, g.WelcomeType, g.ChasePrefix, g.Chase, true
}

// ScheduleCCChase enrols a gate's chase chain (exported for the scheduler).
func ScheduleCCChase(ctx context.Context, s *store.PostgresStore, userID string, after time.Time, chase []ccSequence) {
	ScheduleCCSequence(ctx, s, userID, after, chase)
}

// ChaseFor resolves a chase email_type to its gate's ChasePrefix + WelcomeType,
// so the scheduler can re-check engagement before each chase send. Matches by the
// gate's ChasePrefix.
func ChaseFor(emailType string) (prefix, welcomeType string, ok bool) {
	for _, g := range ccGates {
		if g.ChasePrefix != "" && strings.HasPrefix(emailType, g.ChasePrefix) {
			return g.ChasePrefix, g.WelcomeType, true
		}
	}
	return "", "", false
}

// ccSequenceStarters maps a sequence-starting routing key to the follow-up steps
// scheduled after its instant email is sent. Delays come straight from the flow spec.
var ccSequenceStarters = map[string][]ccSequence{
	// Outreach NOT used is engagement-gated: event.cc.welcome_new_user schedules a
	// +7h gate (cc_gate_outreach_notused) instead of the nudges directly. The gate
	// enrols OutreachNotUsedNudges only if the user hasn't engaged. See HandleCCEmail
	// and the scheduler's gate handling.

	// Outreach USED (high intent): fires when the student uses the outreach tool.
	// The instant email is push1 (cc-outreach-push1); these are the follow-ups.
	// Entering this also cancels the not-used nudge chain (see HandleCCEmail).
	"event.cc.outreach_used": {
		{"cc_outreach_push2", 24 * hour},
		{"cc_outreach_push3", 50 * hour},
		{"cc_outreach_convert1", 60 * hour},
		{"cc_outreach_convert2", 75 * hour},
	},
	// Career Coach NOT started is now engagement-gated (see ccGates) — the chase
	// is enrolled by the +7h gate only if the user hasn't engaged, so it is no
	// longer scheduled up front here.

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
	// Old-user stages are handled separately by ccSpreadStarters (see below) so
	// they cascade across days under a per-day cap.
}

// ccDeferredStarters maps routing keys that send NOTHING instantly and instead
// schedule their whole sequence (including the first email) with a delay. Used
// for abandoned-checkout: the student reached the payment page, but we wait so a
// student who pays within the window never gets the "you were right there"
// email. Payment success cancels all pending cc_ rows, which drains these.
var ccDeferredStarters = map[string][]ccSequence{
	// Reached the outreach payment page but did not pay yet.
	"event.cc.outreach_payment_page": {
		{"cc_outreach_payment_page", 2 * hour}, // abandoned-checkout nudge after a 2h buffer
		{"cc_outreach_coupon", 6 * hour},        // founder coupon a few hours later
	},
}

// ccOldUserPerDay caps how many old-user re-engagement emails are scheduled to
// land on any single day, so a large dormant batch cascades over days and never
// consumes the daily provider quota that new-user mail needs. Configurable via
// EMAIL_OLDUSER_PER_DAY (default 100). New-user mail is uncapped (top priority).
func ccOldUserPerDay() int {
	if v := os.Getenv("EMAIL_OLDUSER_PER_DAY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 100
}

// ccSpreadStarters maps old-user routing keys to their full sequence (first email
// included). Unlike instant starters, the first email is placed at the next
// available "spread slot" (respecting the per-day cap) and the followups chain
// off that slot, so the whole dormant batch cascades across days by intent: the
// queue is also priority-ordered (s1 before s2 before s3) at send time.
var ccSpreadStarters = map[string][]ccSequence{
	"event.cc.old_s1": {
		{"cc_old_s1_1", 0},
		{"cc_old_s1_2", 5 * day},
		{"cc_old_s1_3", 9 * day},
	},
	"event.cc.old_s2": {
		{"cc_old_s2_1", 0},
		{"cc_old_s2_2", 6 * day},
		{"cc_old_s2_3", 9 * day},
	},
	"event.cc.old_s3": {
		{"cc_old_s3_1", 0},
		{"cc_old_s3_2", 7 * day},
		{"cc_old_s3_3", 14 * day},
	},
}

// HandleCCEmail handles all event.cc.* events. Instant-send keys send their email
// now and schedule any follow-ups; deferred-start keys send nothing now and
// schedule the whole sequence with a delay; spread-start (old-user) keys place
// the first email at the next per-day spread slot so big dormant batches cascade.
func (h *EventHandler) HandleCCEmail(ctx context.Context, routingKey string, event *CCEmailEvent) error {
	// Spread starters (old-user re-engagement): cascade across days under a cap,
	// using only the room new-user mail leaves free.
	if steps, ok := ccSpreadStarters[routingKey]; ok {
		if event.UserID == "" {
			slog.Warn("cc spread email: missing user_id, cannot schedule", "routing_key", routingKey)
			return nil
		}
		start, err := h.Store.NextSpreadSlot(ctx, "cc_old_", ccOldUserPerDay(), time.Now().UTC())
		if err != nil {
			slog.Error("cc spread: failed to find slot, scheduling now", "error", err)
			start = time.Now().UTC()
		}
		ScheduleCCSequence(ctx, h.Store, event.UserID, start, steps)
		slog.Info("cc old-user sequence scheduled (spread)", "routing_key", routingKey, "user_id", event.UserID, "first_at", start)
		return nil
	}

	// Deferred starters: schedule the sequence, send nothing now.
	if steps, ok := ccDeferredStarters[routingKey]; ok {
		if event.UserID == "" {
			slog.Warn("cc deferred email: missing user_id, cannot schedule", "routing_key", routingKey)
			return nil
		}
		ScheduleCCSequence(ctx, h.Store, event.UserID, time.Now().UTC(), steps)
		slog.Info("cc deferred sequence scheduled", "routing_key", routingKey, "user_id", event.UserID)
		return nil
	}
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
		// Exit rule: using the outreach tool cancels both the pending engagement
		// gate AND any already-enrolled not-used nudges, so a high-intent user
		// never gets "did you try it?" emails.
		if routingKey == "event.cc.outreach_used" {
			if n, err := h.Store.CancelPendingEmailsByPrefix(ctx, event.UserID, "cc_outreach_nudge"); err != nil {
				slog.Error("cc email: failed to cancel not-used chain", "user_id", event.UserID, "error", err)
			} else if n > 0 {
				slog.Info("cc email: cancelled not-used chain on tool use", "user_id", event.UserID, "count", n)
			}
			if _, err := h.Store.CancelPendingEmailsByPrefix(ctx, event.UserID, "cc_gate_outreach_notused"); err != nil {
				slog.Error("cc email: failed to cancel engagement gate", "user_id", event.UserID, "error", err)
			}
		}
		// Engagement-gated chase: the welcome was just sent. For any gated flow,
		// schedule a single gate check (+7h) instead of the chase. When the gate
		// comes due the scheduler enrols the chase only if the user has NOT
		// opened/clicked the welcome (and, for outreach, not used the tool).
		// Non-gated flows schedule their follow-up steps immediately as before.
		if gate, gated := ccGates[routingKey]; gated {
			if err := h.Store.CreateScheduledEmail(ctx, event.UserID, gate.GateType, time.Now().UTC().Add(7*hour)); err != nil {
				slog.Error("cc email: failed to schedule engagement gate", "user_id", event.UserID, "gate", gate.GateType, "error", err)
			}
		} else if steps, ok := ccSequenceStarters[routingKey]; ok {
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

// HandleCCPaid cancels all pending cc marketing sequences for a user who just
// paid, without sending anything. Used by the Outreach checkout, which confirms
// payment via its own backend (not event.payment.success), so this is the signal
// that drains the abandoned-checkout and nudge sequences for converters.
func (h *EventHandler) HandleCCPaid(ctx context.Context, event *CCEmailEvent) error {
	if event.UserID == "" {
		return nil
	}
	n, err := h.Store.CancelPendingCCMarketingEmails(ctx, event.UserID)
	if err != nil {
		slog.Error("cc paid: failed to cancel cc marketing emails", "user_id", event.UserID, "error", err)
		return err
	}
	if n > 0 {
		slog.Info("cc paid: cancelled pending cc marketing emails", "user_id", event.UserID, "count", n)
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

	case "event.cc.paid":
		// Cancel-only signal from a flow whose payment is confirmed out-of-band
		// (e.g. the Outreach checkout). Drains pending cc marketing sequences.
		var event CCEmailEvent
		if err := json.Unmarshal(body, &event); err != nil {
			return err
		}
		return h.HandleCCPaid(ctx, &event)

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
