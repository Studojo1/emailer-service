package handlers

import (
	"context"
	"encoding/json"
	"fmt"
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
	// Webinar — instant "registration confirmed" on signup
	"event.cc.webinar_registered": "cc-webinar-confirm",
	// Old / dormant user keys are handled by ccSpreadStarters (see HandleCCEmail),
	// not here, so they are scheduled with a per-day spread instead of sent now.
}

// perRecipientCouponTemplates are the templates that render a real coupon code
// ({{.CouponCode}}) and must therefore get a UNIQUE, single-use, user-bound code
// whose 10h expiry starts when the recipient opens the email. cc-coupon-unlock is
// intentionally NOT here: that email does not show a code, it tells the user to
// log an action to unlock one later.
var perRecipientCouponTemplates = map[string]bool{
	"cc-outreach-coupon": true,
	"cc-cart-goat":       true,
}

// PerRecipientCouponTemplate reports whether a template renders a real coupon
// code and should therefore get a unique, user-bound, open-expiring code. Used by
// the scheduler (which shares the same template list as instant sends).
func PerRecipientCouponTemplate(templateName string) bool {
	return perRecipientCouponTemplates[templateName]
}

// FounderCouponPercent is the discount carried by per-recipient founder coupons.
// Matches the "10% off" copy baked into the templates. Override via
// FOUNDER_COUPON_PERCENT.
func FounderCouponPercent() float64 {
	if v := strings.TrimSpace(os.Getenv("FOUNDER_COUPON_PERCENT")); v != "" {
		if p, err := strconv.ParseFloat(v, 64); err == nil && p > 0 {
			return p
		}
	}
	return 10
}

// FounderCouponOpenTTL is the window a per-recipient founder coupon stays valid,
// measured from the recipient's FIRST open. Override via
// FOUNDER_COUPON_OPEN_TTL_HOURS.
func FounderCouponOpenTTL() time.Duration {
	if v := strings.TrimSpace(os.Getenv("FOUNDER_COUPON_OPEN_TTL_HOURS")); v != "" {
		if h, err := strconv.Atoi(v); err == nil && h > 0 {
			return time.Duration(h) * time.Hour
		}
	}
	return 10 * time.Hour
}

// instantMarketingRoutingKeys are the instant-send routing keys that are
// MARKETING and must therefore respect the user's unsubscribe (ProductEmails)
// preference. Everything else in ccRoutingKeyToTemplate is transactional/expected
// (welcome, dna_ready, roadmap_delivered, webinar_registered) and always sends.
var instantMarketingRoutingKeys = map[string]bool{
	"event.cc.outreach_used":   true,
	"event.cc.outreach_coupon": true,
	"event.cc.cart_goat":       true,
	"event.cc.coupon_unlock":   true,
	"event.cc.resume_strong":   true,
	"event.cc.resume_weak":     true,
	"event.cc.id_two_tools":    true,
}

// ccSequence is one step of a scheduled cc sequence.
type ccSequence struct {
	emailType string
	delay     time.Duration
}

// ScheduleCCSequence queues a list of cc sequence steps into scheduled_emails with
// per-type dedup. Mirrors ScheduleFunnelSequence. Safe to call repeatedly.
// ScheduleCCSequence queues each step. Returns an error if ANY step failed to
// schedule (a dedup-check error or an insert error), so callers like the gate
// can avoid consuming their placeholder row when enrolment didn't actually land.
// A step that already exists (dedup hit) is NOT an error.
func ScheduleCCSequence(ctx context.Context, s *store.PostgresStore, userID string, after time.Time, steps []ccSequence) error {
	var failed int
	for _, step := range steps {
		exists, err := s.HasScheduledOrReceivedEmail(ctx, userID, step.emailType)
		if err != nil {
			slog.Error("cc sequence: dedup check failed", "type", step.emailType, "user_id", userID, "error", err)
			failed++
			continue
		}
		if exists {
			continue
		}
		if err := s.CreateScheduledEmail(ctx, userID, step.emailType, after.Add(step.delay)); err != nil {
			slog.Error("cc sequence: schedule failed", "type", step.emailType, "user_id", userID, "error", err)
			failed++
		}
	}
	if failed > 0 {
		return fmt.Errorf("cc sequence: %d of %d steps failed to schedule", failed, len(steps))
	}
	return nil
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

// ── Cross-tool engagement router ────────────────────────────────────────────
//
// Every tool is treated uniformly. When a user demonstrably USES a tool (e.g.
// uploads a resume on Outreach, creates a resume, applies to an internship,
// starts a coach session), we:
//   1. cancel the not-used chase + pending gate of EVERY tool (they engaged
//      somewhere — stop chasing them for not starting),
//   2. start that tool's used-flow.
// The "used" signal may arrive from the email (click) or directly (tool action);
// both land here as the same routing key.

// toolFlow describes one tool's used-flow + the not-used artefacts to clear.
type toolFlow struct {
	Name          string       // tool label for logs
	GatePrefix    string       // not-used gate row prefix to cancel
	ChasePrefix   string       // not-used chase prefix to cancel
	UsedStarter   string       // routing key whose instant email starts the used-flow
}

// toolFlows: registry of every tool's used-routing. Keyed by the "used" routing key.
var toolFlows = map[string]toolFlow{
	"event.cc.outreach_used": {
		Name: "outreach", GatePrefix: "cc_gate_outreach_notused",
		ChasePrefix: "cc_outreach_nudge", UsedStarter: "event.cc.outreach_used",
	},
	"event.cc.coach_used": {
		Name: "coach", GatePrefix: "cc_gate_coach_notstarted",
		ChasePrefix: "cc_nudge", UsedStarter: "event.cc.welcome", // coach used == active session; coach flow already drives DNA/roadmap
	},
	"event.cc.resume_used": {
		Name: "resume", GatePrefix: "", ChasePrefix: "", UsedStarter: "",
	},
	"event.cc.internship_used": {
		Name: "internship", GatePrefix: "", ChasePrefix: "cc_id_reengage", UsedStarter: "",
	},
}

// allNotUsedTypes lists the EXACT email_types of every tool's not-used gate row
// and every chase step, so cross-tool cancellation only ever touches the precise
// "did you start?" emails — never a value-delivery email (DNA, roadmap, push,
// convert) that merely shares a prefix. Built from the gate registry.
func allNotUsedTypes() []string {
	out := []string{}
	for _, g := range ccGates {
		if g.GateType != "" {
			out = append(out, g.GateType)
		}
		for _, step := range g.Chase {
			out = append(out, step.emailType)
		}
	}
	return out
}

// RouteToolUsed cancels every tool's not-used gate + chase rows for the user
// (they engaged somewhere), returning how many rows were cleared. Exported so it
// can be called from the click handler (engagement-via-email) and tool events.
func RouteToolUsed(ctx context.Context, s *store.PostgresStore, userID string) int {
	n, err := s.CancelPendingEmailsByTypes(ctx, userID, allNotUsedTypes())
	if err != nil {
		return 0
	}
	return n
}

// ScheduleCCChase enrols a gate's chase chain (exported for the scheduler).
func ScheduleCCChase(ctx context.Context, s *store.PostgresStore, userID string, after time.Time, chase []ccSequence) error {
	return ScheduleCCSequence(ctx, s, userID, after, chase)
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
		// DISABLED pending verification of the rebuilt founder-coupon email
		// (styled HTML + per-recipient, 10h-from-open code). The old send went out
		// as broken monospace text with a non-expiring blanket code. Re-enable this
		// line once the new cc-outreach-coupon email is verified in staging.
		// {"cc_outreach_coupon", 6 * hour}, // founder coupon a few hours later
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

// ccKind is the routing class of an event.cc.* key. It is derived ONCE from the
// flow registries below by flowKind(), so the dispatch in HandleCCEmail is a
// single switch over the kind rather than a chain of ad-hoc map lookups. Adding
// a flow = adding it to the relevant registry; the kind (and thus the handling)
// follows automatically.
type ccKind int

const (
	kindUnknown      ccKind = iota
	kindSpread              // old-user: schedule at next per-day spread slot, no instant
	kindDeferred            // schedule the whole sequence with a delay, no instant
	kindResumeImport        // special: record resume use, route to coach welcome
	kindRouterOnly          // tool-used signal: no email, just clear not-used chases
	kindInstant             // send the mapped template now (+ gate/sequence follow-ups)
)

// flowKind classifies a routing key into exactly one ccKind using the existing
// registries. This is the single routing table; HandleCCEmail switches on it.
func flowKind(routingKey string) ccKind {
	if _, ok := ccSpreadStarters[routingKey]; ok {
		return kindSpread
	}
	if _, ok := ccDeferredStarters[routingKey]; ok {
		return kindDeferred
	}
	if routingKey == "event.cc.resume_imported_existing" {
		return kindResumeImport
	}
	if tf, ok := toolFlows[routingKey]; ok && tf.UsedStarter == "" {
		return kindRouterOnly
	}
	if _, ok := ccRoutingKeyToTemplate[routingKey]; ok {
		return kindInstant
	}
	return kindUnknown
}

// HandleCCEmail handles all event.cc.* events. The kind (from flowKind) decides
// the path: spread (old-user, per-day slot), deferred (delayed sequence, no
// instant), resume-import (route to coach), router-only (tool-used, clear
// chases), or instant (send template now + gate/sequence follow-ups).
func (h *EventHandler) HandleCCEmail(ctx context.Context, routingKey string, event *CCEmailEvent) error {
	switch flowKind(routingKey) {
	case kindSpread:
		return h.handleSpread(ctx, routingKey, event)
	case kindDeferred:
		return h.handleDeferred(ctx, routingKey, event)
	case kindResumeImport:
		return h.handleResumeImport(ctx, event)
	case kindRouterOnly:
		return h.handleRouterOnly(ctx, routingKey, event)
	case kindInstant:
		return h.handleInstant(ctx, routingKey, event)
	default:
		slog.Warn("unknown cc routing key", "routing_key", routingKey)
		return nil
	}
}

// handleInstant is the original instant-send path (template now + gate/sequence
// follow-ups). Kept as the bulk of the prior HandleCCEmail body.
// handleSpread schedules an old-user sequence at the next per-day spread slot.
func (h *EventHandler) handleSpread(ctx context.Context, routingKey string, event *CCEmailEvent) error {
	steps := ccSpreadStarters[routingKey]
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

// handleDeferred schedules a whole sequence with a delay, sending nothing now.
func (h *EventHandler) handleDeferred(ctx context.Context, routingKey string, event *CCEmailEvent) error {
	steps := ccDeferredStarters[routingKey]
	if event.UserID == "" {
		slog.Warn("cc deferred email: missing user_id, cannot schedule", "routing_key", routingKey)
		return nil
	}
	ScheduleCCSequence(ctx, h.Store, event.UserID, time.Now().UTC(), steps)
	slog.Info("cc deferred sequence scheduled", "routing_key", routingKey, "user_id", event.UserID)
	return nil
}

// handleResumeImport: they uploaded an EXISTING resume, so they need direction,
// not the maker. Record resume use, clear not-used chases, route to coach.
func (h *EventHandler) handleResumeImport(ctx context.Context, event *CCEmailEvent) error {
	uid := event.UserID
	if uid == "" && event.Email != "" {
		if u, err := h.Store.GetUserByEmail(ctx, event.Email); err == nil && u != nil {
			uid = u.ID
		}
	}
	if uid != "" {
		_ = h.Store.RecordToolUsed(ctx, uid, event.Email, "resume")
		RouteToolUsed(ctx, h.Store, uid) // they engaged — clear not-used chases
	}
	// Guide to coach: run the coach welcome flow (gated, dedup-safe).
	return h.HandleCCEmail(ctx, "event.cc.welcome", event)
}

// handleRouterOnly: a tool-used signal with no instant email — record the use
// (with attribution) and clear not-used chases across tools. Resolves the user
// by id, falling back to email.
func (h *EventHandler) handleRouterOnly(ctx context.Context, routingKey string, event *CCEmailEvent) error {
	tf := toolFlows[routingKey]
	uid := event.UserID
	em := event.Email
	if uid == "" && em != "" {
		if u, err := h.Store.GetUserByEmail(ctx, em); err == nil && u != nil {
			uid = u.ID
		}
	}
	if uid != "" {
		if err := h.Store.RecordToolUsed(ctx, uid, em, tf.Name); err != nil {
			slog.Warn("tool used: record failed", "tool", tf.Name, "user_id", uid, "err", err)
		}
		if n := RouteToolUsed(ctx, h.Store, uid); n > 0 {
			slog.Info("tool used (router-only), cleared not-used chases", "tool", tf.Name, "user_id", uid, "cleared", n)
		}
	}
	return nil
}

func (h *EventHandler) handleInstant(ctx context.Context, routingKey string, event *CCEmailEvent) error {
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
		// For gated welcomes, also skip if a gate row is already pending — the
		// welcome was sent within the last 7h (the welcome row is marked sent, but
		// HasReceivedEmail on the template alone could miss a same-window re-fire
		// once cleanup races). The pending gate is the definitive "already started".
		if gate, gated := ccGates[routingKey]; gated {
			if pending, err := h.Store.HasScheduledOrReceivedEmail(ctx, event.UserID, gate.GateType); err == nil && pending {
				slog.Info("cc email: gate already pending/done, skipping duplicate welcome", "user_id", event.UserID, "gate", gate.GateType)
				return nil
			}
		}
	}

	// Respect the unsubscribe preference for MARKETING instant sends. The
	// scheduler already gates scheduled marketing on ProductEmails, but instant
	// sends bypassed it entirely (audit J2) — an unsubscribed user still got
	// instant coupons/nudges. Transactional/expected sends (welcome, dna_ready,
	// roadmap_delivered, webinar confirm) are NOT gated — those are the user's own
	// requested actions and must always go.
	if event.UserID != "" && instantMarketingRoutingKeys[routingKey] {
		if prefs, err := h.Store.GetEmailPreferences(ctx, event.UserID); err != nil {
			slog.Warn("cc email: prefs check failed, sending anyway", "user_id", event.UserID, "err", err)
		} else if !prefs.ProductEmails {
			slog.Info("cc email: user unsubscribed from product emails, skipping marketing send", "user_id", event.UserID, "routing_key", routingKey)
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
	// Never "send" to an empty address — it would hit the provider with an empty
	// recipient and then still get recorded as sent (audit E1). Bail cleanly.
	if strings.TrimSpace(recipientEmail) == "" {
		slog.Warn("cc email: no recipient address, skipping", "routing_key", routingKey, "user_id", event.UserID)
		return nil
	}

	data := map[string]interface{}{
		"UserName":     recipientName,
		"DashboardURL": h.FrontendURL + "/",
		"CouponCode":   event.CouponCode,
	}

	// Coupon emails get a UNIQUE, single-use, user-bound code instead of the
	// blanket code in the event payload. valid_until is left NULL now; the open
	// pixel starts the 10h clock on first open. If minting fails, fall back to the
	// payload code (better a working blanket code than a broken email).
	if perRecipientCouponTemplates[templateName] {
		code, err := h.Store.CreatePerRecipientCoupon(ctx, "JEREMY", event.UserID, recipientEmail, templateName, FounderCouponPercent())
		if err != nil {
			slog.Error("cc email: per-recipient coupon mint failed, using payload code", "template", templateName, "email", recipientEmail, "err", err)
		} else {
			data["CouponCode"] = code
		}
	}

	// Webinar confirmation: enrich with the admin-set title/when so the email
	// reflects the current webinar. The join link itself goes out 1 day before.
	if routingKey == "event.cc.webinar_registered" {
		if cfg, err := h.Store.GetWebinarConfig(ctx); err == nil && cfg != nil {
			data["WebinarTitle"] = cfg.Title
			data["WebinarWhen"] = webinarWhen(cfg)
		}
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
		// Cross-tool exit rule: using ANY tool means the user engaged, so cancel
		// every tool's not-used chase + gate (not just this one). A high-intent
		// user never gets "did you try it?" emails for any tool once they act.
		if tf, isUsed := toolFlows[routingKey]; isUsed {
			if err := h.Store.RecordToolUsed(ctx, event.UserID, event.Email, tf.Name); err != nil {
				slog.Warn("cc email: tool-used record failed", "tool", tf.Name, "user_id", event.UserID, "err", err)
			}
			if n := RouteToolUsed(ctx, h.Store, event.UserID); n > 0 {
				slog.Info("cc email: tool used, cleared not-used chases across tools", "user_id", event.UserID, "routing_key", routingKey, "cleared", n)
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
// HandleUserSignup is intentionally a NO-OP. The old transactional "welcome"
// email is retired — it produced a duplicate second welcome alongside the new
// flow's cc-welcome-new-user. Signups now get exactly ONE welcome, sent by the
// event.cc.welcome_new_user flow. We keep this handler so any stray/legacy
// event.user.signup is accepted and dropped (no email) rather than erroring.
func (h *EventHandler) HandleUserSignup(ctx context.Context, event *UserSignupEvent) error {
	slog.Info("event.user.signup received — old welcome retired, no email sent (new flow handles welcome)", "user_id", event.UserID)
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
		// Handle all event.cc.* routing keys generically (the new efficient flow):
		// instant-email keys, deferred/spread starters, and router-only "used"
		// events (which send no email but clear not-used chases).
		_, isInstant := ccRoutingKeyToTemplate[routingKey]
		_, isDeferred := ccDeferredStarters[routingKey]
		_, isSpread := ccSpreadStarters[routingKey]
		_, isUsed := toolFlows[routingKey]
		if isInstant || isDeferred || isSpread || isUsed {
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

// webinarWhen formats the admin-set webinar date + time into a human string for
// the email, e.g. "Monday, 15 Jun 2026 at 6:00 PM IST". Falls back gracefully.
func webinarWhen(cfg *store.WebinarConfig) string {
	if cfg == nil || cfg.WebinarDate == nil {
		return cfg.WebinarTime
	}
	d := cfg.WebinarDate.Format("Monday, 2 Jan 2006")
	if cfg.WebinarTime != "" {
		return d + " at " + cfg.WebinarTime
	}
	return d
}
