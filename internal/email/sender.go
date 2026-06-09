package email

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// SendLogger is implemented by the store to record email sends for the admin dashboard.
type SendLogger interface {
	LogEmailSent(ctx context.Context, userID, userName, emailTo, templateName, fromAddress string) error
}

// ContextKey is a typed key for values stored in a send context.
type ContextKey string

const (
	// UserIDKey carries the user's ID so LogEmailSent records it correctly.
	UserIDKey ContextKey = "emailUserID"
	// UserNameKey carries the user's display name for the same reason.
	UserNameKey ContextKey = "emailUserName"
)

// Sender handles email sending with retries
type Sender struct {
	client      *Client
	renderer    *TemplateRenderer
	trackingURL string // base URL for open tracking, e.g. https://api.studojo.com
	logger      SendLogger

	// Per-category sender addresses (set via env vars)
	supportSender    string // support.studojo.com — transactional, payment, product updates
	welcomeSender    string // welcome.studojo.pro  — onboarding, signup
	promotionsSender string // promotions.studojo.pro — funnel, marketing

	// senderPool is all configured non-empty senders, rotated round-robin so
	// volume is spread evenly across all domains instead of hammering one.
	senderPool  []string
	senderIndex uint64 // atomic counter

	unsubscribeSecret  string
	unsubscribeBaseURL string
}

// NewSender creates a new email sender
func NewSender(client *Client, renderer *TemplateRenderer) *Sender {
	return &Sender{
		client:   client,
		renderer: renderer,
	}
}

// RenderPreview renders a template with sample data and returns the HTML, for dashboard previews.
func (s *Sender) RenderPreview(name string) (string, error) {
	sample := map[string]interface{}{
		"UserName":            "Alex",
		"DashboardURL":        "https://studojo.com/",
		"OutreachURL":         "https://studojo.com/outreach",
		"InternshipURL":       "https://studojo.com/outreach",
		"ResumeName":          "My Resume.pdf",
		"ImprovementsSummary": "Your resume was optimised with 5 key improvements.",
		"ViewResumeURL":       "https://studojo.com/resumes",
		"ViewApplicationURL":  "https://studojo.com/my-applications",
		"InternshipTitle":     "Software Engineering Intern",
		"CompanyName":         "Example Corp",
		"PlanName":            "Pro",
		"Amount":              "$99",
		"OrderID":             "ORD-PREVIEW-001",
		"Name":                "Alex",
		"Email":               "alex@example.com",
		"Subject":             "Preview subject",
		"Message":             "This is a preview message body.",
		"TrackingPixelURL":    "",
	}
	if err := s.renderer.LoadTemplate(name); err != nil {
		return "", fmt.Errorf("template not found: %w", err)
	}
	return s.renderer.Render(name, sample)
}

// SetLogger attaches a send logger (the store) for admin dashboard logging
func (s *Sender) SetLogger(l SendLogger) {
	s.logger = l
}

// SetSenderAddresses configures per-category from addresses and builds the
// round-robin pool so volume is spread across all domains.
func (s *Sender) SetSenderAddresses(support, welcome, promotions string) {
	s.supportSender = support
	s.welcomeSender = welcome
	s.promotionsSender = promotions

	s.senderPool = nil
	for _, addr := range []string{support, welcome, promotions} {
		if addr != "" {
			s.senderPool = append(s.senderPool, addr)
		}
	}
}

// nextSender returns the next sender from the round-robin pool.
// Falls back to the template-appropriate sender if the pool is empty.
func (s *Sender) nextSender(templateName string) string {
	if len(s.senderPool) > 0 {
		idx := atomic.AddUint64(&s.senderIndex, 1) - 1
		return s.senderPool[int(idx)%len(s.senderPool)]
	}
	return s.getSenderForTemplate(templateName)
}

// getSenderForTemplate returns the right from address for a given template.
// Personal/onboarding emails include a display name so they appear as
// "Jeremy from Studojo" rather than a bare email address in Gmail.
func (s *Sender) getSenderForTemplate(templateName string) string {
	switch templateName {
	// Transactional — support domain signals "not marketing" to Gmail
	case "payment-thankyou", "password-changed", "forgot-password",
		"resume-optimized", "internship-applied", "contact-form",
		"welcome", "leads-ready", "signup-thankyou", "signup-followup",
		"signup-welcome-v1", "signup-welcome-v2", "signup-welcome-v3",
		"signup-welcome-v4", "signup-welcome-v5":
		if s.supportSender != "" {
			return s.supportSender
		}
	// Onboarding funnel — welcome domain
	case "funnel-welcome-new", "funnel-welcome-existing",
		"funnel-onboarding", "funnel-congratulations":
		if s.welcomeSender != "" {
			return s.welcomeSender
		}
	}
	// Everything else (funnel, nurture, promo) → promotions sender
	if s.promotionsSender != "" {
		return s.promotionsSender
	}
	return ""
}

// SetTrackingURL sets the base URL used to generate tracking pixel URLs
func (s *Sender) SetTrackingURL(baseURL string) {
	s.trackingURL = strings.TrimSuffix(baseURL, "/")
}

// SetUnsubscribeSecret configures the HMAC secret and base URL for signed unsubscribe links.
func (s *Sender) SetUnsubscribeSecret(secret, baseURL string) {
	s.unsubscribeSecret = secret
	s.unsubscribeBaseURL = strings.TrimSuffix(baseURL, "/")
}

// unsubscribeURL returns a signed one-click unsubscribe URL for the given user ID.
func (s *Sender) unsubscribeURL(userID string) string {
	if userID == "" || s.unsubscribeSecret == "" {
		return ""
	}
	mac := hmac.New(sha256.New, []byte(s.unsubscribeSecret))
	mac.Write([]byte(userID))
	token := hex.EncodeToString(mac.Sum(nil))
	return s.unsubscribeBaseURL + "/v1/unsubscribe?uid=" + url.QueryEscape(userID) + "&t=" + token
}

// SendTemplateEmail sends an email using a template
func (s *Sender) SendTemplateEmail(ctx context.Context, to, templateName string, data interface{}) error {
	// Inject tracking pixel URL into template data
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		dataMap = map[string]interface{}{}
	}
	if s.trackingURL != "" {
		trackID := templateName + "__" + to + "__" + uuid.New().String()
		dataMap["TrackingPixelURL"] = s.trackingURL + "/v1/email/track/" + trackID
	} else {
		dataMap["TrackingPixelURL"] = ""
	}

	uid, _ := ctx.Value(UserIDKey).(string)
	dataMap["UnsubscribeURL"] = s.unsubscribeURL(uid)

	htmlContent, err := s.renderer.Render(templateName, dataMap)
	if err != nil {
		return fmt.Errorf("failed to render template: %w", err)
	}

	subject, err := s.getSubject(templateName, dataMap)
	if err != nil {
		return fmt.Errorf("failed to get subject: %w", err)
	}

	// Retry logic: 3 attempts with backoff.
	// 429 rate-limit responses wait 60s before retrying; other errors use 1s/2s.
	maxRetries := 3
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			backoff := time.Duration(i) * time.Second
			if lastErr != nil && strings.Contains(lastErr.Error(), "429") {
				backoff = 62 * time.Second
			}
			slog.Warn("retrying email send", "attempt", i+1, "backoff", backoff)
			time.Sleep(backoff)
		}

		fromAddr := s.nextSender(templateName)
		err := s.client.SendEmailFrom(ctx, fromAddr, to, subject, htmlContent)
		if err == nil {
			if s.logger != nil {
				uid, _ := ctx.Value(UserIDKey).(string)
				uname, _ := ctx.Value(UserNameKey).(string)
				go func(addr, uid, uname string) {
					_ = s.logger.LogEmailSent(context.Background(), uid, uname, to, templateName, addr)
				}(fromAddr, uid, uname)
			}
			return nil
		}

		lastErr = err
		slog.Error("email send failed", "attempt", i+1, "error", err)

		// Don't retry if all ACS resources are rate-limited — no point waiting,
		// the quota is subscription-wide and won't recover in seconds.
		if strings.Contains(err.Error(), "exhausted") && strings.Contains(err.Error(), "429") {
			break
		}
	}

	return fmt.Errorf("failed to send email after %d attempts: %w", maxRetries, lastErr)
}

// getSubject returns the email subject based on template name
func (s *Sender) getSubject(templateName string, data map[string]interface{}) (string, error) {
	switch templateName {
	case "service-update":
		return "A note on service continuity", nil
	case "leads-ready":
		return "Your leads are ready.", nil
	case "checkin-reminder":
		return "Your weekly check-in is due", nil
	case "welcome":
		return "You just made a better decision than most students will this week.", nil
	case "forgot-password":
		return "Reset your Studojo password", nil
	case "resume-optimized":
		return "Your resume has been optimized", nil
	case "internship-applied":
		return "Application submitted successfully", nil
	case "password-changed":
		return "Your password has been changed", nil
	case "nurture-day3":
		return "Most students apply the wrong way", nil
	case "nurture-day7":
		return "Still looking?", nil
	case "nurture-day14":
		return "A student got 3 interview calls in one week", nil
	case "nurture-day30":
		return "One month in. Wanted to check in.", nil
	case "contact-form":
		if subj, ok := data["Subject"].(string); ok && subj != "" {
			return fmt.Sprintf("Contact Form: %s", subj), nil
		}
		return "New Contact Form Submission", nil
	// Funnel subjects
	case "funnel-welcome-new":
		return "You're in. Here's where to start.", nil
	case "funnel-welcome-existing":
		return "Good to have you back.", nil
	case "funnel-followup-v1":
		return "Still there?", nil
	case "funnel-followup-v2":
		return "Quick one.", nil
	case "funnel-followup-v3":
		return "Last nudge.", nil
	case "funnel-segmentation-v1":
		return "What are you actually trying to do right now?", nil
	case "funnel-segmentation-v2":
		return "Which one sounds like you?", nil
	case "funnel-exploration-v1":
		return "Where are students actually finding internships?", nil
	case "funnel-exploration-v2":
		return "The role that never gets posted", nil
	case "funnel-congratulations":
		return "You landed it. Now what?", nil
	case "funnel-comparison":
		return "What 300 applications and 4 callbacks actually means", nil
	case "funnel-pitching-v1":
		return "The students who skip the queue", nil
	case "funnel-pitching-v2":
		return "A reply in 48 hours", nil
	case "funnel-pitching-v3":
		return "One thing different about this approach", nil
	case "funnel-honest-question-v1":
		return "Honest question", nil
	case "funnel-honest-question-v2":
		return "Why most applications go nowhere", nil
	case "funnel-honest-question-v3":
		return "Is this still useful to you?", nil
	case "funnel-onboarding":
		return "Your 5-minute setup", nil
	case "funnel-recognition-v1":
		return "138 students placed. As of yesterday.", nil
	case "funnel-recognition-v2":
		return "Priya got 4 callbacks in 10 days", nil
	case "funnel-recognition-v3":
		return "From 0 replies to an offer in 2 weeks", nil
	case "funnel-recognition-v4":
		return "What changed for Tom at UCL", nil
	case "funnel-testimonial":
		return "Real students. Real roles.", nil
	case "funnel-pricing":
		return "Here's what you get (it's less than you think)", nil
	case "funnel-case-study":
		return "Monday to Friday: one student's week on Studojo", nil
	case "funnel-walkthrough":
		return "How it works in 4 steps", nil
	case "funnel-educational":
		return "The outreach playbook that actually gets replies", nil
	case "funnel-winback":
		return "Still here if you want it", nil
	case "signup-thankyou", "signup-welcome-v1":
		return "You're in. Here's where to start.", nil
	case "signup-welcome-v2":
		return "Most students apply to 40 roles and hear back from 3.", nil
	case "signup-welcome-v3":
		return "Here's the honest version.", nil
	case "signup-welcome-v4":
		return "One thing. Just one.", nil
	case "signup-welcome-v5":
		return "You just made a better decision than most students will this week.", nil
	case "signup-followup":
		return "Did you get a chance to try it?", nil
	case "payment-thankyou":
		return "Your payment is confirmed. You're all set.", nil
	// ── Career Coach / new efficient flows ──
	case "cc-welcome-new-user":
		return "You're in the top 3%. Here's how we know.", nil
	case "cc-outreach-nudge-d1":
		return "Did you get a chance to try Outreach Dojo?", nil
	case "cc-outreach-nudge-d2":
		return "What one student got after using Outreach Dojo", nil
	case "cc-outreach-nudge-d3":
		return "Here's exactly how to get started", nil
	case "cc-outreach-nudge-d4":
		return "The number that changes everything", nil
	case "cc-outreach-push1":
		return "You started. Here's what happens next", nil
	case "cc-outreach-push2":
		return "Students who finished this got real replies", nil
	case "cc-outreach-push3":
		return "You're one step away", nil
	case "cc-outreach-convert1":
		return "Here's what you actually get", nil
	case "cc-outreach-convert2":
		return "After you sign up. Here's exactly what happens", nil
	case "cc-outreach-payment-page":
		return "You were right there", nil
	case "cc-outreach-coupon":
		return "Something from me, Jeremy", nil
	case "cc-welcome":
		return "You asked for an honest look. Good.", nil
	case "cc-nudge-1":
		return "Did you get started?", nil
	case "cc-nudge-2":
		return "What the coach actually tells you", nil
	case "cc-nudge-3":
		return "What changed when she finally started", nil
	case "cc-profiling-idle-1":
		return "You started. Pick up where you left off.", nil
	case "cc-profiling-idle-2":
		return "You're closer than you think", nil
	case "cc-profiling-idle-3":
		return "What Vikram found when he finished", nil
	case "cc-dna-ready":
		return "Your career analysis is ready", nil
	case "cc-dna-confirm-nudge":
		return "Your analysis needs your confirmation", nil
	case "cc-roadmap-delivered":
		return "You have your roadmap. Here is how to use it.", nil
	case "cc-checkin-1":
		return "One action. This week.", nil
	case "cc-checkin-2":
		return "What students who act do differently", nil
	case "cc-checkin-3":
		return "Have you marked anything complete yet?", nil
	case "cc-upskill-nudge":
		return "The coach gets sharper every time you use it", nil
	case "cc-coupon-unlock":
		return "Log your progress and unlock something", nil
	case "cc-dormant":
		return "Most students stop here", nil
	case "cc-to-outreach":
		return "You know where you stand. Here is what to do with it.", nil
	case "cc-returning-1":
		return "Your analysis is still there", nil
	case "cc-returning-2":
		return "The gap closes fast when you focus", nil
	case "cc-returning-3":
		return "The most direct path from here", nil
	case "cc-rm-strong-1":
		return "Your resume is strong. Here is the next move.", nil
	case "cc-rm-strong-2":
		return "What students with strong resumes do next", nil
	case "cc-rm-strong-3":
		return "Your resume is ready. Are you using it?", nil
	case "cc-rm-weak-1":
		return "Your resume is a start. Here is what to do next.", nil
	case "cc-rm-weak-2":
		return "What was actually holding her back", nil
	case "cc-rm-weak-3":
		return "Before you apply, know where you stand", nil
	case "cc-id-two-tools":
		return "You have been applying. Here are two tools that change the results.", nil
	case "cc-id-reengage-1":
		return "The reason your applications are not landing", nil
	case "cc-id-reengage-2":
		return "How Rohan went from silence to three interviews", nil
	case "cc-old-1":
		return "The job search has changed since you were last here", nil
	case "cc-old-2":
		return "The students who came back", nil
	case "cc-old-3":
		return "One step, whenever you are ready", nil
	default:
		return "From Studojo", nil
	}
}
