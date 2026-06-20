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

// SendLogger is implemented by the store to record email sends for the admin
// dashboard and to gate sends on the suppression list.
type SendLogger interface {
	LogEmailSent(ctx context.Context, userID, userName, emailTo, templateName, fromAddress string) error
	// IsEmailSuppressed reports whether an address has hard-bounced or complained
	// and must never be emailed again.
	IsEmailSuppressed(ctx context.Context, email string) (bool, error)
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

	limiter *rateLimiter // global ACS throttle shared by instant + scheduled sends

	lastRetryAfter time.Duration // Retry-After parsed from the most recent 429, if any
}

// transactionalTemplates are user-triggered, low-volume, time-sensitive emails.
// They draw down the shared rate budget but are never delayed by it — a depleted
// bucket must not hold up a password reset or a payment receipt. Everything else
// (onboarding, marketing, cc sequences) is paced through the limiter so a signup
// or event burst can never exceed the provider quota.
var transactionalTemplates = map[string]bool{
	"forgot-password":    true,
	"password-changed":   true,
	"payment-thankyou":   true,
	"resume-optimized":   true,
	"internship-applied": true,
	"contact-form":       true,
}

// LastRetryAfter returns the Retry-After duration the provider asked for on the
// most recent 429, or 0 if none was surfaced. The scheduler uses it to size its
// backoff; 0 means "use the default window".
func (s *Sender) LastRetryAfter() time.Duration {
	return s.lastRetryAfter
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
	// The founder coupon blast is framed as a personal note, so it always comes
	// from Jeremy (welcome sender) rather than the round-robin pool.
	if templateName == "cc-cart-goat" && s.welcomeSender != "" {
		return s.welcomeSender
	}
	// Transactional / confirmation / onboarding emails must ALWAYS use their
	// correct domain (support or welcome), never the round-robin pool. Sending a
	// webinar confirmation, payment receipt, or analysis email from the
	// promotions domain is what pushes them into Gmail's Promotions tab. The
	// round-robin (volume spreading) is only for marketing/engagement sends.
	if fixedSender := s.fixedSenderForTemplate(templateName); fixedSender != "" {
		return fixedSender
	}
	if len(s.senderPool) > 0 {
		idx := atomic.AddUint64(&s.senderIndex, 1) - 1
		return s.senderPool[int(idx)%len(s.senderPool)]
	}
	return s.getSenderForTemplate(templateName)
}

// fixedSenderForTemplate returns a non-pool sender for templates whose Gmail
// category placement matters (transactional/confirmation -> support domain,
// onboarding -> welcome domain). Returns "" for marketing/engagement sends,
// which then go through the round-robin pool.
func (s *Sender) fixedSenderForTemplate(templateName string) string {
	switch templateName {
	// Transactional + confirmations + analysis + onboarding + webinar — all routed
	// through the trusted support.studojo.com (.com) domain so Gmail places them in
	// PRIMARY, not Promotions. The .pro welcome/promotions subdomains were getting
	// tabbed into Promotions; the time-sensitive + onboarding emails belong in Primary.
	case "payment-thankyou", "password-changed", "forgot-password",
		"resume-optimized", "internship-applied", "contact-form",
		"welcome", "leads-ready",
		"cc-dna-ready", "cc-roadmap-delivered",
		"cc-webinar-confirm", "cc-webinar-link",
		// onboarding -> Primary (was welcome.studojo.pro -> Promotions)
		"cc-welcome", "cc-welcome-new-user",
		// webinar intent-funnel (the "second" webinar email) -> Primary
		"cc-webinar-funnel-all", "cc-webinar-funnel-outreach",
		"cc-webinar-funnel-coach", "cc-webinar-funnel-resume":
		if s.supportSender != "" {
			return s.supportSender
		}
	}
	return ""
}

// getSenderForTemplate returns the right from address for a given template.
// Personal/onboarding emails include a display name so they appear as
// "Jeremy from Studojo" rather than a bare email address in Gmail.
func (s *Sender) getSenderForTemplate(templateName string) string {
	switch templateName {
	// Transactional — support domain signals "not marketing" to Gmail
	case "payment-thankyou", "password-changed", "forgot-password",
		"resume-optimized", "internship-applied", "contact-form",
		"welcome", "leads-ready",
		// new-flow transactional sends (analysis/roadmap are not marketing)
		"cc-dna-ready", "cc-roadmap-delivered":
		if s.supportSender != "" {
			return s.supportSender
		}
	// Onboarding — welcome domain
	case "cc-welcome", "cc-welcome-new-user":
		if s.welcomeSender != "" {
			return s.welcomeSender
		}
	}
	// Everything else (cc engagement, promo) → promotions sender
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

// SetRateLimit installs the global ACS send throttle, sized to perHour sends.
// Both instant event sends and the scheduler pass through it, so combined volume
// can never exceed the provider quota. Call once at startup.
func (s *Sender) SetRateLimit(perHour int) {
	s.limiter = newRateLimiter(perHour)
}

// unsubscribeURL returns a signed one-click unsubscribe URL for the given user ID.
func (s *Sender) unsubscribeURL(userID string) string {
	if userID == "" || s.unsubscribeSecret == "" {
		return ""
	}
	mac := hmac.New(sha256.New, []byte(s.unsubscribeSecret))
	mac.Write([]byte(userID))
	token := hex.EncodeToString(mac.Sum(nil))
	return s.unsubscribeBaseURL + "/v1/email/unsubscribe?uid=" + url.QueryEscape(userID) + "&t=" + token
}

// SendTemplateEmail sends an email using a template
func (s *Sender) SendTemplateEmail(ctx context.Context, to, templateName string, data interface{}) error {
	// Suppression gate: never send to an address that has hard-bounced or filed a
	// spam complaint. Continuing to mail dead/complained addresses is what wrecks
	// sender-domain reputation (audit R1). A suppression-store error is logged but
	// does not block the send (fail-open on the check, not on the suppression).
	if s.logger != nil {
		if suppressed, err := s.logger.IsEmailSuppressed(ctx, to); err != nil {
			slog.Warn("suppression check failed, sending anyway", "to", to, "err", err)
		} else if suppressed {
			slog.Info("skipping send to suppressed address", "to", to, "template", templateName)
			return nil
		}
	}

	// Inject tracking pixel URL into template data
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		dataMap = map[string]interface{}{}
	}
	if s.trackingURL != "" {
		trackID := templateName + "__" + to + "__" + uuid.New().String()
		dataMap["TrackingPixelURL"] = s.trackingURL + "/v1/email/track/" + trackID
		// Click-tracking base for CTA links. Templates wrap a destination URL as
		// {{.ClickBase}}?u=<urlencoded dest> so the click is recorded before the
		// redirect. Shares the trackID with the open pixel (same email__address).
		dataMap["ClickBase"] = s.trackingURL + "/v1/email/click/" + trackID
	} else {
		dataMap["TrackingPixelURL"] = ""
		dataMap["ClickBase"] = ""
	}

	uid, _ := ctx.Value(UserIDKey).(string)
	unsubURL := s.unsubscribeURL(uid)
	dataMap["UnsubscribeURL"] = unsubURL

	// RFC 8058 one-click unsubscribe headers. Only set when we have a signed URL
	// (i.e. a marketing/sequence send with a known user) — transactional mail with
	// no uid gets no header, which is correct. Gmail/Yahoo require these to show a
	// native unsubscribe button and to keep bulk-sender reputation healthy.
	var sendHeaders map[string]string
	if unsubURL != "" {
		sendHeaders = map[string]string{
			"List-Unsubscribe":      "<" + unsubURL + ">",
			"List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
		}
	}

	htmlContent, err := s.renderer.Render(templateName, dataMap)
	if err != nil {
		return fmt.Errorf("failed to render template: %w", err)
	}

	subject, err := s.getSubject(templateName, dataMap)
	if err != nil {
		return fmt.Errorf("failed to get subject: %w", err)
	}

	// Global ACS throttle. Transactional mail draws down the budget but is never
	// blocked; everything else waits for a token so a burst can't exceed quota.
	if s.limiter != nil {
		if transactionalTemplates[templateName] {
			s.limiter.tryTake()
		} else if err := s.limiter.wait(ctx); err != nil {
			return fmt.Errorf("rate limiter wait cancelled: %w", err)
		}
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
		err := s.client.SendEmailFromWithHeaders(ctx, fromAddr, to, subject, htmlContent, sendHeaders)
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
	case "contact-form":
		if subj, ok := data["Subject"].(string); ok && subj != "" {
			return fmt.Sprintf("Contact Form: %s", subj), nil
		}
		return "New Contact Form Submission", nil
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
	case "cc-cart-goat":
		return "A code from me, GOAT10", nil
	case "cc-outreach-pricing":
		return "New pricing for Outreach Dojo", nil
	case "cc-webinar-confirm":
		return "You're registered. Here's what happens next.", nil
	case "cc-webinar-link":
		return "Your webinar link — it's tomorrow", nil
	case "cc-webinar-funnel-all", "cc-webinar-funnel-coach", "cc-webinar-funnel-resume":
		return "Before the webinar: a head start", nil
	case "cc-webinar-funnel-outreach":
		return "Before the webinar: see how replies actually happen", nil
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
	// Old / dormant user flow (tool-neutral 3 stages)
	case "cc-old-s1-1":
		return "You were nearly there", nil
	case "cc-old-s1-2":
		return "What finishing actually looks like", nil
	case "cc-old-s1-3":
		return "The last step is the easy one", nil
	case "cc-old-s2-1":
		return "You got real work done, then it stalled", nil
	case "cc-old-s2-2":
		return "What picking back up actually takes", nil
	case "cc-old-s2-3":
		return "The one step to restart", nil
	case "cc-old-s3-1":
		return "What Studojo can do for you now", nil
	case "cc-old-s3-2":
		return "A student who came back and started fresh", nil
	case "cc-old-s3-3":
		return "One step, whenever you are ready", nil
	// CTA variant blocks (swapped into S1/S2 closing emails at send time)
	case "cc-old-cta-outreach":
		return "Reach hiring managers now", nil
	case "cc-old-cta-coach":
		return "Talk to your career coach", nil
	case "cc-old-cta-two-tool":
		return "Pick the move that fits", nil
	default:
		return "From Studojo", nil
	}
}
