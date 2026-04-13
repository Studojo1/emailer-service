package email

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Sender handles email sending with retries
type Sender struct {
	client      *Client
	renderer    *TemplateRenderer
	trackingURL string // base URL for open tracking, e.g. https://api.studojo.com
}

// NewSender creates a new email sender
func NewSender(client *Client, renderer *TemplateRenderer) *Sender {
	return &Sender{
		client:   client,
		renderer: renderer,
	}
}

// SetTrackingURL sets the base URL used to generate tracking pixel URLs
func (s *Sender) SetTrackingURL(baseURL string) {
	s.trackingURL = strings.TrimSuffix(baseURL, "/")
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

	htmlContent, err := s.renderer.Render(templateName, dataMap)
	if err != nil {
		return fmt.Errorf("failed to render template: %w", err)
	}

	subject, err := s.getSubject(templateName, dataMap)
	if err != nil {
		return fmt.Errorf("failed to get subject: %w", err)
	}

	// Retry logic: 3 attempts with exponential backoff
	maxRetries := 3
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			backoff := time.Duration(i) * time.Second
			slog.Warn("retrying email send", "attempt", i+1, "backoff", backoff)
			time.Sleep(backoff)
		}

		err := s.client.SendEmail(ctx, to, subject, htmlContent)
		if err == nil {
			return nil
		}

		lastErr = err
		slog.Error("email send failed", "attempt", i+1, "error", err)
	}

	return fmt.Errorf("failed to send email after %d attempts: %w", maxRetries, lastErr)
}

// getSubject returns the email subject based on template name
func (s *Sender) getSubject(templateName string, data map[string]interface{}) (string, error) {
	switch templateName {
	case "welcome":
		return "Welcome to Studojo", nil
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
	case "signup-thankyou":
		return "Welcome to Studojo. Here's where to start.", nil
	case "signup-followup":
		return "Did you get a chance to try it?", nil
	case "payment-thankyou":
		return "Your payment is confirmed. You're all set.", nil
	default:
		return "From Studojo", nil
	}
}
