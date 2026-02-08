package email

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Sender handles email sending with retries
type Sender struct {
	client   *Client
	renderer *TemplateRenderer
}

// NewSender creates a new email sender
func NewSender(client *Client, renderer *TemplateRenderer) *Sender {
	return &Sender{
		client:   client,
		renderer: renderer,
	}
}

// SendTemplateEmail sends an email using a template
func (s *Sender) SendTemplateEmail(ctx context.Context, to, templateName string, data interface{}) error {
	htmlContent, err := s.renderer.Render(templateName, data)
	if err != nil {
		return fmt.Errorf("failed to render template: %w", err)
	}

	subject, err := s.getSubject(templateName, data)
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
func (s *Sender) getSubject(templateName string, data interface{}) (string, error) {
	switch templateName {
	case "welcome":
		return "Welcome to Studojo!", nil
	case "forgot-password":
		return "Reset your Studojo password", nil
	case "resume-optimized":
		return "Your resume has been optimized", nil
	case "internship-applied":
		return "Application submitted successfully", nil
	case "password-changed":
		return "Your password has been changed", nil
	default:
		return "Message from Studojo", nil
	}
}

