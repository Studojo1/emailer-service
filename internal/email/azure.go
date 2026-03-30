package email

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/smtp"
	"strings"
	"time"
)

// Client sends email via Resend (or MailHog in development)
type Client struct {
	resendAPIKey string
	senderEmail  string
	mailhogAddr  string // non-empty when in dev/mailhog mode
	httpClient   *http.Client
}

// NewClient creates a new email client.
// If RESEND_API_KEY env var is set, use Resend.
// Otherwise fall back to MailHog via connectionString (legacy dev path).
func NewClient(connectionString, senderEmail string) (*Client, error) {
	c := &Client{
		senderEmail: senderEmail,
		httpClient:  &http.Client{Timeout: 30 * time.Second},
	}

	// Check if this looks like a Resend API key
	if strings.HasPrefix(connectionString, "re_") {
		c.resendAPIKey = connectionString
		return c, nil
	}

	// Legacy: parse as ACS connection string and check for MailHog
	endpoint := ""
	for _, part := range strings.Split(connectionString, ";") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "endpoint=") {
			endpoint = strings.TrimPrefix(part, "endpoint=")
		}
	}

	isMailHog := strings.Contains(endpoint, "mailhog") ||
		strings.Contains(endpoint, "localhost:8025") ||
		strings.Contains(endpoint, "127.0.0.1:8025")

	if isMailHog {
		host := strings.TrimPrefix(endpoint, "http://")
		host = strings.TrimPrefix(host, "https://")
		host = strings.TrimSuffix(host, ":8025")
		host = strings.TrimSuffix(host, "/")
		if host == "" {
			host = "mailhog"
		}
		c.mailhogAddr = host + ":1025"
		return c, nil
	}

	return nil, fmt.Errorf("no valid email provider configured: set RESEND_API_KEY or use MailHog endpoint")
}

// SendEmail sends an email via Resend or MailHog.
func (c *Client) SendEmail(ctx context.Context, to, subject, htmlContent string) error {
	if c.mailhogAddr != "" {
		return c.sendViaMailHog(to, subject, htmlContent)
	}
	return c.sendViaResend(ctx, to, subject, htmlContent)
}

// sendViaResend sends email using the Resend API.
func (c *Client) sendViaResend(ctx context.Context, to, subject, htmlContent string) error {
	payload := map[string]interface{}{
		"from":    c.senderEmail,
		"to":      []string{to},
		"subject": subject,
		"html":    htmlContent,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", "https://api.resend.com/emails", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.resendAPIKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("email send failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err == nil {
		if id, ok := result["id"].(string); ok {
			slog.Info("email sent via Resend", "id", id, "to", to)
		}
	}
	return nil
}

// sendViaMailHog sends email via MailHog SMTP (development mode).
func (c *Client) sendViaMailHog(to, subject, htmlContent string) error {
	msg := []byte("From: " + c.senderEmail + "\r\n" +
		"To: " + to + "\r\n" +
		"Subject: " + subject + "\r\n" +
		"MIME-Version: 1.0\r\n" +
		"Content-Type: text/html; charset=UTF-8\r\n" +
		"\r\n" +
		htmlContent + "\r\n")

	if err := smtp.SendMail(c.mailhogAddr, nil, c.senderEmail, []string{to}, msg); err != nil {
		return fmt.Errorf("failed to send via MailHog: %w", err)
	}
	slog.Info("email sent via MailHog", "to", to, "addr", c.mailhogAddr)
	return nil
}

// ValidateConnection checks if the client is properly configured.
func (c *Client) ValidateConnection(ctx context.Context) error {
	if c.resendAPIKey == "" && c.mailhogAddr == "" {
		return fmt.Errorf("no email provider configured")
	}
	return nil
}
