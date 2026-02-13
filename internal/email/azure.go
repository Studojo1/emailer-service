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

// Client wraps Azure Communication Services Email client using REST API
type Client struct {
	endpoint    string
	accessKey   string
	senderEmail string
	httpClient  *http.Client
}

// NewClient creates a new Azure Email client from connection string
// Connection string format: endpoint=https://...;accesskey=...
// For development: endpoint=http://mailhog:8025 (MailHog API) or endpoint=smtp://mailhog:1025 (MailHog SMTP)
func NewClient(connectionString, senderEmail string) (*Client, error) {
	// Parse connection string
	endpoint := ""
	accessKey := ""
	
	parts := strings.Split(connectionString, ";")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "endpoint=") {
			endpoint = strings.TrimPrefix(part, "endpoint=")
		} else if strings.HasPrefix(part, "accesskey=") {
			accessKey = strings.TrimPrefix(part, "accesskey=")
		}
	}
	
	// For MailHog (development), endpoint is the MailHog API URL and accesskey can be empty
	// Check if this is MailHog by looking for mailhog in the endpoint
	isMailHog := strings.Contains(endpoint, "mailhog") || strings.Contains(endpoint, "localhost:8025") || strings.Contains(endpoint, "127.0.0.1:8025")
	
	if !isMailHog && (endpoint == "" || accessKey == "") {
		return nil, fmt.Errorf("invalid connection string: missing endpoint or accesskey")
	}

	return &Client{
		endpoint:    endpoint,
		accessKey:   accessKey,
		senderEmail: senderEmail,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}, nil
}

// SendEmail sends an email via Azure Communication Services REST API or MailHog (development)
func (c *Client) SendEmail(ctx context.Context, to, subject, htmlContent string) error {
	// Check if this is MailHog (development mode)
	isMailHog := strings.Contains(c.endpoint, "mailhog") || strings.Contains(c.endpoint, "localhost:8025") || strings.Contains(c.endpoint, "127.0.0.1:8025")
	
	if isMailHog {
		return c.sendViaMailHog(ctx, to, subject, htmlContent)
	}
	
	// Azure Communication Services Email REST API endpoint
	// Remove trailing slash from endpoint if present to avoid double slashes
	endpoint := strings.TrimSuffix(c.endpoint, "/")
	url := fmt.Sprintf("%s/emails:send?api-version=2023-03-31", endpoint)
	
	// Build request payload
	payload := map[string]interface{}{
		"senderAddress": c.senderEmail,
		"content": map[string]interface{}{
			"subject":     subject,
			"html":        htmlContent,
		},
		"recipients": map[string]interface{}{
			"to": []map[string]string{
				{"address": to},
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal email payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.accessKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send email request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("email send failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err == nil {
		if messageID, ok := result["messageId"].(string); ok {
			slog.Info("email sent successfully", "message_id", messageID, "to", to)
		}
	}

	return nil
}

// sendViaMailHog sends email via MailHog SMTP (development mode)
// MailHog accepts SMTP connections on port 1025
func (c *Client) sendViaMailHog(ctx context.Context, to, subject, htmlContent string) error {
	// Extract host from endpoint (e.g., http://mailhog:8025 -> mailhog)
	mailhogHost := strings.TrimPrefix(c.endpoint, "http://")
	mailhogHost = strings.TrimPrefix(mailhogHost, "https://")
	mailhogHost = strings.TrimSuffix(mailhogHost, ":8025")
	mailhogHost = strings.TrimSuffix(mailhogHost, "/")
	if mailhogHost == "" {
		mailhogHost = "mailhog"
	}
	
	// MailHog SMTP server address
	addr := mailhogHost + ":1025"
	
	// Build email message
	msg := []byte("From: " + c.senderEmail + "\r\n" +
		"To: " + to + "\r\n" +
		"Subject: " + subject + "\r\n" +
		"MIME-Version: 1.0\r\n" +
		"Content-Type: text/html; charset=UTF-8\r\n" +
		"\r\n" +
		htmlContent + "\r\n")
	
	// Send email via SMTP (MailHog doesn't require authentication)
	err := smtp.SendMail(addr, nil, c.senderEmail, []string{to}, msg)
	if err != nil {
		return fmt.Errorf("failed to send email via MailHog SMTP: %w", err)
	}
	
	slog.Info("email sent via MailHog SMTP", "to", to, "subject", subject, "host", addr)
	return nil
}

// ValidateConnection checks if the Azure client is properly configured
func (c *Client) ValidateConnection(ctx context.Context) error {
	// Simple validation - check if endpoint and key are set
	if c.endpoint == "" || c.accessKey == "" {
		return fmt.Errorf("invalid client configuration")
	}
	return nil
}

