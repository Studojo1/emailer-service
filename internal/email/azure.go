package email

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/smtp"
	"net/url"
	"strings"
	"time"
)

// Client sends email via ACS Email, Resend, or MailHog (dev)
type Client struct {
	// Resend
	resendAPIKey string
	// ACS Email
	acsEndpoint  string
	acsAccessKey string
	// MailHog (dev)
	mailhogAddr string
	senderEmail string
	httpClient  *http.Client
}

// NewClient creates a new email client.
// Priority: RESEND_API_KEY → ACS connection string → MailHog
func NewClient(connectionString, senderEmail string) (*Client, error) {
	c := &Client{
		senderEmail: senderEmail,
		httpClient:  &http.Client{Timeout: 30 * time.Second},
	}

	// Resend API key
	if strings.HasPrefix(connectionString, "re_") {
		c.resendAPIKey = connectionString
		return c, nil
	}

	// Parse key=value; pairs (ACS / MailHog connection string)
	endpoint := ""
	accessKey := ""
	for _, part := range strings.Split(connectionString, ";") {
		part = strings.TrimSpace(part)
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch strings.ToLower(kv[0]) {
		case "endpoint":
			endpoint = kv[1]
		case "accesskey":
			accessKey = kv[1]
		}
	}

	// MailHog (dev)
	if strings.Contains(endpoint, "mailhog") || strings.Contains(endpoint, "localhost:8025") || strings.Contains(endpoint, "127.0.0.1:8025") {
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

	// ACS Email
	if endpoint != "" && accessKey != "" {
		c.acsEndpoint = strings.TrimSuffix(endpoint, "/")
		c.acsAccessKey = accessKey
		return c, nil
	}

	return nil, fmt.Errorf("no valid email provider configured: set RESEND_API_KEY or ACS connection string")
}

// SendEmail dispatches to the right provider.
func (c *Client) SendEmail(ctx context.Context, to, subject, htmlContent string) error {
	switch {
	case c.mailhogAddr != "":
		return c.sendViaMailHog(to, subject, htmlContent)
	case c.resendAPIKey != "":
		return c.sendViaResend(ctx, to, subject, htmlContent)
	case c.acsEndpoint != "":
		return c.sendViaACS(ctx, to, subject, htmlContent)
	default:
		return fmt.Errorf("no email provider configured")
	}
}

// sendViaACS sends email using Azure Communication Services Email REST API.
func (c *Client) sendViaACS(ctx context.Context, to, subject, htmlContent string) error {
	path := "/emails:send?api-version=2023-03-31"
	fullURL := c.acsEndpoint + path

	payload := map[string]interface{}{
		"senderAddress": c.senderEmail,
		"recipients": map[string]interface{}{
			"to": []map[string]string{
				{"address": to},
			},
		},
		"content": map[string]string{
			"subject":   subject,
			"html":      htmlContent,
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("acs: failed to marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", fullURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("acs: failed to create request: %w", err)
	}

	// ACS uses HMAC-SHA256 signing
	if err := c.signACSRequest(req, body); err != nil {
		return fmt.Errorf("acs: failed to sign request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("acs: request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	// 202 Accepted = queued successfully
	if resp.StatusCode != http.StatusAccepted && (resp.StatusCode < 200 || resp.StatusCode >= 300) {
		return fmt.Errorf("acs: send failed status %d: %s", resp.StatusCode, string(respBody))
	}

	operationID := resp.Header.Get("Operation-Location")
	slog.Info("email sent via ACS", "to", to, "operation", operationID)
	return nil
}

// signACSRequest adds HMAC-SHA256 auth headers required by ACS.
func (c *Client) signACSRequest(req *http.Request, body []byte) error {
	keyBytes, err := base64.StdEncoding.DecodeString(c.acsAccessKey)
	if err != nil {
		return fmt.Errorf("failed to decode access key: %w", err)
	}

	now := time.Now().UTC().Format(http.TimeFormat)
	req.Header.Set("x-ms-date", now)

	// Content hash
	hash := sha256.Sum256(body)
	contentHash := base64.StdEncoding.EncodeToString(hash[:])
	req.Header.Set("x-ms-content-sha256", contentHash)

	// Build string-to-sign
	parsedURL, err := url.Parse(req.URL.String())
	if err != nil {
		return err
	}
	pathAndQuery := parsedURL.Path
	if parsedURL.RawQuery != "" {
		pathAndQuery += "?" + parsedURL.RawQuery
	}
	host := parsedURL.Host
	stringToSign := fmt.Sprintf("%s\n%s\n%s;%s;%s",
		req.Method,
		pathAndQuery,
		now,
		host,
		contentHash,
	)

	// HMAC-SHA256
	mac := hmac.New(sha256.New, keyBytes)
	mac.Write([]byte(stringToSign))
	sig := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	req.Header.Set("Authorization", fmt.Sprintf(
		"HMAC-SHA256 SignedHeaders=x-ms-date;host;x-ms-content-sha256&Signature=%s", sig,
	))
	return nil
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
		return fmt.Errorf("resend: send failed status %d: %s", resp.StatusCode, string(respBody))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err == nil {
		if id, ok := result["id"].(string); ok {
			slog.Info("email sent via Resend", "id", id, "to", to)
		}
	}
	return nil
}

// sendViaMailHog sends email via MailHog SMTP (development only).
func (c *Client) sendViaMailHog(to, subject, htmlContent string) error {
	msg := []byte("From: " + c.senderEmail + "\r\n" +
		"To: " + to + "\r\n" +
		"Subject: " + subject + "\r\n" +
		"MIME-Version: 1.0\r\n" +
		"Content-Type: text/html; charset=UTF-8\r\n" +
		"\r\n" +
		htmlContent + "\r\n")

	if err := smtp.SendMail(c.mailhogAddr, nil, c.senderEmail, []string{to}, msg); err != nil {
		return fmt.Errorf("mailhog: send failed: %w", err)
	}
	slog.Info("email sent via MailHog", "to", to, "addr", c.mailhogAddr)
	return nil
}

// ValidateConnection checks the client is configured.
func (c *Client) ValidateConnection(ctx context.Context) error {
	if c.resendAPIKey == "" && c.mailhogAddr == "" && c.acsEndpoint == "" {
		return fmt.Errorf("no email provider configured")
	}
	return nil
}
