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
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

// acsConfig holds the endpoint + access key for one ACS resource.
type acsConfig struct {
	endpoint  string
	accessKey string
}

// Client sends email via ACS Email, Resend, or MailHog (dev)
type Client struct {
	// Resend
	resendAPIKey string
	// ACS Email — pool of resources, round-robined for higher throughput
	acsPool  []acsConfig
	acsIndex uint64 // atomic counter for round-robin
	// MailHog (dev)
	mailhogAddr string
	senderEmail string
	httpClient  *http.Client
}

// parseACSConnectionString extracts endpoint + accessKey from an ACS connection string.
// Returns ("", "") if not a valid ACS string.
func parseACSConnectionString(s string) (endpoint, accessKey string) {
	for _, part := range strings.Split(s, ";") {
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
	return
}

// NewClient creates a new email client from one connection string.
// Priority: RESEND_API_KEY → ACS connection string → MailHog
// To use multiple ACS resources, call AddACSResource after construction.
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

	endpoint, accessKey := parseACSConnectionString(connectionString)

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
		c.acsPool = append(c.acsPool, acsConfig{
			endpoint:  strings.TrimSuffix(endpoint, "/"),
			accessKey: accessKey,
		})
		return c, nil
	}

	return nil, fmt.Errorf("no valid email provider configured: set RESEND_API_KEY or ACS connection string")
}

// AddACSResource registers an additional ACS connection string for round-robin sending.
// Call after NewClient to expand the ACS pool.
func (c *Client) AddACSResource(connectionString string) error {
	if connectionString == "" {
		return nil
	}
	endpoint, accessKey := parseACSConnectionString(connectionString)
	if endpoint == "" || accessKey == "" {
		return fmt.Errorf("invalid ACS connection string")
	}
	c.acsPool = append(c.acsPool, acsConfig{
		endpoint:  strings.TrimSuffix(endpoint, "/"),
		accessKey: accessKey,
	})
	slog.Info("ACS resource added to pool", "endpoint", endpoint, "pool_size", len(c.acsPool))
	return nil
}

// htmlToPlainText strips HTML tags to produce a plain-text fallback.
// Including a plain-text part alongside HTML improves Gmail Primary placement.
func htmlToPlainText(html string) string {
	// Block elements → newlines
	re := regexp.MustCompile(`(?i)<br\s*/?>|</p>|</div>|</tr>|</li>|</h[1-6]>`)
	html = re.ReplaceAllString(html, "\n")
	// Remove all remaining tags
	html = regexp.MustCompile(`<[^>]+>`).ReplaceAllString(html, "")
	// Decode common entities
	html = strings.NewReplacer(
		"&amp;", "&", "&lt;", "<", "&gt;", ">",
		"&nbsp;", " ", "&#39;", "'", "&quot;", `"`,
		"&middot;", "·",
	).Replace(html)
	// Collapse whitespace / blank lines
	html = regexp.MustCompile(`[ \t]+`).ReplaceAllString(html, " ")
	html = regexp.MustCompile(`\n[ \t]+`).ReplaceAllString(html, "\n")
	html = regexp.MustCompile(`\n{3,}`).ReplaceAllString(html, "\n\n")
	return strings.TrimSpace(html)
}

// SendEmail dispatches to the right provider using the default sender address.
func (c *Client) SendEmail(ctx context.Context, to, subject, htmlContent string) error {
	return c.SendEmailFrom(ctx, "", to, subject, htmlContent)
}

// SendEmailFrom dispatches with an explicit from address (empty = use client default).
func (c *Client) SendEmailFrom(ctx context.Context, from, to, subject, htmlContent string) error {
	if from == "" {
		from = c.senderEmail
	}
	switch {
	case c.mailhogAddr != "":
		return c.sendViaMailHogFrom(from, to, subject, htmlContent)
	case c.resendAPIKey != "":
		return c.sendViaResend(ctx, from, to, subject, htmlContent)
	case len(c.acsPool) > 0:
		return c.sendViaACSPool(ctx, from, to, subject, htmlContent)
	default:
		return fmt.Errorf("no email provider configured")
	}
}

// sendViaACSPool sends via the ACS pool, round-robining across resources.
// On 429 from one resource it immediately tries the next.
func (c *Client) sendViaACSPool(ctx context.Context, from, to, subject, htmlContent string) error {
	n := uint64(len(c.acsPool))
	start := atomic.AddUint64(&c.acsIndex, 1) - 1
	var lastErr error
	for i := uint64(0); i < n; i++ {
		cfg := c.acsPool[(start+i)%n]
		err := c.sendViaACSConfig(ctx, cfg, from, to, subject, htmlContent)
		if err == nil {
			return nil
		}
		lastErr = err
		if strings.Contains(err.Error(), "429") {
			slog.Warn("ACS resource rate-limited, trying next in pool",
				"endpoint", cfg.endpoint, "pool_size", n)
			continue
		}
		if strings.Contains(err.Error(), "DomainNotLinked") {
			slog.Warn("ACS domain not linked on resource, trying next in pool",
				"endpoint", cfg.endpoint, "pool_size", n)
			continue
		}
		return err // non-retryable error
	}
	return fmt.Errorf("all ACS resources exhausted: %w", lastErr)
}

// parseSenderAddress splits "Display Name <email@domain.com>" into (name, email).
// If no display name is present, returns ("", address).
func parseSenderAddress(from string) (displayName, address string) {
	from = strings.TrimSpace(from)
	if lt := strings.Index(from, "<"); lt != -1 {
		displayName = strings.TrimSpace(from[:lt])
		address = strings.Trim(from[lt:], "<> ")
		return
	}
	return "", from
}

// sendViaACSConfig sends email via a specific ACS resource config.
func (c *Client) sendViaACSConfig(ctx context.Context, cfg acsConfig, from, to, subject, htmlContent string) error {
	path := "/emails:send?api-version=2023-03-31"
	fullURL := cfg.endpoint + path

	// ACS 2023-03-31 only accepts a plain email address in senderAddress.
	// Strip any "Display Name <email>" wrapper before sending.
	_, senderAddr := parseSenderAddress(from)

	payload := map[string]interface{}{
		"senderAddress": senderAddr,
		"recipients": map[string]interface{}{
			"to": []map[string]string{
				{"address": to},
			},
		},
		"replyTo": []map[string]string{
			{"address": "studojo@gmail.com", "displayName": "Studojo Support"},
		},
		"content": map[string]string{
			"subject":   subject,
			"html":      htmlContent,
			"plainText": htmlToPlainText(htmlContent),
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
	if err := c.signACSRequest(req, body, cfg.accessKey); err != nil {
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
func (c *Client) signACSRequest(req *http.Request, body []byte, accessKey string) error {
	keyBytes, err := base64.StdEncoding.DecodeString(accessKey)
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
func (c *Client) sendViaResend(ctx context.Context, from, to, subject, htmlContent string) error {
	payload := map[string]interface{}{
		"from":    from,
		"to":      []string{to},
		"subject": subject,
		"html":    htmlContent,
		"text":    htmlToPlainText(htmlContent),
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

// sendViaMailHogFrom sends email via MailHog SMTP (development only).
func (c *Client) sendViaMailHogFrom(from, to, subject, htmlContent string) error {
	msg := []byte("From: " + from + "\r\n" +
		"To: " + to + "\r\n" +
		"Subject: " + subject + "\r\n" +
		"MIME-Version: 1.0\r\n" +
		"Content-Type: text/html; charset=UTF-8\r\n" +
		"\r\n" +
		htmlContent + "\r\n")

	if err := smtp.SendMail(c.mailhogAddr, nil, from, []string{to}, msg); err != nil {
		return fmt.Errorf("mailhog: send failed: %w", err)
	}
	slog.Info("email sent via MailHog", "to", to, "addr", c.mailhogAddr)
	return nil
}

// ValidateConnection checks the client is configured.
func (c *Client) ValidateConnection(ctx context.Context) error {
	if c.resendAPIKey == "" && c.mailhogAddr == "" && len(c.acsPool) == 0 {
		return fmt.Errorf("no email provider configured")
	}
	return nil
}
