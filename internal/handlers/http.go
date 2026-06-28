package handlers

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"bytes"
	"io"
	"strconv"
	"golang.org/x/crypto/bcrypt"
	"github.com/studojo/emailer-service/internal/auth"
	"github.com/studojo/emailer-service/internal/email"
	"github.com/studojo/emailer-service/internal/store"
)

// Handler holds HTTP handlers for emailer service
type Handler struct {
	Store               *store.PostgresStore
	Sender              *email.Sender
	TokenStore          *auth.TokenStore
	EventHandler        *EventHandler
	FrontendURL         string // For internal service-to-service calls (e.g., http://frontend:3000)
	EmailFrontendURL    string // For email links that users click (e.g., http://localhost:3000)
	UnsubscribeSecret   string // HMAC secret for signing unsubscribe tokens
}

// ForgotPasswordRequest represents a forgot password request
type ForgotPasswordRequest struct {
	Email string `json:"email"`
}

// ResetPasswordRequest represents a password reset request
type ResetPasswordRequest struct {
	Token       string `json:"token"`
	NewPassword string `json:"new_password"`
}

// ChangePasswordRequest represents a password change request (for logged-in users)
type ChangePasswordRequest struct {
	UserID         string `json:"user_id"`
	CurrentPassword string `json:"current_password"`
	NewPassword     string `json:"new_password"`
}

// EmailPreferencesRequest represents email preferences update
type EmailPreferencesRequest struct {
	ProductEmails    *bool `json:"product_emails"`
	ResumeEmails     *bool `json:"resume_emails"`
	InternshipEmails *bool `json:"internship_emails"`
	SecurityEmails   *bool `json:"security_emails"`
}

// HandleForgotPassword handles POST /v1/email/forgot-password
func (h *Handler) HandleForgotPassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ForgotPasswordRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Email == "" {
		writeError(w, "email is required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// Get user by email
	user, err := h.Store.GetUserByEmail(ctx, req.Email)
	if err != nil {
		slog.Error("failed to get user", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Don't reveal if user exists or not (security best practice)
	if user == nil {
		// Still return success to prevent email enumeration
		slog.Info("forgot password requested for non-existent user", "email", req.Email)
		writeJSON(w, map[string]string{"message": "If an account exists, a password reset link has been sent"}, http.StatusOK)
		return
	}

	// Check if user has a password account (OAuth users may not have one)
	hasPassword, err := h.Store.HasPasswordAccount(ctx, user.ID)
	if err != nil {
		slog.Error("failed to check password account", "error", err)
		// Continue anyway - allow reset for OAuth users to create password
		hasPassword = false
	}

	// Generate reset token (expires in 1 hour)
	token, err := h.TokenStore.CreatePasswordResetToken(ctx, user.ID, time.Hour)
	if err != nil {
		slog.Error("failed to create reset token", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Send email - customize message for OAuth users
	// Use EmailFrontendURL for links that users click (localhost), not internal service name
	resetURL := fmt.Sprintf("%s/reset-password?token=%s", h.EmailFrontendURL, token)
	emailData := map[string]interface{}{
		"UserName":  user.Name,
		"ResetURL":  resetURL,
		"Token":     token,
		"ExpiresIn": "1 hour",
	}
	
	// Add flag for OAuth users (creating password for first time)
	if !hasPassword {
		emailData["IsOAuthUser"] = true
		emailData["Message"] = "You can use this link to create a password for your account. After setting a password, you'll be able to sign in with either your email and password or continue using Google."
	} else {
		emailData["IsOAuthUser"] = false
		emailData["Message"] = "Click the button below to reset your password."
	}
	
	slog.Info("sending password reset email", "user_id", user.ID, "email", user.Email, "has_password", hasPassword)
	err = h.Sender.SendTemplateEmail(ctx, user.Email, "forgot-password", emailData)
	if err != nil {
		// Log error but don't reveal failure to client (security best practice - prevents email enumeration)
		slog.Error("failed to send reset email", "error", err, "user_id", user.ID, "email", user.Email)
		// Still return success to prevent email enumeration attacks
		// The token was created successfully, so the user can still reset via direct link if needed
	} else {
		slog.Info("password reset email sent successfully", "user_id", user.ID, "email", user.Email)
	}

	// Always return success to prevent email enumeration (don't reveal if email was sent or not)
	writeJSON(w, map[string]string{"message": "If an account exists, a password reset link has been sent"}, http.StatusOK)
}

// HandleResetPassword handles POST /v1/email/reset-password
// This now uses Better Auth's API endpoint to ensure 100% compatibility
func (h *Handler) HandleResetPassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ResetPasswordRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Token == "" || req.NewPassword == "" {
		writeError(w, "token and new_password are required", http.StatusBadRequest)
		return
	}

	if len(req.NewPassword) < 8 {
		writeError(w, "password must be at least 8 characters", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// Validate token first to get userID (for checking if password account exists)
	userID, err := h.TokenStore.ValidatePasswordResetToken(ctx, req.Token)
	if err != nil {
		if err == auth.ErrInvalidToken || err == auth.ErrTokenExpired || err == auth.ErrTokenUsed {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		slog.Error("failed to validate token", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Check if user already has a password account (BEFORE updating)
	wasPasswordAccount, err := h.Store.HasPasswordAccount(ctx, userID)
	if err != nil {
		slog.Warn("failed to check password account before reset", "error", err)
		wasPasswordAccount = false
	}

	// Call Better Auth's reset-password API endpoint
	// Better Auth handles password hashing, validation, and account updates internally
	resetURL := fmt.Sprintf("%s/api/auth/reset-password?token=%s", h.FrontendURL, req.Token)
	reqBody, err := json.Marshal(map[string]string{
		"newPassword": req.NewPassword,
		"token":       req.Token,
	})
	if err != nil {
		slog.Error("failed to marshal request", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", resetURL, bytes.NewBuffer(reqBody))
	if err != nil {
		slog.Error("failed to create request to Better Auth", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		slog.Error("failed to call Better Auth reset-password endpoint", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		slog.Error("failed to read Better Auth response", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if resp.StatusCode != http.StatusOK {
		slog.Error("Better Auth reset-password endpoint returned error", "status", resp.StatusCode, "body", string(body))
		// Try to parse error message from Better Auth
		var errorResp struct {
			Error   string `json:"error"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(body, &errorResp); err == nil {
			if errorResp.Message != "" {
				writeError(w, errorResp.Message, resp.StatusCode)
			} else if errorResp.Error != "" {
				writeError(w, errorResp.Error, resp.StatusCode)
			} else {
				writeError(w, "failed to reset password", resp.StatusCode)
			}
		} else {
			writeError(w, "failed to reset password", resp.StatusCode)
		}
		return
	}

	// Mark our token as used (Better Auth may have its own token system, but we track ours too)
	if err := h.TokenStore.MarkTokenAsUsed(ctx, req.Token); err != nil {
		slog.Warn("failed to mark token as used", "error", err)
		// Don't fail the request if this fails
	}

	// Send password changed email only if password was updated (not created)
	if wasPasswordAccount {
		user, err := h.Store.GetUserByID(ctx, userID)
		if err == nil && user != nil {
			// Send password changed email (non-blocking)
			go func() {
				emailCtx := context.Background()
				_ = h.Sender.SendTemplateEmail(emailCtx, user.Email, "password-changed", map[string]interface{}{
					"UserName":  user.Name,
					"Timestamp": time.Now().UTC().Format(time.RFC3339),
					"SettingsURL": h.EmailFrontendURL + "/settings",
				})
			}()
		}
	}

	// Return success response with indication if password was created vs reset
	var response map[string]interface{}
	if !wasPasswordAccount {
		response = map[string]interface{}{
			"message":          "Password created successfully! You can now sign in with email and password or continue using Google.",
			"password_created": "true",
		}
	} else {
		response = map[string]interface{}{
			"message":          "Password reset successfully",
			"password_created": "false",
		}
	}
	writeJSON(w, response, http.StatusOK)
}

// HandleGetEmailPreferences handles GET /v1/email/preferences/:user_id
//
// Internal-only: reading another user's preferences by id is an IDOR, so this is
// gated by the X-Internal-Secret header matching INTERNAL_SECRET. The frontend
// settings page must call this through a server-side proxy that holds the secret
// and scopes the user_id to the authenticated session — never from the browser.
func (h *Handler) HandleGetEmailPreferences(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !requireInternalSecret(w, r) {
		return
	}

	userID := r.PathValue("user_id")
	if userID == "" {
		writeError(w, "user_id is required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	prefs, err := h.Store.GetEmailPreferences(ctx, userID)
	if err != nil {
		slog.Error("failed to get email preferences", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, prefs, http.StatusOK)
}

// HandleUpdateEmailPreferences handles PUT /v1/email/preferences/:user_id
//
// Internal-only: writing another user's preferences by id is an IDOR (it lets a
// caller silently re-enable marketing mail for someone who opted out, or disable
// everyone's), so this is gated by the X-Internal-Secret header. The frontend
// settings page must proxy this server-side, scoping user_id to the session.
func (h *Handler) HandleUpdateEmailPreferences(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !requireInternalSecret(w, r) {
		return
	}

	userID := r.PathValue("user_id")
	if userID == "" {
		writeError(w, "user_id is required", http.StatusBadRequest)
		return
	}

	var req EmailPreferencesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// Get existing preferences
	prefs, err := h.Store.GetEmailPreferences(ctx, userID)
	if err != nil {
		slog.Error("failed to get email preferences", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Update only provided fields
	if req.ProductEmails != nil {
		prefs.ProductEmails = *req.ProductEmails
	}
	if req.ResumeEmails != nil {
		prefs.ResumeEmails = *req.ResumeEmails
	}
	if req.InternshipEmails != nil {
		prefs.InternshipEmails = *req.InternshipEmails
	}
	// Security emails cannot be disabled
	if req.SecurityEmails != nil && *req.SecurityEmails {
		prefs.SecurityEmails = true
	}

	// Update in database
	if err := h.Store.UpdateEmailPreferences(ctx, userID, prefs); err != nil {
		slog.Error("failed to update email preferences", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, prefs, http.StatusOK)
}

// HandleChangePassword handles POST /v1/email/change-password (for logged-in users)
func (h *Handler) HandleChangePassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ChangePasswordRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.UserID == "" || req.CurrentPassword == "" || req.NewPassword == "" {
		writeError(w, "user_id, current_password, and new_password are required", http.StatusBadRequest)
		return
	}

	if len(req.NewPassword) < 8 {
		writeError(w, "password must be at least 8 characters", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// Verify current password
	valid, err := h.TokenStore.VerifyPassword(ctx, req.UserID, req.CurrentPassword)
	if err != nil {
		slog.Error("failed to verify password", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}
	if !valid {
		writeError(w, "current password is incorrect", http.StatusBadRequest)
		return
	}

	// Hash new password using Better Auth's API to ensure 100% compatibility
	// Better Auth doesn't have a change-password endpoint for logged-in users,
	// so we use their hash-password endpoint and then update manually
	passwordHash, err := h.hashPasswordWithBetterAuth(req.NewPassword)
	if err != nil {
		slog.Error("failed to hash password with Better Auth", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Update password in database (Better Auth's hash format ensures compatibility)
	if err := h.TokenStore.UpdateUserPassword(ctx, req.UserID, string(passwordHash)); err != nil {
		slog.Error("failed to update password", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Send password changed email (non-blocking, always send for security)
	user, err := h.Store.GetUserByID(ctx, req.UserID)
	if err == nil && user != nil {
		go func() {
			emailCtx := context.Background()
			_ = h.Sender.SendTemplateEmail(emailCtx, user.Email, "password-changed", map[string]interface{}{
				"UserName":  user.Name,
				"Timestamp": time.Now().UTC().Format(time.RFC3339),
				"SettingsURL": h.FrontendURL + "/settings",
			})
		}()
	}

	writeJSON(w, map[string]string{"message": "Password changed successfully"}, http.StatusOK)
}

// HandlePublishEvent handles POST /v1/email/events - accepts events from other
// services (coach backend, main platform server-side). Internal-only: gated by
// the X-Internal-Secret header matching INTERNAL_SECRET. Browser/client callers
// must go through a server-side proxy that holds the secret, never call this
// endpoint directly.
func (h *Handler) HandlePublishEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !requireInternalSecret(w, r) {
		return
	}

	var event struct {
		RoutingKey string          `json:"routing_key"`
		Event      json.RawMessage `json:"event"`
	}
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		writeError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if event.RoutingKey == "" {
		writeError(w, "routing_key is required", http.StatusBadRequest)
		return
	}

	// Process the event asynchronously so we can return 200 immediately
	go func() {
		if err := h.EventHandler.ProcessEvent(context.Background(), event.RoutingKey, event.Event); err != nil {
			slog.Error("failed to process event", "routing_key", event.RoutingKey, "error", err)
		}
	}()

	writeJSON(w, map[string]string{"message": "event received"}, http.StatusOK)
}

// BulkSendRequest represents a bulk email send request
type BulkSendRequest struct {
	EmailType  string `json:"email_type"`  // a cc-* template, "welcome", or "leads_ready"
	WithinDays int    `json:"within_days"` // 0 = all users (ignored for order-stage types)
	Limit      int    `json:"limit"`       // >0 = target the most recent N signups (e.g. last 700)
}

// HandleBulkSendPreview handles GET /v1/email/bulk-send/preview.
// Internal-only (X-Internal-Secret) — reached via the control-plane admin proxy.
func (h *Handler) HandleBulkSendPreview(w http.ResponseWriter, r *http.Request) {
	if !requireInternalSecret(w, r) {
		return
	}
	emailType := r.URL.Query().Get("email_type")
	withinDays := 0
	if d := r.URL.Query().Get("within_days"); d != "" {
		if parsed, err := strconv.Atoi(d); err == nil && parsed >= 0 {
			withinDays = parsed
		}
	}
	limit := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed >= 0 {
			limit = parsed
		}
	}

	var count int
	var err error
	switch {
	case emailType == "leads_ready":
		count, err = h.Store.CountUsersAtOrderStage(r.Context(), "leads_ready")
	case limit > 0:
		count, err = h.Store.CountRecentUsers(r.Context(), limit)
	default:
		count, err = h.Store.CountUsersBySignupDate(r.Context(), withinDays)
	}
	if err != nil {
		slog.Error("failed to count users for bulk send preview", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]interface{}{
		"count":       count,
		"within_days": withinDays,
		"email_type":  emailType,
	}, http.StatusOK)
}

// HandleBulkSend handles POST /v1/email/bulk-send
// Writes rows into scheduled_emails with staggered scheduled_at times so the
// scheduler drains them. Restart-safe: rows survive pod restarts.
func (h *Handler) HandleBulkSend(w http.ResponseWriter, r *http.Request) {
	if !requireInternalSecret(w, r) {
		return
	}
	var req BulkSendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.EmailType == "" {
		writeError(w, "email_type is required", http.StatusBadRequest)
		return
	}

	// Allow the transactional bulk types plus any cc_ / cc- sequence type.
	validTypes := map[string]bool{
		"welcome": true, "leads_ready": true,
	}
	isCC := strings.HasPrefix(req.EmailType, "cc_") || strings.HasPrefix(req.EmailType, "cc-")
	if !validTypes[req.EmailType] && !isCC {
		writeError(w, "invalid email_type", http.StatusBadRequest)
		return
	}

	var users []store.User
	var err error
	switch {
	case req.EmailType == "leads_ready":
		// targets users at that outreach order stage, not by signup date
		users, err = h.Store.ListUsersAtOrderStage(r.Context(), "leads_ready")
	case req.Limit > 0:
		// "last N users" — most recent N signups (e.g. pricing blast to last 700)
		users, err = h.Store.ListRecentUsers(r.Context(), req.Limit)
	default:
		users, err = h.Store.ListUsersBySignupDate(r.Context(), req.WithinDays)
	}
	if err != nil {
		slog.Error("bulk send: list users", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Write rows into scheduled_emails with staggered scheduled_at.
	// 10s between each send, +2min cooldown every 20 — all encoded as future timestamps.
	// The scheduler drains these; pod restarts just pick up where the scheduler left off.
	ctx := r.Context()
	queued := 0
	skipped := 0
	now := time.Now().UTC()
	delay := time.Duration(0)
	batchPos := 0 // position within current batch of 20

	for _, user := range users {
		already, err := h.Store.HasScheduledOrReceivedEmail(ctx, user.ID, req.EmailType)
		if err != nil {
			slog.Error("bulk send: dedup check", "user_id", user.ID, "error", err)
		} else if already {
			skipped++
			continue
		}

		// Stagger: 10s per email, 2-min cooldown after every 20
		if batchPos > 0 && batchPos%20 == 0 {
			delay += 120 * time.Second // cooldown between batches
		}
		scheduledAt := now.Add(delay)
		delay += 10 * time.Second
		batchPos++

		if err := h.Store.CreateScheduledEmail(ctx, user.ID, req.EmailType, scheduledAt); err != nil {
			slog.Error("bulk send: create scheduled email", "user_id", user.ID, "error", err)
			continue
		}
		queued++
	}

	slog.Info("bulk send queued", "email_type", req.EmailType, "queued", queued, "skipped", skipped)
	writeJSON(w, map[string]interface{}{
		"message": fmt.Sprintf("queued %s for %d users (skipped %d already sent)", req.EmailType, queued, skipped),
		"queued":  queued,
		"skipped": skipped,
	}, http.StatusOK)
}

// buildTemplateData returns the template name and data for a given email type
func (h *Handler) buildTemplateData(emailType string, user *store.User) (string, map[string]interface{}) {
	switch emailType {
	case "leads-ready", "leads_ready":
		return "leads-ready", map[string]interface{}{
			"UserName":    user.Name,
			"OutreachURL": h.EmailFrontendURL + "/outreach",
		}
	case "welcome":
		return "welcome", map[string]interface{}{
			"UserName":     user.Name,
			"DashboardURL": h.EmailFrontendURL + "/",
		}
	default:
		// cc_* types map to cc-* templates; any other passes through unchanged.
		tmpl := emailType
		if strings.HasPrefix(emailType, "cc_") {
			tmpl = strings.ReplaceAll(emailType, "_", "-")
		}
		return tmpl, map[string]interface{}{
			"UserName":     user.Name,
			"DashboardURL": h.EmailFrontendURL + "/",
		}
	}
}

// hashPasswordWithBetterAuth hashes a password using Better Auth's API
// DEPRECATED: This function is only used for HandleChangePassword since Better Auth
// doesn't have a change-password endpoint for logged-in users. HandleResetPassword
// now uses Better Auth's /api/auth/reset-password endpoint directly.
// This ensures the hash format is compatible with Better Auth's validation
func (h *Handler) hashPasswordWithBetterAuth(password string) (string, error) {
	// Try to use Better Auth's hash-password endpoint
	hashURL := fmt.Sprintf("%s/api/auth/hash-password", h.FrontendURL)
	
	reqBody, err := json.Marshal(map[string]string{"password": password})
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}
	
	req, err := http.NewRequest("POST", hashURL, bytes.NewBuffer(reqBody))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		// Fallback to local bcrypt if Better Auth endpoint is unavailable
		slog.Warn("Better Auth hash endpoint unavailable, using local bcrypt", "error", err)
		hash, err := bcrypt.GenerateFromPassword([]byte(password), 10)
		if err != nil {
			return "", fmt.Errorf("failed to hash password: %w", err)
		}
		return string(hash), nil
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		// Fallback to local bcrypt if Better Auth endpoint returns error
		slog.Warn("Better Auth hash endpoint returned error, using local bcrypt", "status", resp.StatusCode, "body", string(body))
		hash, err := bcrypt.GenerateFromPassword([]byte(password), 10)
		if err != nil {
			return "", fmt.Errorf("failed to hash password: %w", err)
		}
		return string(hash), nil
	}
	
	var result struct {
		Hash string `json:"hash"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		// Fallback to local bcrypt if response parsing fails
		slog.Warn("Failed to parse Better Auth response, using local bcrypt", "error", err)
		hash, err := bcrypt.GenerateFromPassword([]byte(password), 10)
		if err != nil {
			return "", fmt.Errorf("failed to hash password: %w", err)
		}
		return string(hash), nil
	}
	
	return result.Hash, nil
}

// HandleHealth handles GET /health
func (h *Handler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]string{"status": "ok"}, http.StatusOK)
}

// parseTrackID splits a track_id of the form {emailType}__{email}__{uuid}.
// The email's local part can legitimately contain "__", which broke the old
// left-to-right SplitN (audit N7) — it would truncate the email at the first
// "__" and record opens/clicks for the wrong address. emailType (a template
// name) and the uuid never contain "__", so we take emailType from the FIRST
// "__" and the uuid from the LAST "__", leaving everything in between as the
// full email. ok is false if the id isn't well-formed.
func parseTrackID(trackID string) (emailType, email string, ok bool) {
	first := strings.Index(trackID, "__")
	last := strings.LastIndex(trackID, "__")
	if first < 0 || last <= first {
		return "", "", false
	}
	return trackID[:first], trackID[first+2 : last], true
}

// HandleTrackOpen handles GET /v1/email/track/{track_id}
// Returns a 1x1 transparent pixel and records the open event.
// track_id format: {emailType}__{email}__{uuid}
func (h *Handler) HandleTrackOpen(w http.ResponseWriter, r *http.Request) {
	trackID := r.PathValue("track_id")
	if trackID == "" {
		http.NotFound(w, r)
		return
	}

	emailType, emailAddr, ok := parseTrackID(trackID)

	userAgent := r.Header.Get("User-Agent")
	go func() {
		ctx := context.Background()
		h.Store.RecordEmailOpen(ctx, trackID, emailAddr, emailType, userAgent)
		// Also mark opened_at in email_send_log (best-effort, non-blocking)
		if ok {
			h.Store.MarkEmailOpened(ctx, emailAddr, emailType)
		}
	}()

	// 1x1 transparent GIF
	pixel, _ := base64.StdEncoding.DecodeString("R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7")
	w.Header().Set("Content-Type", "image/gif")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
	w.Header().Set("Pragma", "no-cache")
	w.WriteHeader(http.StatusOK)
	w.Write(pixel)
}

// HandleTrackClick handles GET /v1/email/click/{track_id}?u=<dest>
// Records a CTA click, then 302-redirects to the destination URL. This is the
// "engaged" signal that gates the not-used chase (alongside opens).
// track_id format matches the open pixel: {emailType}__{email}__{uuid}.
func (h *Handler) HandleTrackClick(w http.ResponseWriter, r *http.Request) {
	trackID := r.PathValue("track_id")
	dest := r.URL.Query().Get("u")

	// Only redirect to our own properties — never an arbitrary external URL
	// (open-redirect guard). Fall back to the frontend root on anything odd.
	fallback := h.EmailFrontendURL
	if fallback == "" {
		fallback = "https://studojo.com"
	}
	target := fallback
	if dest != "" {
		if u, err := url.Parse(dest); err == nil && (u.Scheme == "https" || u.Scheme == "http") &&
			(strings.HasSuffix(u.Hostname(), "studojo.com") || strings.HasSuffix(u.Hostname(), "studojo.pro")) {
			target = dest
		}
	}

	if trackID != "" {
		emailType, emailAddr, _ := parseTrackID(trackID)
		userAgent := r.Header.Get("User-Agent")
		go func() {
			ctx := context.Background()
			h.Store.RecordEmailClick(ctx, trackID, emailAddr, emailType, userAgent)
			// Engagement-via-email: clicking any flow email means the user engaged,
			// so clear their not-used chases across every tool (same as tool use).
			if emailAddr != "" {
				if u, err := h.Store.GetUserByEmail(ctx, emailAddr); err == nil && u != nil {
					if n := RouteToolUsed(ctx, h.Store, u.ID); n > 0 {
						slog.Info("click engagement: cleared not-used chases", "user_id", u.ID, "cleared", n)
					}
				}
			}
		}()
	}

	http.Redirect(w, r, target, http.StatusFound)
}

// HandleUnsubscribe handles GET and POST /v1/email/unsubscribe?uid=<userID>&t=<hmac>.
//
// Public endpoint, signed-token protected. RFC 8058 one-click unsubscribe:
//   - POST performs the opt-out. Gmail/Yahoo's native button (driven by the
//     List-Unsubscribe-Post header) POSTs here, as does the confirm button below.
//   - GET only renders a confirmation page and mutates NOTHING. This is deliberate:
//     mail scanners, SafeLinks, and image/link prefetchers issue GET requests to
//     every URL in an email, so performing the opt-out on GET would unsubscribe
//     users who never clicked. The page POSTs back to this same URL to confirm.
func (h *Handler) HandleUnsubscribe(w http.ResponseWriter, r *http.Request) {
	uid := r.URL.Query().Get("uid")
	token := r.URL.Query().Get("t")
	if uid == "" || token == "" {
		http.Error(w, "invalid unsubscribe link", http.StatusBadRequest)
		return
	}

	// Verify HMAC (constant-time) before doing anything else.
	mac := hmac.New(sha256.New, []byte(h.UnsubscribeSecret))
	mac.Write([]byte(uid))
	expected := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(token), []byte(expected)) {
		http.Error(w, "invalid unsubscribe link", http.StatusBadRequest)
		return
	}

	// GET: show a confirmation page only — never mutate (prefetch/scanner safe).
	if r.Method == http.MethodGet {
		action := html.EscapeString(r.URL.RequestURI())
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprintf(w, `<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Unsubscribe — Studojo</title>
<style>body{margin:0;padding:40px 16px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f5f5;color:#171717;display:flex;align-items:center;justify-content:center;min-height:80vh;}
.card{max-width:480px;background:#fff;border:2px solid #171717;border-radius:24px;padding:40px 36px;box-shadow:6px 6px 0 #171717;text-align:center;}
h1{font-size:24px;font-weight:700;margin:0 0 12px;}
p{color:#525252;font-size:15px;line-height:1.7;margin:0 0 24px;}
button{background:#171717;color:#fff;border:none;border-radius:12px;padding:14px 28px;font-size:15px;font-weight:600;cursor:pointer;}
a{display:inline-block;margin-top:16px;color:#8b5cf6;text-decoration:none;font-weight:600;}</style>
</head><body><div class="card">
<h1>Unsubscribe from marketing emails?</h1>
<p>You'll stop receiving marketing emails from Studojo. Transactional emails (password resets, payment confirmations) will still come through.</p>
<form method="POST" action="%s"><button type="submit">Confirm unsubscribe</button></form>
<a href="https://studojo.com">No, keep me subscribed</a>
</div></body></html>`, action)
		return
	}

	// POST: perform the opt-out.
	if err := h.Store.UnsubscribeUser(r.Context(), uid); err != nil {
		slog.Error("unsubscribe: failed to update preferences", "user_id", uid, "error", err)
		http.Error(w, "something went wrong, please try again", http.StatusInternalServerError)
		return
	}

	slog.Info("user unsubscribed", "user_id", uid)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, `<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Unsubscribed — Studojo</title>
<style>body{margin:0;padding:40px 16px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f5f5;color:#171717;display:flex;align-items:center;justify-content:center;min-height:80vh;}
.card{max-width:480px;background:#fff;border:2px solid #171717;border-radius:24px;padding:40px 36px;box-shadow:6px 6px 0 #171717;text-align:center;}
h1{font-size:24px;font-weight:700;margin:0 0 12px;}
p{color:#525252;font-size:15px;line-height:1.7;margin:0 0 20px;}
a{color:#8b5cf6;text-decoration:none;font-weight:600;}</style>
</head><body><div class="card">
<h1>You're unsubscribed.</h1>
<p>You won't receive marketing emails from Studojo anymore. Transactional emails (password resets, payment confirmations) will still come through.</p>
<p><a href="https://studojo.com">Back to Studojo</a></p>
</div></body></html>`)
}

// HandleInbound records an inbound reply (the highest-intent signal a user can
// give) and cancels all their pending marketing chases. Secret-gated; meant to
// be fed by a mailbox poller / forward rule / provider inbound webhook pointed at
// the reply-to address. Body: {"from": "...", "subject": "...", "email_type": "..."}.
// email_type is optional/best-effort.
func (h *Handler) HandleInbound(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !requireInternalSecret(w, r) {
		return
	}
	var req struct {
		From      string `json:"from"`
		Subject   string `json:"subject"`
		EmailType string `json:"email_type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.From) == "" {
		writeError(w, "from is required", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	if err := h.Store.RecordReply(ctx, req.From, req.Subject, req.EmailType); err != nil {
		slog.Error("inbound: failed to record reply", "from", req.From, "error", err)
		writeError(w, "failed to record reply", http.StatusInternalServerError)
		return
	}
	// A reply means they're engaged and talking to us — stop all marketing chases.
	cancelled := 0
	if u, err := h.Store.GetUserByEmail(ctx, req.From); err == nil && u != nil {
		if n, cerr := h.Store.CancelPendingCCMarketingEmails(ctx, u.ID); cerr == nil {
			cancelled = n
		}
	}
	slog.Info("inbound reply recorded", "from", req.From, "cancelled_chases", cancelled)
	writeJSON(w, map[string]interface{}{"status": "recorded", "cancelled": cancelled}, http.StatusOK)
}

// HandleEmailDeliveryReport receives Azure Communication Services delivery
// reports (via Event Grid) and suppresses addresses that hard-bounce or file a
// spam complaint, so we stop mailing dead/complaining inboxes and protect sender
// reputation (audit R1). Secret-gated. Handles the Event Grid subscription
// validation handshake, and is tolerant of the event shape (best-effort parse).
func (h *Handler) HandleEmailDeliveryReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !requireInternalSecret(w, r) {
		return
	}

	// Event Grid delivers a JSON array of events.
	var events []struct {
		EventType string          `json:"eventType"`
		Data      json.RawMessage `json:"data"`
	}
	if err := json.NewDecoder(r.Body).Decode(&events); err != nil {
		writeError(w, "invalid body", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	suppressed := 0
	for _, ev := range events {
		// 1) Subscription validation handshake — echo the code back.
		if ev.EventType == "Microsoft.EventGrid.SubscriptionValidationEvent" {
			var vd struct {
				ValidationCode string `json:"validationCode"`
			}
			_ = json.Unmarshal(ev.Data, &vd)
			writeJSON(w, map[string]string{"validationResponse": vd.ValidationCode}, http.StatusOK)
			return
		}

		// 2) Delivery report — suppress on a permanent failure or complaint.
		if ev.EventType == "Microsoft.Communication.EmailDeliveryReportReceived" {
			var dr struct {
				Recipient      string `json:"recipient"`
				DeliveryStatus string `json:"deliveryStatus"`
			}
			if err := json.Unmarshal(ev.Data, &dr); err != nil || dr.Recipient == "" {
				continue
			}
			// ACS statuses: Delivered, Bounced, Failed, Quarantined, Suppressed,
			// FilteredSpam. Treat permanent-failure / complaint signals as suppressible;
			// transient/Delivered are ignored.
			reason := ""
			switch strings.ToLower(dr.DeliveryStatus) {
			case "bounced":
				reason = "hard_bounce"
			case "quarantined", "filteredspam", "suppressed":
				reason = "complaint"
			}
			if reason == "" {
				continue
			}
			if err := h.Store.SuppressEmail(ctx, dr.Recipient, reason); err != nil {
				slog.Error("delivery-report: suppress failed", "recipient", dr.Recipient, "err", err)
				continue
			}
			// Also stop any pending marketing chases to this person.
			if u, err := h.Store.GetUserByEmail(ctx, dr.Recipient); err == nil && u != nil {
				_, _ = h.Store.CancelPendingCCMarketingEmails(ctx, u.ID)
			}
			suppressed++
			slog.Info("delivery-report: address suppressed", "recipient", dr.Recipient, "reason", reason, "status", dr.DeliveryStatus)
		}
	}
	writeJSON(w, map[string]interface{}{"status": "ok", "suppressed": suppressed}, http.StatusOK)
}

// HandleWebinarLinkCron sends the day-before webinar email (single template for
// everyone — the toolkit + playbook + join-link email) to every registrant when
// the webinar is exactly ONE DAY away. Idempotent (webinar_link_sent dedup) so it
// can run daily and safely re-run. Secret-gated; driven by a scheduled GitHub Action.
func (h *Handler) HandleWebinarLinkCron(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !requireInternalSecret(w, r) {
		return
	}
	ctx := r.Context()
	cfg, err := h.Store.GetWebinarConfig(ctx)
	if err != nil || cfg == nil || cfg.WebinarDate == nil || cfg.JoinURL == "" {
		writeJSON(w, map[string]interface{}{"sent": 0, "reason": "no webinar configured"}, http.StatusOK)
		return
	}
	// Send only when the webinar is tomorrow (date-only comparison, UTC) — UNLESS
	// ?force=true is passed. force is a manual one-off override (e.g. to re-send a
	// corrected join link on the webinar day itself, after the normal day-before
	// window has passed). It bypasses ONLY the date gate; dedup (webinar_link_sent)
	// and the corrected saved JoinURL still apply, so it can't double-send and uses
	// the right link.
	force := r.URL.Query().Get("force") == "true"
	tomorrow := time.Now().UTC().AddDate(0, 0, 1).Format("2006-01-02")
	webinarDay := cfg.WebinarDate.Format("2006-01-02")
	if webinarDay != tomorrow && !force {
		writeJSON(w, map[string]interface{}{"sent": 0, "reason": "not one day before", "webinar_date": webinarDay, "tomorrow": tomorrow}, http.StatusOK)
		return
	}

	regs, err := h.Store.ListWebinarRegistrantsNeedingLink(ctx, webinarDay)
	if err != nil {
		writeError(w, "failed to list registrants", http.StatusInternalServerError)
		return
	}
	when := webinarWhen(cfg)

	// Send in the background and return immediately. Sending is synchronous and
	// rate-limited (~80/hr) inside SendTemplateEmail, so a loop over hundreds of
	// registrants takes far longer than the gateway's request timeout — the old
	// inline loop reliably 504'd partway through, leaving most registrants without
	// the link. We detach the work onto context.Background() (the request ctx is
	// cancelled the moment we respond) and dedup via webinar_link_sent so a partial
	// run, a retry, or an overlapping cron tick never double-sends.
	title, joinURL := cfg.Title, cfg.JoinURL
	go func() {
		bg := context.Background()
		sent, failed := 0, 0
		for _, reg := range regs {
			data := map[string]interface{}{
				"UserName":     reg.FullName,
				"WebinarTitle": title,
				"WebinarWhen":  when,
				"JoinURL":      joinURL,
			}
			// One email for everyone (no intent branching): the toolkit + playbook +
			// join-link email. The toolkit button points to the all-tools landing page;
			// the playbook button to the PDF; the join button to the meeting.
			if serr := h.Sender.SendTemplateEmail(bg, reg.Email, "cc-webinar-toolkit", data); serr != nil {
				slog.Error("webinar toolkit: send failed", "email", reg.Email, "error", serr)
				failed++
				continue
			}
			_ = h.Store.MarkWebinarLinkSent(bg, reg.Email, webinarDay)
			sent++
		}
		slog.Info("webinar link cron complete", "sent", sent, "failed", failed, "webinar_date", webinarDay)
	}()

	slog.Info("webinar link cron started", "registrants", len(regs), "webinar_date", webinarDay, "forced", force)
	writeJSON(w, map[string]interface{}{"status": "started", "registrants": len(regs), "webinar_date": webinarDay, "forced": force}, http.StatusAccepted)
}

// HandleAdminWebinars (GET /v1/admin/webinars) returns the list of webinars with
// per-webinar registrant counts + how many are conducted vs upcoming. Powers the
// dashboard "webinars conducted" view.
func (h *Handler) HandleAdminWebinars(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	list, err := h.Store.ListWebinars(ctx)
	if err != nil {
		writeError(w, "failed to list webinars", http.StatusInternalServerError)
		return
	}
	conducted, upcoming, _ := h.Store.CountWebinarsByStatus(ctx)
	totalReg, _ := h.Store.CountWebinarRegistrants(ctx)
	if list == nil {
		list = []store.Webinar{}
	}
	writeJSON(w, map[string]interface{}{
		"webinars":            list,
		"conducted":           conducted,
		"upcoming":            upcoming,
		"total_registrations": totalReg,
	}, http.StatusOK)
}

// HandleWebinarConfig is the admin GET/PUT for the webinar date + join link.
func (h *Handler) HandleWebinarConfig(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	switch r.Method {
	case http.MethodGet:
		cfg, _ := h.Store.GetWebinarConfig(ctx)
		count, _ := h.Store.CountWebinarRegistrants(ctx)
		date := ""
		if cfg != nil && cfg.WebinarDate != nil {
			date = cfg.WebinarDate.Format("2006-01-02")
		}
		writeJSON(w, map[string]interface{}{
			"title": cfg.Title, "webinar_date": date, "webinar_time": cfg.WebinarTime,
			"join_url": cfg.JoinURL, "registrants": count,
		}, http.StatusOK)
	case http.MethodPut:
		var req struct {
			Title       string `json:"title"`
			WebinarDate string `json:"webinar_date"`
			WebinarTime string `json:"webinar_time"`
			JoinURL     string `json:"join_url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, "invalid body", http.StatusBadRequest)
			return
		}
		if err := h.Store.SetWebinarConfig(ctx, req.Title, req.WebinarDate, req.WebinarTime, req.JoinURL); err != nil {
			writeError(w, "failed to save: "+err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]string{"status": "saved"}, http.StatusOK)
	default:
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// HandleWebinarTest sends the real day-before toolkit email (cc-webinar-toolkit)
// to a single test address using the CURRENTLY SAVED webinar config — same Title,
// WebinarWhen and JoinURL the cron would send to registrants. This is the only
// faithful preview of what registrants get (the generic Send-Email path renders
// the template's fallback JoinURL, not the saved one). It does NOT touch the
// webinar_link_sent dedup table, so it never affects the real send.
func (h *Handler) HandleWebinarTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		To   string `json:"to"`
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.To == "" {
		writeError(w, "to is required", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	cfg, err := h.Store.GetWebinarConfig(ctx)
	if err != nil || cfg == nil {
		writeError(w, "no webinar configured", http.StatusBadRequest)
		return
	}
	name := req.Name
	if name == "" {
		name = "there"
	}
	data := map[string]interface{}{
		"UserName":     name,
		"WebinarTitle": cfg.Title,
		"WebinarWhen":  webinarWhen(cfg),
		"JoinURL":      cfg.JoinURL,
	}
	if serr := h.Sender.SendTemplateEmail(ctx, req.To, "cc-webinar-toolkit", data); serr != nil {
		slog.Error("webinar test send failed", "to", req.To, "error", serr)
		writeError(w, "send failed: "+serr.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{
		"status": "sent", "to": req.To, "join_url": cfg.JoinURL,
	}, http.StatusOK)
}

// HandleWebinarLinkSentStats is a READ-ONLY admin check: for a webinar date and
// a cutoff time, how many link emails went out before vs after the cutoff. Use
// it to size a corrected-link re-send (before_cutoff = recipients who got the
// OLD/broken link) without sending or deleting anything.
//   GET /v1/admin/webinar/link-stats?date=2026-06-28&cutoff=2026-06-27T17:30:00Z
// cutoff defaults to now if omitted.
func (h *Handler) HandleWebinarLinkSentStats(w http.ResponseWriter, r *http.Request) {
	date := r.URL.Query().Get("date")
	if date == "" {
		if cfg, _ := h.Store.GetWebinarConfig(r.Context()); cfg != nil && cfg.WebinarDate != nil {
			date = cfg.WebinarDate.Format("2006-01-02")
		}
	}
	if date == "" {
		writeError(w, "date is required (no webinar configured)", http.StatusBadRequest)
		return
	}
	cutoff := time.Now().UTC()
	if c := r.URL.Query().Get("cutoff"); c != "" {
		parsed, err := time.Parse(time.RFC3339, c)
		if err != nil {
			writeError(w, "cutoff must be RFC3339 (e.g. 2026-06-27T17:30:00Z)", http.StatusBadRequest)
			return
		}
		cutoff = parsed
	}
	stats, err := h.Store.GetWebinarLinkSentStats(r.Context(), date, cutoff)
	if err != nil {
		writeError(w, "failed to load stats: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{
		"webinar_date": date, "cutoff": cutoff.Format(time.RFC3339), "stats": stats,
	}, http.StatusOK)
}

// requireInternalSecret enforces that the request carries the shared internal
// secret. Returns true if the caller is authorised; otherwise it writes a 401
// and returns false. Used by every service-to-service route so browser/client
// callers must go through a server-side proxy that holds the secret.
func requireInternalSecret(w http.ResponseWriter, r *http.Request) bool {
	secret := os.Getenv("INTERNAL_SECRET")
	if secret == "" {
		// Misconfiguration, not an attack: the service can't authenticate anyone,
		// so every event POST silently 401s. Log it loudly so it's diagnosable
		// instead of looking like "emails just aren't firing" (audit B8).
		slog.Error("requireInternalSecret: INTERNAL_SECRET is not set — rejecting all internal calls", "path", r.URL.Path)
		writeError(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	if r.Header.Get("X-Internal-Secret") != secret {
		slog.Warn("requireInternalSecret: rejected call with missing/invalid X-Internal-Secret", "path", r.URL.Path)
		writeError(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

// Helper functions
func writeJSON(w http.ResponseWriter, data interface{}, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, message string, statusCode int) {
	writeJSON(w, map[string]string{"error": message}, statusCode)
}

// CheckinReminderRequest is the body for POST /v1/email/checkin-reminder.
type CheckinReminderRequest struct {
	To       string `json:"to"`
	UserName string `json:"user_name"`
}

// HandleCheckinReminder sends the Career Coach weekly check-in reminder.
// Internal service-to-service route (the coach calls it). Gated by the
// X-Internal-Secret header matching INTERNAL_SECRET. Unlike the funnel events
// it does NOT dedup, since the reminder is intentionally recurring (weekly).
func (h *Handler) HandleCheckinReminder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !requireInternalSecret(w, r) {
		return
	}
	var req CheckinReminderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.To == "" {
		writeError(w, "to is required", http.StatusBadRequest)
		return
	}
	if req.UserName == "" {
		req.UserName = "there"
	}
	ctx := r.Context()
	if err := h.Sender.SendTemplateEmail(ctx, req.To, "checkin-reminder", map[string]interface{}{
		"UserName": req.UserName,
	}); err != nil {
		slog.Error("checkin-reminder send failed", "to", req.To, "error", err)
		writeError(w, "send failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{"status": "sent", "to": req.To}, http.StatusOK)
}

// SendTemplateRequest is the body for POST /v1/email/send-template.
type SendTemplateRequest struct {
	To         string `json:"to"`
	Template   string `json:"template"`
	UserName   string `json:"user_name"`
	CouponCode string `json:"coupon_code"`
}

// HandleSendTemplate sends any registered template by name. Internal
// service-to-service route (the Career Coach backend calls it for all the
// new flow emails). Gated by the X-Internal-Secret header matching
// INTERNAL_SECRET. Deduplication and scheduling are the caller's
// responsibility, so this route always sends what it is asked to.
func (h *Handler) HandleSendTemplate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !requireInternalSecret(w, r) {
		return
	}
	var req SendTemplateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.To == "" || req.Template == "" {
		writeError(w, "to and template are required", http.StatusBadRequest)
		return
	}
	if req.UserName == "" {
		req.UserName = "there"
	}
	data := map[string]interface{}{
		"UserName":     req.UserName,
		"DashboardURL": "https://studojo.com/",
		"CouponCode":   req.CouponCode,
	}
	ctx := r.Context()
	if err := h.Sender.SendTemplateEmail(ctx, req.To, req.Template, data); err != nil {
		slog.Error("send-template failed", "to", req.To, "template", req.Template, "error", err)
		writeError(w, "send failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{"status": "sent", "to": req.To, "template": req.Template}, http.StatusOK)
}

