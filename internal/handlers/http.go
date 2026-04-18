package handlers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
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
	Store            *store.PostgresStore
	Sender           *email.Sender
	TokenStore       *auth.TokenStore
	EventHandler     *EventHandler
	FrontendURL      string // For internal service-to-service calls (e.g., http://frontend:3000)
	EmailFrontendURL string // For email links that users click (e.g., http://localhost:3000)
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
func (h *Handler) HandleGetEmailPreferences(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
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
func (h *Handler) HandleUpdateEmailPreferences(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
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

// HandlePublishEvent handles POST /v1/email/events - accepts events from frontend/other services
func (h *Handler) HandlePublishEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
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
	EmailType  string `json:"email_type"`  // welcome, nurture_day3, nurture_day7, nurture_day14, nurture_day30, leads_ready
	WithinDays int    `json:"within_days"` // 0 = all users (ignored for order-stage types)
}

// HandleBulkSendPreview handles GET /v1/email/bulk-send/preview
func (h *Handler) HandleBulkSendPreview(w http.ResponseWriter, r *http.Request) {
	emailType := r.URL.Query().Get("email_type")
	withinDays := 0
	if d := r.URL.Query().Get("within_days"); d != "" {
		if parsed, err := strconv.Atoi(d); err == nil && parsed >= 0 {
			withinDays = parsed
		}
	}

	var count int
	var err error
	if emailType == "leads_ready" {
		count, err = h.Store.CountUsersAtOrderStage(r.Context(), "leads_ready")
	} else {
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
	var req BulkSendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.EmailType == "" {
		writeError(w, "email_type is required", http.StatusBadRequest)
		return
	}

	validTypes := map[string]bool{
		"welcome": true, "nurture_day3": true, "nurture_day7": true,
		"nurture_day14": true, "nurture_day30": true, "leads_ready": true,
	}
	if !validTypes[req.EmailType] {
		writeError(w, "invalid email_type", http.StatusBadRequest)
		return
	}

	var users []store.User
	var err error
	// leads_ready targets users at that outreach order stage, not by signup date
	if req.EmailType == "leads_ready" {
		users, err = h.Store.ListUsersAtOrderStage(r.Context(), "leads_ready")
	} else {
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
	case "nurture_day3":
		return "nurture-day3", map[string]interface{}{
			"UserName":      user.Name,
			"InternshipURL": h.EmailFrontendURL + "/outreach",
		}
	case "nurture_day7":
		return "nurture-day7", map[string]interface{}{
			"UserName":    user.Name,
			"OutreachURL": h.EmailFrontendURL + "/outreach",
		}
	case "nurture_day14":
		return "nurture-day14", map[string]interface{}{
			"UserName":    user.Name,
			"OutreachURL": h.EmailFrontendURL + "/outreach",
		}
	case "nurture_day30":
		return "nurture-day30", map[string]interface{}{
			"UserName":     user.Name,
			"DashboardURL": h.EmailFrontendURL + "/",
		}
	default:
		return emailType, map[string]interface{}{
			"UserName": user.Name,
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

// HandleTrackOpen handles GET /v1/email/track/{track_id}
// Returns a 1x1 transparent pixel and records the open event.
// track_id format: {emailType}__{userID}__{uuid}
func (h *Handler) HandleTrackOpen(w http.ResponseWriter, r *http.Request) {
	trackID := r.PathValue("track_id")
	if trackID == "" {
		http.NotFound(w, r)
		return
	}

	// Parse emailType and userID from trackID (format: emailType__userID__uuid)
	parts := strings.SplitN(trackID, "__", 3)
	emailType, userID := "", ""
	if len(parts) >= 2 {
		emailType = parts[0]
		userID = parts[1]
	}

	userAgent := r.Header.Get("User-Agent")
	go func() {
		ctx := context.Background()
		h.Store.RecordEmailOpen(ctx, trackID, userID, emailType, userAgent)
		// Also mark opened_at in email_send_log (best-effort, non-blocking)
		if len(parts) >= 2 {
			emailAddr := parts[1] // track_id format: template__email__uuid
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

// Helper functions
func writeJSON(w http.ResponseWriter, data interface{}, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, message string, statusCode int) {
	writeJSON(w, map[string]string{"error": message}, statusCode)
}

