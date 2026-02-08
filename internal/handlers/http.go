package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"golang.org/x/crypto/bcrypt"
	"github.com/studojo/emailer-service/internal/auth"
	"github.com/studojo/emailer-service/internal/email"
	"github.com/studojo/emailer-service/internal/store"
)

// Handler holds HTTP handlers for emailer service
type Handler struct {
	Store      *store.PostgresStore
	Sender     *email.Sender
	TokenStore *auth.TokenStore
	FrontendURL string
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
	resetURL := fmt.Sprintf("%s/reset-password?token=%s", h.FrontendURL, token)
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
	
	err = h.Sender.SendTemplateEmail(ctx, user.Email, "forgot-password", emailData)
	if err != nil {
		slog.Error("failed to send reset email", "error", err)
		writeError(w, "failed to send email", http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]string{"message": "If an account exists, a password reset link has been sent"}, http.StatusOK)
}

// HandleResetPassword handles POST /v1/email/reset-password
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

	// Validate token
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

	// Hash password using bcrypt
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(req.NewPassword), bcrypt.DefaultCost)
	if err != nil {
		slog.Error("failed to hash password", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Update password
	if err := h.TokenStore.UpdateUserPassword(ctx, userID, string(passwordHash)); err != nil {
		slog.Error("failed to update password", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Check if this is creating a new password (OAuth user) or resetting existing
	hasPassword, err := h.Store.HasPasswordAccount(ctx, userID)
	if err != nil {
		slog.Warn("failed to check password account", "error", err)
		hasPassword = false
	}

	// Mark token as used
	if err := h.TokenStore.MarkTokenAsUsed(ctx, req.Token); err != nil {
		slog.Warn("failed to mark token as used", "error", err)
		// Don't fail the request if this fails
	}

	// Send password changed email only if password was updated (not created)
	if hasPassword {
		user, err := h.Store.GetUserByID(ctx, userID)
		if err == nil && user != nil {
			// Send password changed email (non-blocking)
			go func() {
				emailCtx := context.Background()
				_ = h.Sender.SendTemplateEmail(emailCtx, user.Email, "password-changed", map[string]interface{}{
					"UserName":  user.Name,
					"Timestamp": time.Now().UTC().Format(time.RFC3339),
					"SettingsURL": h.FrontendURL + "/settings",
				})
			}()
		}
	}

	message := "Password reset successfully"
	if !hasPassword {
		message = "Password created successfully! You can now sign in with email and password or continue using Google."
	}
	writeJSON(w, map[string]string{"message": message, "password_created": fmt.Sprintf("%v", !hasPassword)}, http.StatusOK)
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

	// Hash new password
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(req.NewPassword), bcrypt.DefaultCost)
	if err != nil {
		slog.Error("failed to hash password", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Update password
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

	// This endpoint is for internal use - in production, events should be published directly
	// For now, we'll just acknowledge - actual publishing happens in backend services
	writeJSON(w, map[string]string{"message": "event received"}, http.StatusOK)
}

// HandleHealth handles GET /health
func (h *Handler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]string{"status": "ok"}, http.StatusOK)
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

