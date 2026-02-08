package auth

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

// TokenStore handles password reset token operations
type TokenStore struct {
	db *sql.DB
}

// NewTokenStore creates a new token store
func NewTokenStore(db *sql.DB) *TokenStore {
	return &TokenStore{db: db}
}

// GenerateToken generates a secure random token
func GenerateToken() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(bytes), nil
}

// CreatePasswordResetToken creates a new password reset token
// It stores the token in both our password_reset_tokens table (for tracking) 
// and Better Auth's verification table (for Better Auth API compatibility)
func (ts *TokenStore) CreatePasswordResetToken(ctx context.Context, userID string, expiresIn time.Duration) (string, error) {
	token, err := GenerateToken()
	if err != nil {
		return "", err
	}

	id := uuid.New()
	expiresAt := time.Now().UTC().Add(expiresIn)
	now := time.Now().UTC()

	// Store in our password_reset_tokens table (for tracking and our own validation)
	_, err = ts.db.ExecContext(ctx, `
		INSERT INTO password_reset_tokens (id, user_id, token, expires_at, created_at)
		VALUES ($1, $2, $3, $4, $5)`,
		id.String(), userID, token, expiresAt, now,
	)
	if err != nil {
		return "", err
	}

	// Also store in Better Auth's verification table format for API compatibility
	// Better Auth expects: identifier = "reset-password:${token}", value = userID
	verificationID := uuid.New().String()
	identifier := fmt.Sprintf("reset-password:%s", token)
	// Delete any existing verification entry for this identifier first (in case of re-request)
	_, _ = ts.db.ExecContext(ctx, `DELETE FROM verification WHERE identifier = $1`, identifier)
	// Insert new verification entry
	_, err = ts.db.ExecContext(ctx, `
		INSERT INTO verification (id, identifier, value, expires_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		verificationID, identifier, userID, expiresAt, now, now,
	)
	if err != nil {
		// Log error but don't fail - our token system still works
		// Better Auth API might not work, but our manual flow will
		slog.Warn("failed to create Better Auth verification token", "error", err)
		// Continue - our password_reset_tokens table entry was successful
	}

	return token, nil
}

// ValidatePasswordResetToken validates a password reset token
func (ts *TokenStore) ValidatePasswordResetToken(ctx context.Context, token string) (string, error) {
	var userID string
	var expiresAt time.Time
	var usedAt sql.NullTime

	err := ts.db.QueryRowContext(ctx, `
		SELECT user_id, expires_at, used_at
		FROM password_reset_tokens
		WHERE token = $1`,
		token,
	).Scan(&userID, &expiresAt, &usedAt)

	if err == sql.ErrNoRows {
		return "", ErrInvalidToken
	}
	if err != nil {
		return "", err
	}

	if time.Now().UTC().After(expiresAt) {
		return "", ErrTokenExpired
	}

	if usedAt.Valid {
		return "", ErrTokenUsed
	}

	return userID, nil
}

// MarkTokenAsUsed marks a password reset token as used
func (ts *TokenStore) MarkTokenAsUsed(ctx context.Context, token string) error {
	_, err := ts.db.ExecContext(ctx, `
		UPDATE password_reset_tokens
		SET used_at = $1
		WHERE token = $2`,
		time.Now().UTC(), token,
	)
	return err
}

// UpdateUserPassword updates or creates a user's password account (hybrid model)
// This function ensures we never create duplicate accounts - it always updates existing ones if they exist
// Safe for production migration: will update existing accounts instead of creating duplicates
func (ts *TokenStore) UpdateUserPassword(ctx context.Context, userID, passwordHash string) error {
	// Get user email first (needed for both create and update)
	var userEmail string
	err := ts.db.QueryRowContext(ctx, `SELECT email FROM "user" WHERE id = $1`, userID).Scan(&userEmail)
	if err != nil {
		return fmt.Errorf("failed to get user email: %w", err)
	}

	// Try to update existing account first (prevents duplicates in production)
	// This ensures we update existing accounts instead of creating new ones
	result, err := ts.db.ExecContext(ctx, `
		UPDATE account
		SET password = $1, account_id = $2, updated_at = $3
		WHERE user_id = $4 AND provider_id = 'credential'`,
		passwordHash, userEmail, time.Now().UTC(), userID,
	)
	if err != nil {
		return fmt.Errorf("failed to update account: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	// If account was updated, we're done
	if rowsAffected > 0 {
		return nil
	}

	// No account exists - create one for OAuth users (hybrid model)
	// Better Auth expects account_id to be the user's email for credential accounts
	accountID := uuid.New().String()
	now := time.Now().UTC()
	_, err = ts.db.ExecContext(ctx, `
		INSERT INTO account (id, account_id, provider_id, user_id, password, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		accountID, userEmail, "credential", userID, passwordHash, now, now,
	)
	
	// If INSERT fails (e.g., due to race condition), try UPDATE again
	if err != nil {
		// Account might have been created by another process, try update again
		_, updateErr := ts.db.ExecContext(ctx, `
			UPDATE account
			SET password = $1, account_id = $2, updated_at = $3
			WHERE user_id = $4 AND provider_id = 'credential'`,
			passwordHash, userEmail, time.Now().UTC(), userID,
		)
		if updateErr == nil {
			// Update succeeded, ignore the INSERT error
			return nil
		}
		// Both failed, return the original INSERT error
		return fmt.Errorf("failed to create account: %w", err)
	}

	return nil
}

// VerifyPassword verifies a user's current password
func (ts *TokenStore) VerifyPassword(ctx context.Context, userID, password string) (bool, error) {
	var passwordHash string
	err := ts.db.QueryRowContext(ctx, `
		SELECT password FROM account
		WHERE user_id = $1 AND provider_id = 'credential' AND password IS NOT NULL
		LIMIT 1`,
		userID,
	).Scan(&passwordHash)

	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	err = bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(password))
	return err == nil, nil
}

// Errors
var (
	ErrInvalidToken = &TokenError{Message: "invalid token"}
	ErrTokenExpired = &TokenError{Message: "token has expired"}
	ErrTokenUsed    = &TokenError{Message: "token has already been used"}
)

// TokenError represents a token-related error
type TokenError struct {
	Message string
}

func (e *TokenError) Error() string {
	return e.Message
}
