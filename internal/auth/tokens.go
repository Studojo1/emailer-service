package auth

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
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
func (ts *TokenStore) CreatePasswordResetToken(ctx context.Context, userID string, expiresIn time.Duration) (string, error) {
	token, err := GenerateToken()
	if err != nil {
		return "", err
	}

	id := uuid.New()
	expiresAt := time.Now().UTC().Add(expiresIn)

	_, err = ts.db.ExecContext(ctx, `
		INSERT INTO password_reset_tokens (id, user_id, token, expires_at, created_at)
		VALUES ($1, $2, $3, $4, $5)`,
		id.String(), userID, token, expiresAt, time.Now().UTC(),
	)
	if err != nil {
		return "", err
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
func (ts *TokenStore) UpdateUserPassword(ctx context.Context, userID, passwordHash string) error {
	// Check if credential account exists
	var existingID string
	err := ts.db.QueryRowContext(ctx, `
		SELECT id FROM account
		WHERE user_id = $1 AND provider_id = 'credential'
		LIMIT 1`,
		userID,
	).Scan(&existingID)

	if err == sql.ErrNoRows {
		// Create new credential account for OAuth users (hybrid model)
		// Better Auth expects account_id to be the user's email for credential accounts
		var userEmail string
		err = ts.db.QueryRowContext(ctx, `SELECT email FROM "user" WHERE id = $1`, userID).Scan(&userEmail)
		if err != nil {
			return fmt.Errorf("failed to get user email: %w", err)
		}
		
		accountID := uuid.New().String()
		now := time.Now().UTC()
		_, err = ts.db.ExecContext(ctx, `
			INSERT INTO account (id, account_id, provider_id, user_id, password, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			accountID, userEmail, "credential", userID, passwordHash, now, now,
		)
		return err
	}
	if err != nil {
		return err
	}

	// Update existing account
	_, err = ts.db.ExecContext(ctx, `
		UPDATE account
		SET password = $1, updated_at = $2
		WHERE id = $3`,
		passwordHash, time.Now().UTC(), existingID,
	)
	return err
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
