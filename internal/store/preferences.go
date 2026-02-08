package store

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"
)

// GetEmailPreferences gets email preferences for a user, creating defaults if not exists
func (s *PostgresStore) GetEmailPreferences(ctx context.Context, userID string) (*EmailPreferences, error) {
	var prefs EmailPreferences
	var idStr string
	err := s.db.QueryRowContext(ctx, `
		SELECT id, user_id, product_emails, resume_emails, internship_emails, security_emails, created_at, updated_at
		FROM email_preferences
		WHERE user_id = $1`,
		userID,
	).Scan(
		&idStr, &prefs.UserID, &prefs.ProductEmails, &prefs.ResumeEmails,
		&prefs.InternshipEmails, &prefs.SecurityEmails, &prefs.CreatedAt, &prefs.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		// Create default preferences
		return s.CreateDefaultPreferences(ctx, userID)
	}
	if err != nil {
		return nil, err
	}

	prefs.ID, _ = uuid.Parse(idStr)
	return &prefs, nil
}

// CreateDefaultPreferences creates default email preferences for a user
func (s *PostgresStore) CreateDefaultPreferences(ctx context.Context, userID string) (*EmailPreferences, error) {
	id := uuid.New()
	now := time.Now().UTC()
	prefs := &EmailPreferences{
		ID:              id,
		UserID:           userID,
		ProductEmails:    true,
		ResumeEmails:     true,
		InternshipEmails: true,
		SecurityEmails:   true,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO email_preferences (id, user_id, product_emails, resume_emails, internship_emails, security_emails, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		id.String(), userID, true, true, true, true, now, now,
	)
	if err != nil {
		return nil, err
	}

	return prefs, nil
}

// UpdateEmailPreferences updates email preferences for a user
func (s *PostgresStore) UpdateEmailPreferences(ctx context.Context, userID string, prefs *EmailPreferences) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE email_preferences
		SET product_emails = $1, resume_emails = $2, internship_emails = $3, security_emails = $4, updated_at = $5
		WHERE user_id = $6`,
		prefs.ProductEmails, prefs.ResumeEmails, prefs.InternshipEmails, prefs.SecurityEmails,
		time.Now().UTC(), userID,
	)
	return err
}

// GetUserByEmail gets a user by email address
func (s *PostgresStore) GetUserByEmail(ctx context.Context, email string) (*User, error) {
	var user User
	err := s.db.QueryRowContext(ctx, `
		SELECT id, email, name
		FROM "user"
		WHERE email = $1`,
		email,
	).Scan(&user.ID, &user.Email, &user.Name)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return &user, nil
}

// GetUserByID gets a user by ID
func (s *PostgresStore) GetUserByID(ctx context.Context, userID string) (*User, error) {
	var user User
	err := s.db.QueryRowContext(ctx, `
		SELECT id, email, name
		FROM "user"
		WHERE id = $1`,
		userID,
	).Scan(&user.ID, &user.Email, &user.Name)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return &user, nil
}

// HasPasswordAccount checks if user has a credential (password) account
func (s *PostgresStore) HasPasswordAccount(ctx context.Context, userID string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM account
		WHERE user_id = $1 AND provider_id = 'credential' AND password IS NOT NULL`,
		userID,
	).Scan(&count)
	return count > 0, err
}

