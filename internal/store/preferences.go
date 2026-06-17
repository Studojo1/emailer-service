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

// GetUserByEmail gets a user by email address (case-insensitive)
func (s *PostgresStore) GetUserByEmail(ctx context.Context, email string) (*User, error) {
	// First, try exact match
	var user User
	err := s.db.QueryRowContext(ctx, `
		SELECT id, email, name
		FROM "user"
		WHERE LOWER(TRIM(email)) = LOWER(TRIM($1))`,
		email,
	).Scan(&user.ID, &user.Email, &user.Name)

	if err == sql.ErrNoRows {
		// Log for debugging - check if user exists with different casing/whitespace
		var allEmails []string
		rows, _ := s.db.QueryContext(ctx, `SELECT email FROM "user"`)
		if rows != nil {
			for rows.Next() {
				var e string
				if rows.Scan(&e) == nil {
					allEmails = append(allEmails, e)
				}
			}
			rows.Close()
		}
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

// ListUsersBySignupDate returns users who signed up within the given number of days (0 = all users)
func (s *PostgresStore) ListUsersBySignupDate(ctx context.Context, withinDays int) ([]User, error) {
	var query string
	var args []interface{}

	if withinDays > 0 {
		query = `SELECT id, email, COALESCE(name, '') FROM "user" WHERE created_at >= NOW() - INTERVAL '1 day' * $1 ORDER BY created_at DESC`
		args = []interface{}{withinDays}
	} else {
		query = `SELECT id, email, COALESCE(name, '') FROM "user" ORDER BY created_at DESC`
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.Email, &u.Name); err != nil {
			return nil, err
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

// ListRecentUsers returns the most recent N users by signup date (created_at
// DESC, LIMIT n). Used for "last N users" blasts (e.g. a pricing announcement
// to the last 700 signups). limit <= 0 returns all users.
func (s *PostgresStore) ListRecentUsers(ctx context.Context, limit int) ([]User, error) {
	query := `SELECT id, email, COALESCE(name, '') FROM "user" WHERE email <> '' ORDER BY created_at DESC`
	var args []interface{}
	if limit > 0 {
		query += ` LIMIT $1`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var users []User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.Email, &u.Name); err != nil {
			return nil, err
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

// CountRecentUsers returns how many users ListRecentUsers(limit) would target
// (capped at limit, for the dashboard preview).
func (s *PostgresStore) CountRecentUsers(ctx context.Context, limit int) (int, error) {
	var total int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM "user" WHERE email <> ''`).Scan(&total); err != nil {
		return 0, err
	}
	if limit > 0 && total > limit {
		return limit, nil
	}
	return total, nil
}

// MissedSignup is a user who signed up but never received ANY cc-* email — a
// silent failure (their welcome/flow never fired).
type MissedSignup struct {
	ID        string `json:"id"`
	Email     string `json:"email"`
	Name      string `json:"name"`
	CreatedAt string `json:"created_at"`
}

// CountSignupsWithNoCCEmail counts users who signed up (older than graceMinutes,
// so a just-now signup mid-send isn't falsely flagged) and have NO cc-* row in
// email_send_log — i.e. the system never emailed them at all. Checked against
// email_send_log (the authoritative send record), matched by email.
func (s *PostgresStore) CountSignupsWithNoCCEmail(ctx context.Context, graceMinutes int) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM "user" u
		WHERE u.email <> ''
		  AND u.created_at <= NOW() - ($1 * INTERVAL '1 minute')
		  AND NOT EXISTS (
		    SELECT 1 FROM email_send_log l
		    WHERE lower(l.email_to) = lower(u.email) AND l.template_name LIKE 'cc-%'
		  )`, graceMinutes).Scan(&n)
	return n, err
}

// ListSignupsWithNoCCEmail returns the users counted by CountSignupsWithNoCCEmail,
// newest first, capped by limit.
func (s *PostgresStore) ListSignupsWithNoCCEmail(ctx context.Context, graceMinutes, limit int) ([]MissedSignup, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT u.id, u.email, COALESCE(u.name,''), to_char(u.created_at, 'YYYY-MM-DD HH24:MI')
		FROM "user" u
		WHERE u.email <> ''
		  AND u.created_at <= NOW() - ($1 * INTERVAL '1 minute')
		  AND NOT EXISTS (
		    SELECT 1 FROM email_send_log l
		    WHERE lower(l.email_to) = lower(u.email) AND l.template_name LIKE 'cc-%'
		  )
		ORDER BY u.created_at DESC
		LIMIT $2`, graceMinutes, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MissedSignup
	for rows.Next() {
		var m MissedSignup
		if err := rows.Scan(&m.ID, &m.Email, &m.Name, &m.CreatedAt); err == nil {
			out = append(out, m)
		}
	}
	return out, rows.Err()
}

// ListUsersAtOrderStage returns distinct users who have at least one outreach_order
// at the given status (e.g. "leads_ready").
func (s *PostgresStore) ListUsersAtOrderStage(ctx context.Context, stage string) ([]User, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT u.id, u.email, COALESCE(u.name, '')
		FROM "user" u
		INNER JOIN outreach_orders oo ON oo.user_id = u.id
		WHERE oo.status = $1
		ORDER BY u.id
	`, stage)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.Email, &u.Name); err != nil {
			return nil, err
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

// CountUsersAtOrderStage returns the number of distinct users with an outreach_order
// at the given status.
func (s *PostgresStore) CountUsersAtOrderStage(ctx context.Context, stage string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(DISTINCT user_id) FROM outreach_orders WHERE status = $1
	`, stage).Scan(&count)
	return count, err
}

// CountUsersBySignupDate returns count of users who signed up within the given number of days (0 = all)
func (s *PostgresStore) CountUsersBySignupDate(ctx context.Context, withinDays int) (int, error) {
	var query string
	var args []interface{}

	if withinDays > 0 {
		query = `SELECT COUNT(*) FROM "user" WHERE created_at >= NOW() - INTERVAL '1 day' * $1`
		args = []interface{}{withinDays}
	} else {
		query = `SELECT COUNT(*) FROM "user"`
	}

	var count int
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&count)
	return count, err
}

// UnsubscribeUser sets all marketing email preferences to false for a user.
// Uses upsert so it works even when no preferences row exists yet.
func (s *PostgresStore) UnsubscribeUser(ctx context.Context, userID string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO email_preferences (id, user_id, product_emails, resume_emails, internship_emails, security_emails, created_at, updated_at)
		VALUES (gen_random_uuid(), $1, false, false, false, true, NOW(), NOW())
		ON CONFLICT (user_id) DO UPDATE
		SET product_emails = false, resume_emails = false, internship_emails = false, updated_at = NOW()`,
		userID,
	)
	return err
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

