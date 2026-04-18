package store

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"
)

// ScheduledEmail represents a scheduled nurture email
type ScheduledEmail struct {
	ID          uuid.UUID
	UserID      string
	EmailType   string
	ScheduledAt time.Time
	SentAt      *time.Time
	CreatedAt   time.Time
}

// CreateScheduledEmail inserts a new scheduled email row.
// ON CONFLICT DO NOTHING means a duplicate (user_id, email_type) is silently ignored
// once the unique index is in place — safe to call multiple times.
func (s *PostgresStore) CreateScheduledEmail(ctx context.Context, userID, emailType string, scheduledAt time.Time) error {
	id := uuid.New()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO scheduled_emails (id, user_id, email_type, scheduled_at, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (user_id, email_type) DO NOTHING`,
		id.String(), userID, emailType, scheduledAt, time.Now().UTC(),
	)
	return err
}

// GetDueScheduledEmails returns all emails due to be sent (scheduled_at <= now, not yet sent)
func (s *PostgresStore) GetDueScheduledEmails(ctx context.Context) ([]ScheduledEmail, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, user_id, email_type, scheduled_at, sent_at, created_at
		FROM scheduled_emails
		WHERE scheduled_at <= NOW() AND sent_at IS NULL
		ORDER BY scheduled_at ASC
		LIMIT 20`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var emails []ScheduledEmail
	for rows.Next() {
		var e ScheduledEmail
		var idStr string
		if err := rows.Scan(&idStr, &e.UserID, &e.EmailType, &e.ScheduledAt, &e.SentAt, &e.CreatedAt); err != nil {
			return nil, err
		}
		e.ID, _ = uuid.Parse(idStr)
		emails = append(emails, e)
	}
	return emails, rows.Err()
}

// RecordSentEmail inserts an already-sent email into scheduled_emails for tracking.
// ON CONFLICT DO UPDATE: if a pending row exists for this user+type (e.g. from the
// catchup scheduler), update sent_at so HasReceivedEmail correctly returns true and
// the scheduler does not re-send it.
func (s *PostgresStore) RecordSentEmail(ctx context.Context, userID, emailType string) error {
	id := uuid.New()
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO scheduled_emails (id, user_id, email_type, scheduled_at, sent_at, created_at)
		VALUES ($1, $2, $3, $4, $4, $5)
		ON CONFLICT (user_id, email_type) DO UPDATE SET sent_at = EXCLUDED.sent_at`,
		id.String(), userID, emailType, now, now,
	)
	return err
}

// HasReceivedEmail returns true if the user already received this email type
func (s *PostgresStore) HasReceivedEmail(ctx context.Context, userID, emailType string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM scheduled_emails
		WHERE user_id = $1 AND email_type = $2 AND sent_at IS NOT NULL`,
		userID, emailType,
	).Scan(&count)
	return count > 0, err
}

// HasScheduledOrReceivedEmail returns true if ANY row exists for this user+type
// (sent or still pending). Used to prevent duplicate scheduling on event replays.
func (s *PostgresStore) HasScheduledOrReceivedEmail(ctx context.Context, userID, emailType string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM scheduled_emails
		WHERE user_id = $1 AND email_type = $2`,
		userID, emailType,
	).Scan(&count)
	return count > 0, err
}

// ListUsersWithoutEmail returns users who signed up more than minAgeMinutes ago
// but have never received the given email type and don't have it pending.
func (s *PostgresStore) ListUsersWithoutEmail(ctx context.Context, emailType string, minAgeMinutes int) ([]User, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT u.id, u.email, COALESCE(u.name, '') FROM "user" u
		WHERE u.created_at <= NOW() - ($1 * INTERVAL '1 minute')
		  AND NOT EXISTS (
		    SELECT 1 FROM scheduled_emails se
		    WHERE se.user_id = u.id AND se.email_type = $2
		  )
		ORDER BY u.created_at DESC`,
		minAgeMinutes, emailType,
	)
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

// ListQuizCompletedWithoutEmail returns users who have completed the onboarding quiz
// (candidates.target_roles IS NOT NULL) but never received the given email type.
func (s *PostgresStore) ListQuizCompletedWithoutEmail(ctx context.Context, emailType string) ([]User, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT u.id, u.email, COALESCE(u.name, '') FROM "user" u
		INNER JOIN candidates c ON c.user_id = u.id
		WHERE c.target_roles IS NOT NULL
		  AND NOT EXISTS (
		    SELECT 1 FROM scheduled_emails se
		    WHERE se.user_id = u.id AND se.email_type = $1
		  )
		ORDER BY c.created_at DESC`,
		emailType,
	)
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

// PendingScheduledEmail is a scheduled_emails row with denormalised user info.
type PendingScheduledEmail struct {
	ID          uuid.UUID  `json:"id"`
	UserID      string     `json:"user_id"`
	UserName    string     `json:"user_name"`
	UserEmail   string     `json:"user_email"`
	EmailType   string     `json:"email_type"`
	ScheduledAt time.Time  `json:"scheduled_at"`
	CreatedAt   time.Time  `json:"created_at"`
}

// ListPendingScheduledEmails returns unsent scheduled emails with user info, newest first.
func (s *PostgresStore) ListPendingScheduledEmails(ctx context.Context, limit, offset int) ([]PendingScheduledEmail, int, error) {
	var total int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM scheduled_emails WHERE sent_at IS NULL`).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT se.id, se.user_id, COALESCE(u.name,'') AS user_name,
		       COALESCE(u.email,'') AS user_email,
		       se.email_type, se.scheduled_at, se.created_at
		FROM scheduled_emails se
		LEFT JOIN "user" u ON u.id = se.user_id
		WHERE se.sent_at IS NULL
		ORDER BY se.scheduled_at ASC
		LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var out []PendingScheduledEmail
	for rows.Next() {
		var e PendingScheduledEmail
		var idStr string
		if err := rows.Scan(&idStr, &e.UserID, &e.UserName, &e.UserEmail,
			&e.EmailType, &e.ScheduledAt, &e.CreatedAt); err != nil {
			return nil, 0, err
		}
		e.ID, _ = uuid.Parse(idStr)
		out = append(out, e)
	}
	return out, total, rows.Err()
}

// MarkScheduledEmailSent marks a scheduled email as sent
func (s *PostgresStore) MarkScheduledEmailSent(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `
		UPDATE scheduled_emails SET sent_at = $1 WHERE id = $2 AND sent_at IS NULL`,
		now, id.String(),
	)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}
