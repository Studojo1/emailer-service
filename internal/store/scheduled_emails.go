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

// CreateScheduledEmail inserts a new scheduled email row
func (s *PostgresStore) CreateScheduledEmail(ctx context.Context, userID, emailType string, scheduledAt time.Time) error {
	id := uuid.New()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO scheduled_emails (id, user_id, email_type, scheduled_at, created_at)
		VALUES ($1, $2, $3, $4, $5)`,
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
		LIMIT 100`,
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
