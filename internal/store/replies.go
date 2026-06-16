package store

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
)

// RecordReply logs an inbound reply from a user. A reply is the highest-intent
// signal we get — it feeds the engagement model and (in the handler) cancels
// pending chases. email_type is best-effort (the flow they were last sent), may
// be empty. Deduped loosely: one reply row per (email, subject) is enough.
func (s *PostgresStore) RecordReply(ctx context.Context, email, subject, emailType string) error {
	email = strings.ToLower(strings.TrimSpace(email))
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO email_replies (id, email, subject, email_type, replied_at)
		VALUES ($1, $2, $3, $4, $5)`,
		uuid.New().String(), email, subject, emailType, time.Now().UTC(),
	)
	return err
}

// HasReplied reports whether this email has ever replied. Cheap indexed lookup.
func (s *PostgresStore) HasReplied(ctx context.Context, email string) (bool, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM email_replies WHERE lower(email) = $1`, email).Scan(&n)
	return n > 0, err
}

// GetReplyCount returns total distinct repliers (for the dashboard).
func (s *PostgresStore) GetReplyCount(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(DISTINCT lower(email)) FROM email_replies`).Scan(&n)
	return n, err
}
