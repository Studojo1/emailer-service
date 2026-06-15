package store

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// EmailOpen represents a tracked email open event
type EmailOpen struct {
	ID        uuid.UUID
	TrackID   string
	UserID    string
	EmailType string
	OpenedAt  time.Time
	UserAgent string
}

// RecordEmailOpen inserts an open event. The open pixel's track_id carries the
// recipient EMAIL, so `email` here is an email address. During the identity
// migration we write both the new `email` column and the legacy `user_id`
// column (same value) so existing readers keep working until reads are switched.
func (s *PostgresStore) RecordEmailOpen(ctx context.Context, trackID, email, emailType, userAgent string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO email_opens (id, track_id, user_id, email, email_type, opened_at, user_agent)
		VALUES ($1, $2, $3, $3, $4, $5, $6)
		ON CONFLICT (track_id) DO NOTHING`,
		uuid.New().String(), trackID, email, emailType, time.Now().UTC(), userAgent,
	)
	return err
}
