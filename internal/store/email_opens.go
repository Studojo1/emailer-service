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

// RecordEmailOpen inserts an open event
func (s *PostgresStore) RecordEmailOpen(ctx context.Context, trackID, userID, emailType, userAgent string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO email_opens (id, track_id, user_id, email_type, opened_at, user_agent)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (track_id) DO NOTHING`,
		uuid.New().String(), trackID, userID, emailType, time.Now().UTC(), userAgent,
	)
	return err
}
