package store

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
)

// RecordEmailClick inserts a click event for a tracked CTA link. Mirrors
// RecordEmailOpen. track_id format matches the open pixel:
// {emailType}__{email}__{uuid}.
func (s *PostgresStore) RecordEmailClick(ctx context.Context, trackID, email, emailType, userAgent string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO email_clicks (id, track_id, email, email_type, clicked_at, user_agent)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (track_id) DO NOTHING`,
		uuid.New().String(), trackID, email, emailType, time.Now().UTC(), userAgent,
	)
	return err
}

// HasEngagedWithWelcome reports whether the user (by email) has engaged with the
// given welcome email — i.e. opened it OR clicked a CTA in it. This is the
// engagement signal that gates the "not used" chase: an engaged user must not be
// enrolled in (or kept in) the chase sequence.
//
// Product-action usage (e.g. event.cc.outreach_used) cancels the chase separately
// in the event handler, so this only needs to cover the email-engagement half.
func (s *PostgresStore) HasEngagedWithWelcome(ctx context.Context, email, welcomeType string) (bool, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	var n int
	err := s.db.QueryRowContext(ctx, `
		SELECT
		  (SELECT COUNT(DISTINCT lower(user_id)) FROM email_opens  WHERE lower(user_id) = $1 AND email_type = $2)
		+ (SELECT COUNT(DISTINCT lower(email))   FROM email_clicks WHERE lower(email)   = $1 AND email_type = $2)`,
		email, welcomeType,
	).Scan(&n)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}
