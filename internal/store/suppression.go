package store

import (
	"context"
	"strings"
)

// SuppressEmail adds an address to the suppression list so no further mail is
// sent to it. Used for hard bounces (invalid/nonexistent mailbox) and spam
// complaints reported by the provider. Idempotent: re-suppressing updates the
// reason/timestamp. The address is normalised (lower/trim) to match how sends
// look it up.
func (s *PostgresStore) SuppressEmail(ctx context.Context, email, reason string) error {
	email = strings.ToLower(strings.TrimSpace(email))
	if email == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO suppressed_emails (email, reason, suppressed_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (email) DO UPDATE
		SET reason = EXCLUDED.reason, suppressed_at = EXCLUDED.suppressed_at`,
		email, reason,
	)
	return err
}

// IsEmailSuppressed reports whether an address is on the suppression list. The
// send path calls this before every send so a bounced/complained address is
// never mailed again (protecting domain reputation).
func (s *PostgresStore) IsEmailSuppressed(ctx context.Context, email string) (bool, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	if email == "" {
		return false, nil
	}
	var exists bool
	err := s.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM suppressed_emails WHERE email = $1)`, email,
	).Scan(&exists)
	return exists, err
}
