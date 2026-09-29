package store

import (
	"context"
	"strings"
)

// Marketing opt-outs (Privacy Policy v2.0 §14: every marketing email has an
// unsubscribe link). Table marketing_opt_outs is created in cmd/server/main.go.
//
// One row per opt-out, keyed by user id and/or address, so a person we only
// know by email (a webinar registrant) can opt out too. email_preferences.
// product_emails stays in sync for account holders because the settings page
// and older code read it.

// RecordMarketingOptOut records that a user id and/or address no longer wants
// marketing email. Idempotent. When only an address is given it is matched to
// an account so the account's preferences flip too; when only a user id is
// given the account's address is stored as well. Service-email preferences
// (resume, internship, security) are left alone: an unsubscribe from tips and
// offers must not stop receipts and confirmations.
func (s *PostgresStore) RecordMarketingOptOut(ctx context.Context, userID, email, source string) error {
	email = strings.ToLower(strings.TrimSpace(email))
	if userID == "" && email != "" {
		if u, err := s.GetUserByEmail(ctx, email); err == nil && u != nil {
			userID = u.ID
		}
	}
	if email == "" && userID != "" {
		if u, err := s.GetUserByID(ctx, userID); err == nil && u != nil {
			email = strings.ToLower(strings.TrimSpace(u.Email))
		}
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO marketing_opt_outs (user_id, email, source, opted_out_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT DO NOTHING`,
		userID, email, source,
	); err != nil {
		return err
	}
	if userID != "" {
		return s.UnsubscribeUser(ctx, userID)
	}
	return nil
}

// IsMarketingOptedOut reports whether a user id or address has opted out of
// marketing, either through an unsubscribe link (marketing_opt_outs) or the
// product-emails switch in settings (email_preferences).
func (s *PostgresStore) IsMarketingOptedOut(ctx context.Context, userID, email string) (bool, error) {
	var out bool
	err := s.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM marketing_opt_outs
			WHERE ($1 <> '' AND user_id = $1)
			   OR ($2 <> '' AND email = $2)
		) OR EXISTS (
			SELECT 1 FROM email_preferences
			WHERE $1 <> '' AND user_id = $1 AND product_emails = false
		)`,
		userID, strings.ToLower(strings.TrimSpace(email)),
	).Scan(&out)
	return out, err
}

// ClearMarketingOptOut removes a user's opt-out rows (by id and by their
// current address). Called when they switch product emails back on in
// settings, otherwise the old unsubscribe would keep blocking them.
func (s *PostgresStore) ClearMarketingOptOut(ctx context.Context, userID string) error {
	email := ""
	if u, err := s.GetUserByID(ctx, userID); err == nil && u != nil {
		email = strings.ToLower(strings.TrimSpace(u.Email))
	}
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM marketing_opt_outs
		WHERE user_id = $1 OR ($2 <> '' AND email = $2)`,
		userID, email,
	)
	return err
}
