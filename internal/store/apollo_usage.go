package store

import (
	"context"
	"database/sql"
)

// GetApolloBurnToday returns today's (IST) Apollo reveal count and whether an
// ops alert has already been sent for the day. The counter is written by
// job-outreach-svc on every successful Apollo /people/match reveal.
func (s *PostgresStore) GetApolloBurnToday(ctx context.Context) (reveals int, alerted bool, err error) {
	err = s.db.QueryRowContext(ctx, `
		SELECT reveals, alerted_at IS NOT NULL
		FROM apollo_usage_daily
		WHERE day = (now() AT TIME ZONE 'Asia/Kolkata')::date
	`).Scan(&reveals, &alerted)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	return reveals, alerted, err
}

// MarkApolloBurnAlerted records that ops were paged for today, so we alert at
// most once per IST day.
func (s *PostgresStore) MarkApolloBurnAlerted(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE apollo_usage_daily SET alerted_at = now()
		WHERE day = (now() AT TIME ZONE 'Asia/Kolkata')::date
	`)
	return err
}
