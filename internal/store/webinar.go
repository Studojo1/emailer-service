package store

import (
	"context"
	"time"
)

// WebinarConfig is the single-row admin-set config for the upcoming webinar.
type WebinarConfig struct {
	Title       string     `json:"title"`
	WebinarDate *time.Time `json:"webinar_date"` // nil if unset
	WebinarTime string     `json:"webinar_time"`
	JoinURL     string     `json:"join_url"`
	UpdatedAt   *time.Time `json:"updated_at"`
}

// GetWebinarConfig returns the current webinar config (zero-value if unset).
func (s *PostgresStore) GetWebinarConfig(ctx context.Context) (*WebinarConfig, error) {
	c := &WebinarConfig{}
	err := s.db.QueryRowContext(ctx, `
		SELECT title, webinar_date, webinar_time, join_url, updated_at
		FROM webinar_config WHERE id = 1`).
		Scan(&c.Title, &c.WebinarDate, &c.WebinarTime, &c.JoinURL, &c.UpdatedAt)
	if err != nil {
		// No row yet — return empty config, not an error.
		return c, nil
	}
	return c, nil
}

// SetWebinarConfig upserts the single config row. webinarDate may be "" to clear.
func (s *PostgresStore) SetWebinarConfig(ctx context.Context, title, webinarDate, webinarTime, joinURL string) error {
	var datePtr interface{}
	if webinarDate != "" {
		datePtr = webinarDate
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO webinar_config (id, title, webinar_date, webinar_time, join_url, updated_at)
		VALUES (1, $1, $2, $3, $4, NOW())
		ON CONFLICT (id) DO UPDATE SET
			title = EXCLUDED.title,
			webinar_date = EXCLUDED.webinar_date,
			webinar_time = EXCLUDED.webinar_time,
			join_url = EXCLUDED.join_url,
			updated_at = NOW()`,
		title, datePtr, webinarTime, joinURL,
	)
	return err
}

// WebinarRegistrant is a row from the frontend's webinar_registrations table,
// which lives in the same Postgres the emailer connects to.
type WebinarRegistrant struct {
	Email    string
	FullName string
}

// ListWebinarRegistrantsNeedingLink returns registrants who have NOT yet been
// sent the join link for the given webinar_date. Idempotent source for the cron.
func (s *PostgresStore) ListWebinarRegistrantsNeedingLink(ctx context.Context, webinarDate string) ([]WebinarRegistrant, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT lower(r.email) AS email, COALESCE(r.full_name, '') AS full_name
		FROM webinar_registrations r
		WHERE r.email <> ''
		  AND NOT EXISTS (
			SELECT 1 FROM webinar_link_sent ls
			WHERE lower(ls.email) = lower(r.email) AND ls.webinar_date = $1::date
		  )`, webinarDate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WebinarRegistrant
	for rows.Next() {
		var w WebinarRegistrant
		if rows.Scan(&w.Email, &w.FullName) == nil {
			out = append(out, w)
		}
	}
	return out, nil
}

// MarkWebinarLinkSent records that a registrant got the join link for a date.
func (s *PostgresStore) MarkWebinarLinkSent(ctx context.Context, email, webinarDate string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO webinar_link_sent (email, webinar_date, sent_at)
		VALUES (lower($1), $2::date, NOW())
		ON CONFLICT (email, webinar_date) DO NOTHING`,
		email, webinarDate,
	)
	return err
}

// CountWebinarRegistrants returns the total distinct registrants (for the dashboard).
func (s *PostgresStore) CountWebinarRegistrants(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(DISTINCT lower(email)) FROM webinar_registrations WHERE email <> ''`).Scan(&n)
	return n, err
}
