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
// which lives in the same Postgres the emailer connects to. LifeStage is the
// registrant's stated intent, used to pick which intent-funnel email they get.
type WebinarRegistrant struct {
	Email     string
	FullName  string
	LifeStage string
}

// ListWebinarRegistrantsNeedingLink returns registrants who have PAID and have
// NOT yet been sent the join link for the given webinar_date. Idempotent source
// for the cron. DISTINCT ON (lower(email)) dedupes by email and keeps the most
// recent row so we read a single, current life_stage per person.
//
// The paid filter is what makes a ticketed webinar possible: without it this
// query hands the join link to anyone who ever filled in the form, and a paid
// webinar is free to everyone who does not pay. It is applied as
// `COALESCE(r.paid, TRUE)` so a database that predates the column — where the
// frontend has not yet run its ALTER TABLE — keeps the old behaviour of
// emailing every registrant, rather than silently emailing nobody.
func (s *PostgresStore) ListWebinarRegistrantsNeedingLink(ctx context.Context, webinarDate string) ([]WebinarRegistrant, error) {
	paidFilter := "COALESCE(r.paid, TRUE)"
	if !s.webinarHasPaidColumn(ctx) {
		paidFilter = "TRUE"
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT ON (lower(r.email))
		       lower(r.email) AS email,
		       COALESCE(r.full_name, '') AS full_name,
		       COALESCE(r.life_stage, '') AS life_stage
		FROM webinar_registrations r
		WHERE r.email <> ''
		  AND `+paidFilter+`
		  AND NOT EXISTS (
			SELECT 1 FROM webinar_link_sent ls
			WHERE lower(ls.email) = lower(r.email) AND ls.webinar_date = $1::date
		  )
		ORDER BY lower(r.email), r.created_at DESC`, webinarDate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WebinarRegistrant
	for rows.Next() {
		var w WebinarRegistrant
		if rows.Scan(&w.Email, &w.FullName, &w.LifeStage) == nil {
			out = append(out, w)
		}
	}
	return out, nil
}

// webinarHasPaidColumn reports whether webinar_registrations carries the `paid`
// column yet. The column is created by the frontend service, so during a deploy
// where this service ships first the column may not exist and referencing it
// would make the whole cron error out. Checked per call rather than cached: the
// cron runs once a day, so the catalogue lookup costs nothing, and caching a
// false would keep the gate off until the next restart.
func (s *PostgresStore) webinarHasPaidColumn(ctx context.Context) bool {
	var exists bool
	err := s.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_name = 'webinar_registrations' AND column_name = 'paid'
		)`).Scan(&exists)
	if err != nil {
		return false
	}
	return exists
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

// WebinarLinkSentStats summarises how many link emails went out for a date,
// split around a cutoff time. Read-only: used to size a corrected-link re-send
// without touching anything.
type WebinarLinkSentStats struct {
	Total       int        `json:"total"`
	BeforeCut   int        `json:"before_cutoff"`
	AfterCut    int        `json:"after_cutoff"`
	FirstSentAt *time.Time `json:"first_sent_at"`
	LastSentAt  *time.Time `json:"last_sent_at"`
}

// GetWebinarLinkSentStats counts link-sent rows for a webinar date, partitioned
// by a cutoff timestamp (e.g. when the join link was corrected). before_cutoff =
// recipients who got the OLD link; after_cutoff = the corrected one. Pure SELECT.
func (s *PostgresStore) GetWebinarLinkSentStats(ctx context.Context, webinarDate string, cutoff time.Time) (*WebinarLinkSentStats, error) {
	st := &WebinarLinkSentStats{}
	err := s.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*) AS total,
			COUNT(*) FILTER (WHERE sent_at <  $2) AS before_cut,
			COUNT(*) FILTER (WHERE sent_at >= $2) AS after_cut,
			MIN(sent_at) AS first_at,
			MAX(sent_at) AS last_at
		FROM webinar_link_sent
		WHERE webinar_date = $1::date`,
		webinarDate, cutoff,
	).Scan(&st.Total, &st.BeforeCut, &st.AfterCut, &st.FirstSentAt, &st.LastSentAt)
	if err != nil {
		return nil, err
	}
	return st, nil
}

// CountWebinarRegistrants returns the total distinct registrants (for the dashboard).
func (s *PostgresStore) CountWebinarRegistrants(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(DISTINCT lower(email)) FROM webinar_registrations WHERE email <> ''`).Scan(&n)
	return n, err
}

// Webinar is one webinar with its registrant count, for the dashboard list.
type Webinar struct {
	ID          int        `json:"id"`
	Title       string     `json:"title"`
	WebinarDate *time.Time `json:"webinar_date"`
	WebinarTime string     `json:"webinar_time"`
	Status      string     `json:"status"`      // 'upcoming' | 'conducted'
	Registrants int        `json:"registrants"` // distinct registrants for this webinar
	CreatedAt   *time.Time `json:"created_at"`
}

// ListWebinars returns every webinar with its distinct-registrant count, newest
// first. Powers the dashboard "webinars conducted + per-webinar counts" view.
func (s *PostgresStore) ListWebinars(ctx context.Context) ([]Webinar, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT w.id, w.title, w.webinar_date, w.webinar_time, w.status, w.created_at,
		       COUNT(DISTINCT lower(r.email)) FILTER (WHERE r.email <> '') AS registrants
		FROM webinars w
		LEFT JOIN webinar_registrations r ON r.webinar_id = w.id
		GROUP BY w.id
		ORDER BY w.id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Webinar
	for rows.Next() {
		var w Webinar
		if err := rows.Scan(&w.ID, &w.Title, &w.WebinarDate, &w.WebinarTime, &w.Status, &w.CreatedAt, &w.Registrants); err == nil {
			out = append(out, w)
		}
	}
	return out, nil
}

// CountWebinarsByStatus returns how many webinars are conducted vs upcoming.
func (s *PostgresStore) CountWebinarsByStatus(ctx context.Context) (conducted, upcoming int, err error) {
	err = s.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE status = 'conducted'),
			COUNT(*) FILTER (WHERE status = 'upcoming')
		FROM webinars`).Scan(&conducted, &upcoming)
	return
}
