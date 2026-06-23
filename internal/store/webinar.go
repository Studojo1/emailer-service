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

// ListWebinarRegistrantsNeedingLink returns registrants who have NOT yet been
// sent the join link for the given webinar_date. Idempotent source for the cron.
// DISTINCT ON (lower(email)) dedupes by email and keeps the most recent row so
// we read a single, current life_stage per person.
func (s *PostgresStore) ListWebinarRegistrantsNeedingLink(ctx context.Context, webinarDate string) ([]WebinarRegistrant, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT ON (lower(r.email))
		       lower(r.email) AS email,
		       COALESCE(r.full_name, '') AS full_name,
		       COALESCE(r.life_stage, '') AS life_stage
		FROM webinar_registrations r
		WHERE r.email <> ''
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

// Webinar is one webinar with its registrant count, for the dashboard list.
type Webinar struct {
	ID          int        `json:"id"`
	Title       string     `json:"title"`
	WebinarDate *time.Time `json:"webinar_date"`
	WebinarTime string     `json:"webinar_time"`
	Status      string     `json:"status"`       // 'upcoming' | 'conducted'
	Registrants int        `json:"registrants"`  // distinct registrants for this webinar
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

// ActiveWebinar is the currently-active (status='upcoming') webinar plus its
// join link. Returned by GetActiveWebinar; nil if there is no active webinar.
type ActiveWebinar struct {
	ID      int
	Title   string
	JoinURL string
	When    string // human "date · time" for the email, may be empty
}

// GetActiveWebinar returns the single active ('upcoming') webinar, newest first
// if more than one. Returns (nil, nil) when none is active.
func (s *PostgresStore) GetActiveWebinar(ctx context.Context) (*ActiveWebinar, error) {
	var (
		a    ActiveWebinar
		date *time.Time
		tm   string
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT id, title, join_url, webinar_date, webinar_time
		FROM webinars WHERE status = 'upcoming'
		ORDER BY created_at DESC, id DESC LIMIT 1`).
		Scan(&a.ID, &a.Title, &a.JoinURL, &date, &tm)
	if err != nil {
		return nil, nil // no active webinar
	}
	if date != nil {
		a.When = date.Format("Mon, 02 Jan 2006")
	}
	if tm != "" {
		if a.When != "" {
			a.When += " · " + tm
		} else {
			a.When = tm
		}
	}
	return &a, nil
}

// SetActiveWebinarJoinURL sets the join link (and optional date/time) on a
// webinar row. Used by the dashboard "save details" action.
func (s *PostgresStore) SetActiveWebinarJoinURL(ctx context.Context, webinarID int, joinURL, webinarDate, webinarTime string) error {
	var datePtr interface{}
	if webinarDate != "" {
		datePtr = webinarDate
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE webinars
		SET join_url = $2, webinar_date = $3, webinar_time = $4
		WHERE id = $1`,
		webinarID, joinURL, datePtr, webinarTime,
	)
	return err
}

// HasWebinarLinkSent reports whether this email already got the join link for
// this webinar (per-webinar dedup).
func (s *PostgresStore) HasWebinarLinkSent(ctx context.Context, email string, webinarID int) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM webinar_link_sent
		WHERE lower(email) = lower($1) AND webinar_id = $2`, email, webinarID).Scan(&n)
	return n > 0, err
}

// MarkWebinarLinkSentForWebinar records that an email got the join link for a
// specific webinar (per-webinar dedup). webinar_date is kept for the legacy PK.
func (s *PostgresStore) MarkWebinarLinkSentForWebinar(ctx context.Context, email string, webinarID int) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO webinar_link_sent (email, webinar_date, webinar_id, sent_at)
		VALUES (lower($1), COALESCE((SELECT webinar_date FROM webinars WHERE id = $2), CURRENT_DATE), $2, NOW())
		ON CONFLICT (email, webinar_date) DO UPDATE SET webinar_id = EXCLUDED.webinar_id`,
		email, webinarID,
	)
	return err
}

// ListActiveWebinarRegistrantsNeedingLink returns registrants of the given
// webinar who have not yet been sent its join link. Source for the backfill.
func (s *PostgresStore) ListActiveWebinarRegistrantsNeedingLink(ctx context.Context, webinarID int) ([]WebinarRegistrant, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT ON (lower(r.email))
		       lower(r.email) AS email,
		       COALESCE(r.full_name, '') AS full_name,
		       COALESCE(r.life_stage, '') AS life_stage
		FROM webinar_registrations r
		WHERE r.email <> '' AND r.webinar_id = $1
		  AND NOT EXISTS (
			SELECT 1 FROM webinar_link_sent ls
			WHERE lower(ls.email) = lower(r.email) AND ls.webinar_id = $1
		  )
		ORDER BY lower(r.email), r.created_at DESC`, webinarID)
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

// CountWebinarsByStatus returns how many webinars are conducted vs upcoming.
func (s *PostgresStore) CountWebinarsByStatus(ctx context.Context) (conducted, upcoming int, err error) {
	err = s.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE status = 'conducted'),
			COUNT(*) FILTER (WHERE status = 'upcoming')
		FROM webinars`).Scan(&conducted, &upcoming)
	return
}
