package store

import (
	"context"
	"fmt"
	"time"
)

// EmailLog is a unified send record
type EmailLog struct {
	ID           string     `json:"id"`
	UserID       string     `json:"user_id"`
	UserName     string     `json:"user_name"`
	EmailTo      string     `json:"email_to"`
	TemplateName string     `json:"template_name"`
	FromAddress  string     `json:"from_address"`
	Status       string     `json:"status"`
	SentAt       time.Time  `json:"sent_at"`
	OpenedAt     *time.Time `json:"opened_at,omitempty"`
}

// CampaignGroup groups all sends of a given email_type together
type CampaignGroup struct {
	EmailType  string     `json:"email_type"`
	TotalSent  int        `json:"total_sent"`
	TotalOpened int       `json:"total_opened"`
	OpenRate   float64    `json:"open_rate"`
	FirstSent  *time.Time `json:"first_sent"`
	LastSent   *time.Time `json:"last_sent"`
}

// EmailStats is the top-level stats object for the admin dashboard
type EmailStats struct {
	TotalSent    int              `json:"total_sent"`
	TotalOpened  int              `json:"total_opened"`
	OpenRate     float64          `json:"open_rate"`
	SentToday    int              `json:"sent_today"`
	SentThisWeek int              `json:"sent_this_week"`
	TotalUsers   int              `json:"total_users"`
	TopTemplates []TemplateStats  `json:"top_templates"`
	DailyVolume  []DailyVolume    `json:"daily_volume"`
}

// TemplateStats holds per-template send/open counts
type TemplateStats struct {
	TemplateName string  `json:"template_name"`
	SendCount    int     `json:"send_count"`
	OpenCount    int     `json:"open_count"`
	OpenRate     float64 `json:"open_rate"`
}

// DailyVolume holds emails sent per day
type DailyVolume struct {
	Date  string `json:"date"`
	Count int    `json:"count"`
}

// SignupWindows holds signup counts over rolling windows.
type SignupWindows struct {
	Total   int `json:"total"`
	Last2d  int `json:"last_2d"`
	Last4d  int `json:"last_4d"`
	Last7d  int `json:"last_7d"`
	Last30d int `json:"last_30d"`
}

// FlowEntryRow is per-flow entry counts (people who got the flow's FIRST email)
// over the same rolling windows, for the signups dashboard.
type FlowEntryRow struct {
	Template string `json:"template"` // first-step template (identifies the flow)
	Total    int    `json:"total"`
	Last2d   int    `json:"last_2d"`
	Last7d   int    `json:"last_7d"`
	Last30d  int    `json:"last_30d"`
}

// GetSignupStats returns signup counts by window (from "user".created_at) plus
// per-flow entry counts (first-step template sends from email_send_log) by window.
func (s *PostgresStore) GetSignupStats(ctx context.Context, firstStepTemplates []string) (*SignupWindows, []FlowEntryRow, error) {
	w := &SignupWindows{}
	err := s.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '2 days'),
			COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '4 days'),
			COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days'),
			COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '30 days')
		FROM "user"`).Scan(&w.Total, &w.Last2d, &w.Last4d, &w.Last7d, &w.Last30d)
	if err != nil {
		return nil, nil, fmt.Errorf("signup windows: %w", err)
	}

	rows := []FlowEntryRow{}
	for _, tpl := range firstStepTemplates {
		var r = FlowEntryRow{Template: tpl}
		_ = s.db.QueryRowContext(ctx, `
			SELECT
				COUNT(*),
				COUNT(*) FILTER (WHERE sent_at >= NOW() - INTERVAL '2 days'),
				COUNT(*) FILTER (WHERE sent_at >= NOW() - INTERVAL '7 days'),
				COUNT(*) FILTER (WHERE sent_at >= NOW() - INTERVAL '30 days')
			FROM email_send_log WHERE template_name = $1`, tpl).
			Scan(&r.Total, &r.Last2d, &r.Last7d, &r.Last30d)
		rows = append(rows, r)
	}
	return w, rows, nil
}

// UserWithStats is a user row enriched with email activity counts
type UserWithStats struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Email        string    `json:"email"`
	CreatedAt    time.Time `json:"created_at"`
	EmailsSent   int       `json:"emails_sent"`
	EmailsOpened int       `json:"emails_opened"`
	LastTemplate string    `json:"last_template"` // most recent template sent — drives the flow colour
}

// LogEmailSent inserts a row into email_send_log
func (s *PostgresStore) LogEmailSent(ctx context.Context, userID, userName, emailTo, templateName, fromAddress string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO email_send_log (user_id, user_name, email_to, template_name, from_address, status, sent_at)
		VALUES ($1, $2, $3, $4, $5, 'sent', NOW())
	`, userID, userName, emailTo, templateName, fromAddress)
	return err
}

// MarkEmailOpened sets opened_at on the most recent matching send log row
func (s *PostgresStore) MarkEmailOpened(ctx context.Context, emailTo, templateName string) {
	_, _ = s.db.ExecContext(ctx, `
		UPDATE email_send_log
		SET opened_at = NOW()
		WHERE id = (
			SELECT id FROM email_send_log
			WHERE email_to = $1 AND template_name = $2 AND opened_at IS NULL
			ORDER BY sent_at DESC
			LIMIT 1
		)
	`, emailTo, templateName)
}

// GetEmailStats returns aggregate stats. email_send_log is the source of truth
// for counts and daily volume — every send goes through SendTemplateEmail which
// always calls LogEmailSent. scheduled_emails is used only for scheduled counts.
func (s *PostgresStore) GetEmailStats(ctx context.Context) (*EmailStats, error) {
	stats := &EmailStats{}

	// Primary counts from email_send_log (captures every send, including
	// transactional emails that never touch scheduled_emails)
	row := s.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*)                                                         AS total_sent,
			COUNT(*) FILTER (WHERE sent_at >= NOW() - INTERVAL '24 hours')  AS sent_today,
			COUNT(*) FILTER (WHERE sent_at >= NOW() - INTERVAL '7 days')    AS sent_this_week,
			COUNT(*) FILTER (WHERE opened_at IS NOT NULL)                   AS total_opened
		FROM email_send_log
	`)
	if err := row.Scan(&stats.TotalSent, &stats.SentToday, &stats.SentThisWeek, &stats.TotalOpened); err != nil {
		return nil, fmt.Errorf("aggregate stats: %w", err)
	}
	if stats.TotalSent > 0 {
		stats.OpenRate = float64(stats.TotalOpened) / float64(stats.TotalSent) * 100
	}

	// Total registered users
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM "user"`).Scan(&stats.TotalUsers)

	// Top templates by send count (from email_send_log)
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			template_name,
			COUNT(*)                                          AS send_count,
			COUNT(*) FILTER (WHERE opened_at IS NOT NULL)    AS open_count
		FROM email_send_log
		GROUP BY template_name
		ORDER BY send_count DESC
		LIMIT 8
	`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var ts TemplateStats
			if err := rows.Scan(&ts.TemplateName, &ts.SendCount, &ts.OpenCount); err == nil {
				if ts.SendCount > 0 {
					ts.OpenRate = float64(ts.OpenCount) / float64(ts.SendCount) * 100
				}
				stats.TopTemplates = append(stats.TopTemplates, ts)
			}
		}
	}

	// Daily volume — last 14 days, one row per day, from email_send_log.
	// Always returns exactly 14 rows (missing days filled with 0 in the query).
	drows, err := s.db.QueryContext(ctx, `
		WITH days AS (
			SELECT generate_series(
				(NOW() - INTERVAL '13 days')::date,
				NOW()::date,
				INTERVAL '1 day'
			)::date AS day
		)
		SELECT
			TO_CHAR(d.day, 'YYYY-MM-DD'),
			COALESCE(c.cnt, 0)
		FROM days d
		LEFT JOIN (
			SELECT sent_at::date AS day, COUNT(*) AS cnt
			FROM email_send_log
			WHERE sent_at >= NOW() - INTERVAL '14 days'
			GROUP BY sent_at::date
		) c ON c.day = d.day
		ORDER BY d.day
	`)
	if err == nil {
		defer drows.Close()
		for drows.Next() {
			var dv DailyVolume
			if err := drows.Scan(&dv.Date, &dv.Count); err == nil {
				stats.DailyVolume = append(stats.DailyVolume, dv)
			}
		}
	}

	return stats, nil
}

// ListEmailLogs returns all sends ever, joining scheduled_emails with user table.
// Opens are enriched from email_send_log where available.
func (s *PostgresStore) ListEmailLogs(ctx context.Context, limit, offset int, search string) ([]EmailLog, int, error) {
	var total int
	args := []interface{}{}
	where := "WHERE se.sent_at IS NOT NULL"
	idx := 1

	if search != "" {
		where += fmt.Sprintf(
			` AND (u.email ILIKE $%d OR se.email_type ILIKE $%d OR u.name ILIKE $%d)`,
			idx, idx+1, idx+2,
		)
		p := "%" + search + "%"
		args = append(args, p, p, p)
		idx += 3
	}

	countQ := `
		SELECT COUNT(*)
		FROM scheduled_emails se
		LEFT JOIN "user" u ON u.id = se.user_id
		` + where
	if err := s.db.QueryRowContext(ctx, countQ, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	args = append(args, limit, offset)
	q := fmt.Sprintf(`
		SELECT
			se.id,
			se.user_id,
			COALESCE(u.name, '')  AS user_name,
			COALESCE(u.email, '') AS email_to,
			se.email_type         AS template_name,
			''                    AS from_address,
			'sent'                AS status,
			se.sent_at,
			esl.opened_at
		FROM scheduled_emails se
		LEFT JOIN "user" u ON u.id = se.user_id
		LEFT JOIN LATERAL (
			SELECT opened_at FROM email_send_log
			WHERE email_to = u.email AND template_name = se.email_type
			  AND opened_at IS NOT NULL
			ORDER BY sent_at DESC
			LIMIT 1
		) esl ON true
		%s
		ORDER BY se.sent_at DESC
		LIMIT $%d OFFSET $%d
	`, where, idx, idx+1)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var logs []EmailLog
	for rows.Next() {
		var l EmailLog
		if err := rows.Scan(&l.ID, &l.UserID, &l.UserName, &l.EmailTo, &l.TemplateName,
			&l.FromAddress, &l.Status, &l.SentAt, &l.OpenedAt); err == nil {
			logs = append(logs, l)
		}
	}
	return logs, total, nil
}

// GetCampaignGroups returns sends grouped by email_type — one row per campaign type
func (s *PostgresStore) GetCampaignGroups(ctx context.Context) ([]CampaignGroup, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			email_type,
			COUNT(*)     AS total_sent,
			MIN(sent_at) AS first_sent,
			MAX(sent_at) AS last_sent
		FROM scheduled_emails
		WHERE sent_at IS NOT NULL
		GROUP BY email_type
		ORDER BY last_sent DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var groups []CampaignGroup
	for rows.Next() {
		var g CampaignGroup
		if err := rows.Scan(&g.EmailType, &g.TotalSent, &g.FirstSent, &g.LastSent); err == nil {
			// Enrich with open count
			_ = s.db.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM email_send_log WHERE template_name = $1 AND opened_at IS NOT NULL`,
				g.EmailType).Scan(&g.TotalOpened)
			if g.TotalSent > 0 {
				g.OpenRate = float64(g.TotalOpened) / float64(g.TotalSent) * 100
			}
			groups = append(groups, g)
		}
	}
	return groups, nil
}

// TemplateStat is per-template send + open counts from the authoritative send log.
type TemplateStat struct {
	Sent    int `json:"sent"`
	Opened  int `json:"opened"`
	Clicked int `json:"clicked"`
}

// GetSendCountsByTemplate returns sent + opened + clicked counts per template.
// Sends come from email_send_log (every actual send, keyed by hyphen template
// name). Opens come from the email_opens table and clicks from email_clicks
// (both keyed by email_type = the same hyphen template name) — these are the
// authoritative engagement tables, counted by distinct recipient so a single
// user opening twice counts once.
func (s *PostgresStore) GetSendCountsByTemplate(ctx context.Context) (map[string]TemplateStat, error) {
	out := map[string]TemplateStat{}

	rows, err := s.db.QueryContext(ctx, `
		SELECT template_name, COUNT(*) AS sent FROM email_send_log GROUP BY template_name`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name string
		var st TemplateStat
		if err := rows.Scan(&name, &st.Sent); err == nil {
			out[name] = st
		}
	}
	rows.Close()

	// Opens — distinct recipients per template.
	oRows, err := s.db.QueryContext(ctx, `
		SELECT email_type, COUNT(DISTINCT user_id) FROM email_opens GROUP BY email_type`)
	if err == nil {
		for oRows.Next() {
			var t string
			var n int
			if err := oRows.Scan(&t, &n); err == nil {
				st := out[t]
				st.Opened = n
				out[t] = st
			}
		}
		oRows.Close()
	}

	// Clicks — distinct recipients per template.
	cRows, err := s.db.QueryContext(ctx, `
		SELECT email_type, COUNT(DISTINCT email) FROM email_clicks GROUP BY email_type`)
	if err == nil {
		for cRows.Next() {
			var t string
			var n int
			if err := cRows.Scan(&t, &n); err == nil {
				st := out[t]
				st.Clicked = n
				out[t] = st
			}
		}
		cRows.Close()
	}

	return out, nil
}

// GetPendingByType returns the count of not-yet-sent scheduled emails per
// email_type (underscore form, e.g. cc_outreach_nudge_d1) — i.e. how many are
// still in flight at each step.
func (s *PostgresStore) GetPendingByType(ctx context.Context) (map[string]int, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT email_type, COUNT(*) FROM scheduled_emails
		WHERE sent_at IS NULL GROUP BY email_type`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var t string
		var n int
		if err := rows.Scan(&t, &n); err == nil {
			out[t] = n
		}
	}
	return out, nil
}

// ListLogsByEmailType returns paginated sends for a specific email_type
func (s *PostgresStore) ListLogsByEmailType(ctx context.Context, emailType string, limit, offset int) ([]EmailLog, int, error) {
	var total int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM scheduled_emails WHERE sent_at IS NOT NULL AND email_type = $1`,
		emailType).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT
			se.id,
			se.user_id,
			COALESCE(NULLIF(u.name,''), log_info.user_name, '')    AS user_name,
			COALESCE(NULLIF(u.email,''), log_info.email_to, '')    AS email_to,
			se.email_type         AS template_name,
			''                    AS from_address,
			'sent'                AS status,
			se.sent_at,
			esl.opened_at
		FROM scheduled_emails se
		LEFT JOIN "user" u ON u.id = se.user_id
		LEFT JOIN LATERAL (
			SELECT email_to, user_name FROM email_send_log
			WHERE user_id = se.user_id AND template_name = se.email_type
			ORDER BY sent_at DESC LIMIT 1
		) log_info ON true
		LEFT JOIN LATERAL (
			SELECT opened_at FROM email_send_log
			WHERE (email_to = u.email OR email_to = log_info.email_to)
			  AND template_name = se.email_type
			  AND opened_at IS NOT NULL
			ORDER BY sent_at DESC LIMIT 1
		) esl ON true
		WHERE se.sent_at IS NOT NULL AND se.email_type = $1
		ORDER BY se.sent_at DESC
		LIMIT $2 OFFSET $3
	`, emailType, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var logs []EmailLog
	for rows.Next() {
		var l EmailLog
		if err := rows.Scan(&l.ID, &l.UserID, &l.UserName, &l.EmailTo, &l.TemplateName,
			&l.FromAddress, &l.Status, &l.SentAt, &l.OpenedAt); err == nil {
			logs = append(logs, l)
		}
	}
	return logs, total, nil
}

// GetUserEmailHistory returns the last 50 emails sent to a given user (from both sources)
func (s *PostgresStore) GetUserEmailHistory(ctx context.Context, userID string) ([]EmailLog, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			se.id,
			se.user_id,
			COALESCE(u.name, '')  AS user_name,
			COALESCE(u.email, '') AS email_to,
			se.email_type         AS template_name,
			''                    AS from_address,
			'sent'                AS status,
			se.sent_at,
			esl.opened_at
		FROM scheduled_emails se
		LEFT JOIN "user" u ON u.id = se.user_id
		LEFT JOIN LATERAL (
			SELECT opened_at FROM email_send_log
			WHERE email_to = u.email AND template_name = se.email_type
			  AND opened_at IS NOT NULL
			ORDER BY sent_at DESC
			LIMIT 1
		) esl ON true
		WHERE se.sent_at IS NOT NULL AND se.user_id = $1
		ORDER BY se.sent_at DESC
		LIMIT 50
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []EmailLog
	for rows.Next() {
		var l EmailLog
		if err := rows.Scan(&l.ID, &l.UserID, &l.UserName, &l.EmailTo, &l.TemplateName,
			&l.FromAddress, &l.Status, &l.SentAt, &l.OpenedAt); err == nil {
			logs = append(logs, l)
		}
	}
	return logs, nil
}

// ListUsersWithStats returns a paginated list of users with email activity counts
func (s *PostgresStore) ListUsersWithStats(ctx context.Context, limit, offset int, search string) ([]UserWithStats, int, error) {
	var total int
	args := []interface{}{}
	where := ""
	idx := 1

	if search != "" {
		where = fmt.Sprintf(` WHERE (u.name ILIKE $%d OR u.email ILIKE $%d)`, idx, idx+1)
		p := "%" + search + "%"
		args = append(args, p, p)
		idx += 2
	}

	countQ := `SELECT COUNT(*) FROM "user" u` + where
	if err := s.db.QueryRowContext(ctx, countQ, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count users: %w", err)
	}

	args = append(args, limit, offset)
	q := fmt.Sprintf(`
		SELECT u.id, COALESCE(u.name,''), u.email, u.created_at,
		       COALESCE(sl.sent, 0)   AS emails_sent,
		       COALESCE(ol.opened, 0) AS emails_opened,
		       COALESCE(lt.template_name, '') AS last_template
		FROM "user" u
		LEFT JOIN (
			SELECT user_id, COUNT(*) AS sent
			FROM scheduled_emails
			WHERE sent_at IS NOT NULL
			GROUP BY user_id
		) sl ON sl.user_id = u.id
		LEFT JOIN (
			SELECT email_to, COUNT(*) AS opened
			FROM email_send_log
			WHERE opened_at IS NOT NULL
			GROUP BY email_to
		) ol ON ol.email_to = u.email
		LEFT JOIN LATERAL (
			SELECT template_name FROM email_send_log
			WHERE email_to = u.email
			ORDER BY sent_at DESC NULLS LAST
			LIMIT 1
		) lt ON true
		%s
		ORDER BY u.created_at DESC
		LIMIT $%d OFFSET $%d
	`, where, idx, idx+1)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var users []UserWithStats
	for rows.Next() {
		var u UserWithStats
		if err := rows.Scan(&u.ID, &u.Name, &u.Email, &u.CreatedAt, &u.EmailsSent, &u.EmailsOpened, &u.LastTemplate); err == nil {
			users = append(users, u)
		}
	}
	return users, total, nil
}
