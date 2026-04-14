package store

import (
	"context"
	"fmt"
	"time"
)

// EmailLog is a row in the email_send_log table
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

// EmailStats is the top-level stats object for the admin dashboard
type EmailStats struct {
	TotalSent    int             `json:"total_sent"`
	TotalOpened  int             `json:"total_opened"`
	OpenRate     float64         `json:"open_rate"`
	SentToday    int             `json:"sent_today"`
	SentThisWeek int             `json:"sent_this_week"`
	TotalUsers   int             `json:"total_users"`
	TopTemplates []TemplateStats `json:"top_templates"`
	DailyVolume  []DailyVolume   `json:"daily_volume"`
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

// UserWithStats is a user row enriched with email activity counts
type UserWithStats struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Email        string    `json:"email"`
	CreatedAt    time.Time `json:"created_at"`
	EmailsSent   int       `json:"emails_sent"`
	EmailsOpened int       `json:"emails_opened"`
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

// GetEmailStats returns aggregate stats for the admin dashboard
func (s *PostgresStore) GetEmailStats(ctx context.Context) (*EmailStats, error) {
	stats := &EmailStats{}

	row := s.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*)                                             AS total_sent,
			COUNT(opened_at)                                     AS total_opened,
			COUNT(*) FILTER (WHERE sent_at >= NOW() - INTERVAL '24 hours') AS sent_today,
			COUNT(*) FILTER (WHERE sent_at >= NOW() - INTERVAL '7 days')   AS sent_this_week
		FROM email_send_log
	`)
	if err := row.Scan(&stats.TotalSent, &stats.TotalOpened, &stats.SentToday, &stats.SentThisWeek); err != nil {
		return nil, fmt.Errorf("aggregate stats: %w", err)
	}
	if stats.TotalSent > 0 {
		stats.OpenRate = float64(stats.TotalOpened) / float64(stats.TotalSent) * 100
	}

	// Total users (Better Auth uses "user" table)
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM "user"`).Scan(&stats.TotalUsers)

	// Top 8 templates by send count
	rows, err := s.db.QueryContext(ctx, `
		SELECT template_name, COUNT(*) AS send_count, COUNT(opened_at) AS open_count
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

	// Daily volume — last 14 days
	drows, err := s.db.QueryContext(ctx, `
		SELECT TO_CHAR(sent_at::date, 'YYYY-MM-DD'), COUNT(*)
		FROM email_send_log
		WHERE sent_at >= NOW() - INTERVAL '14 days'
		GROUP BY sent_at::date
		ORDER BY sent_at::date
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

// ListEmailLogs returns paginated send log rows with optional search
func (s *PostgresStore) ListEmailLogs(ctx context.Context, limit, offset int, search string) ([]EmailLog, int, error) {
	var total int
	args := []interface{}{}
	where := ""
	idx := 1

	if search != "" {
		where = fmt.Sprintf(
			" WHERE (email_to ILIKE $%d OR template_name ILIKE $%d OR user_name ILIKE $%d)",
			idx, idx+1, idx+2,
		)
		p := "%" + search + "%"
		args = append(args, p, p, p)
		idx += 3
	}

	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM email_send_log"+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	args = append(args, limit, offset)
	q := fmt.Sprintf(`
		SELECT id, user_id, user_name, email_to, template_name, from_address, status, sent_at, opened_at
		FROM email_send_log%s
		ORDER BY sent_at DESC
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

// GetUserEmailHistory returns the last 50 emails sent to a given user
func (s *PostgresStore) GetUserEmailHistory(ctx context.Context, userID string) ([]EmailLog, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, user_id, user_name, email_to, template_name, from_address, status, sent_at, opened_at
		FROM email_send_log
		WHERE user_id = $1
		ORDER BY sent_at DESC
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
		SELECT u.id, u.name, u.email, u.created_at,
		       COALESCE(sl.sent, 0)   AS emails_sent,
		       COALESCE(sl.opened, 0) AS emails_opened
		FROM "user" u
		LEFT JOIN (
			SELECT user_id,
			       COUNT(*)          AS sent,
			       COUNT(opened_at)  AS opened
			FROM email_send_log
			GROUP BY user_id
		) sl ON sl.user_id = u.id
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
		if err := rows.Scan(&u.ID, &u.Name, &u.Email, &u.CreatedAt, &u.EmailsSent, &u.EmailsOpened); err == nil {
			users = append(users, u)
		}
	}
	return users, total, nil
}
