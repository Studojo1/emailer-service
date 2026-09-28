package store

import "context"

// DataHealthCheck is one invariant that should hold for every new row. Count
// returns how many rows in the last 24 hours break it; anything above
// Threshold pages ops. Each exists because the problem it catches reached
// real students unnoticed until an audit found it weeks later.
type DataHealthCheck struct {
	Name      string
	What      string // one line for the alert email
	Fix       string // where to look first
	Threshold int
	query     string
}

// DataHealthChecks is the registry. Windows skip the newest few minutes so a
// row that is still being written is not counted as broken.
var DataHealthChecks = []DataHealthCheck{
	{
		Name:      "users_without_login",
		What:      "new users with no login account (email is taken, no password or Google link, so they cannot sign in)",
		Fix:       "sign-up must be atomic: drizzleAdapter transaction: true in frontend app/lib/auth.ts. Affected users recover via Forgot password.",
		Threshold: 0,
		query: `SELECT count(*) FROM "user" u
			WHERE u.created_at BETWEEN now() - interval '24 hours' AND now() - interval '10 minutes'
			  AND u.email NOT LIKE '%@studojo.test'
			  AND NOT EXISTS (SELECT 1 FROM account a WHERE a.user_id = u.id)`,
	},
	{
		Name:      "resumes_without_profile",
		What:      "new resumes whose profile extraction never landed (their quiz gets generic questions)",
		Fix:       "job-outreach-svc resume_intelligence (Azure OpenAI). Re-run: python -m scripts.backfill_resume_profiles --limit N --apply",
		Threshold: 2,
		query: `SELECT count(*) FROM candidates c
			WHERE c.created_at BETWEEN now() - interval '24 hours' AND now() - interval '30 minutes'
			  AND c.resume_text IS NOT NULL AND length(trim(c.resume_text)) >= 200
			  AND c.resume_profile IS NULL`,
	},
	{
		Name:      "duplicate_unused_candidates",
		What:      "students with more than one unused candidate row (answers can land on the wrong row)",
		Fix:       "job-outreach-svc find_reusable_candidate in api/routes_candidate.py (upload must reuse an unused row)",
		Threshold: 0,
		query: `SELECT count(*) FROM (
			SELECT c.user_id FROM candidates c
			WHERE c.created_at > now() - interval '24 hours'
			  AND (c.target_roles IS NULL OR c.target_roles::text IN ('null', '[]'))
			  AND NOT EXISTS (SELECT 1 FROM leads l WHERE l.candidate_id = c.id)
			GROUP BY c.user_id HAVING count(*) > 1) x`,
	},
	{
		Name:      "quiz_completed_without_roles",
		What:      "orders marked quiz-completed whose candidate has no target roles (lead search has nothing to look for)",
		Fix:       "job-outreach-svc /generate-payload and _mark_quiz_completed_if_targeted",
		Threshold: 0,
		query: `SELECT count(*) FROM outreach_orders o JOIN candidates c ON c.id = o.candidate_id
			WHERE o.quiz_completed_at > now() - interval '24 hours'
			  AND (c.target_roles IS NULL OR c.target_roles::text IN ('null', '[]'))`,
	},
}

// CountDataHealthViolations runs one check.
func (s *PostgresStore) CountDataHealthViolations(ctx context.Context, c DataHealthCheck) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, c.query).Scan(&n)
	return n, err
}

// ClaimDataHealthAlert records that ops are being paged for this check today
// (IST) and reports whether this caller won the claim. The primary key on
// (check_name, day) makes it atomic, so a restart or a second replica cannot
// page twice for the same problem on the same day (migration 050).
func (s *PostgresStore) ClaimDataHealthAlert(ctx context.Context, check string, count int) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO data_health_alerts (check_name, day, count)
		VALUES ($1, (now() AT TIME ZONE 'Asia/Kolkata')::date, $2)
		ON CONFLICT (check_name, day) DO NOTHING`, check, count)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// ReleaseDataHealthAlert undoes a claim whose alert could not be sent, so the
// next hourly run tries again instead of staying silent for the rest of the day.
func (s *PostgresStore) ReleaseDataHealthAlert(ctx context.Context, check string) error {
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM data_health_alerts
		WHERE check_name = $1 AND day = (now() AT TIME ZONE 'Asia/Kolkata')::date`, check)
	return err
}
