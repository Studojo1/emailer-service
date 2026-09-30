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
	{
		Name:      "funnel_stage_write_failed",
		What:      "funnel/order writes that failed and were swallowed (on 23 Sep this lost 9 students' orders at upload for 8 hours)",
		Fix:       "job-outreach-svc services/stage_tracking.safe_mark_stage: read system_events.metadata->>'error' for the cause (UC-Q16)",
		Threshold: 0,
		query: `SELECT count(*) FROM system_events
			WHERE event_type = 'stage_tracking_failed' AND created_at > now() - interval '24 hours'`,
	},
	// ── Outreach sending (B2C audit 29 Sep 2026) ────────────────────────────
	{
		Name:      "sends_outside_window",
		What:      "outreach emails sent before 9 AM or after 6 PM in the campaign's timezone",
		Fix:       "job-outreach-svc campaign_worker._deferred_to_send_window (PS-N16): every send path must go through it",
		Threshold: 0,
		query: `SELECT count(*) FROM emails_sent e JOIN campaigns c ON c.id = e.campaign_id
			WHERE e.sent_at BETWEEN now() - interval '24 hours' AND now() AND coalesce(e.is_test, false) = false
			  AND extract(hour FROM (e.sent_at AT TIME ZONE 'UTC') AT TIME ZONE coalesce(c.user_timezone, 'Asia/Kolkata')) NOT BETWEEN 9 AND 17`,
	},
	{
		Name:      "campaign_over_paid_credits",
		What:      "campaigns that sent more paid first emails than credits reserved minus released",
		Fix:       "job-outreach-svc campaign_worker._over_paid_cap (PP-P26) in _send_ready / _enrich_one; a legacy campaign with 0 reserved must go through adopt_legacy_reservation before it runs",
		Threshold: 0,
		// Includes credits_reserved = 0: legacy campaigns are capped at 0 new
		// paid sends until a resume re-reserves for them (30 Sep).
		query: `SELECT count(*) FROM campaigns c
			WHERE c.credits_reserved IS NOT NULL
			  AND EXISTS (SELECT 1 FROM emails_sent e WHERE e.campaign_id = c.id AND e.sent_at > now() - interval '24 hours'
			              AND coalesce(e.is_test, false) = false AND e.followup_number = 0
			              AND coalesce(e.replacement_reason, '') <> 'bounce')
			  AND (SELECT count(*) FROM emails_sent e WHERE e.campaign_id = c.id AND coalesce(e.is_test, false) = false
			       AND e.followup_number = 0 AND coalesce(e.replacement_reason, '') <> 'bounce'
			       AND e.status IN ('sending', 'sent', 'replied', 'bounced')) > c.credits_reserved - coalesce(c.credits_released, 0)`,
	},
	{
		Name:      "emails_stuck_sending",
		What:      "emails left in 'sending' for over 30 minutes (a hung Gmail call or a dead pod)",
		Fix:       "job-outreach-svc campaign_worker._reap_stuck_sending (PP-P36) should settle these within 15 minutes",
		Threshold: 0,
		query: `SELECT count(*) FROM emails_sent e
			WHERE e.status = 'sending' AND coalesce(e.status_changed_at, e.scheduled_at) < now() - interval '30 minutes'`,
	},
	{
		Name:      "followup_after_reply",
		What:      "follow-ups sent to someone who had already replied to the first email",
		Fix:       "job-outreach-svc: reply check must run before follow-ups (_process_cycle) and on resume (NEW-03)",
		Threshold: 0,
		query: `SELECT count(*) FROM emails_sent f JOIN emails_sent p ON p.id = f.parent_email_id
			WHERE f.followup_number > 0 AND f.sent_at > now() - interval '24 hours'
			  AND p.reply_received_at IS NOT NULL AND p.reply_received_at < f.sent_at - interval '10 minutes'`,
	},
	{
		Name:      "reply_check_stale",
		What:      "connected mailboxes of running or paused campaigns whose replies have not been read for over an hour",
		Fix:       "job-outreach-svc campaign_worker._reply_check_account_ids / check_mailbox_replies (NEW-03, PS-N14)",
		Threshold: 0,
		query: `SELECT count(DISTINCT a.id) FROM email_accounts a JOIN campaigns c ON c.email_account_id = a.id
			WHERE c.status IN ('running', 'paused') AND a.token_invalid_at IS NULL AND coalesce(a.access_token, '') <> ''
			  AND (a.last_reply_check_at IS NULL OR a.last_reply_check_at < now() - interval '1 hour')`,
	},
	{
		Name:      "unpaid_campaign_setup",
		What:      "orders that reached campaign setup (paid Apollo reveals) without a payment or credits",
		Fix:       "job-outreach-svc routes_orders.update_order payment gate (PS-N08)",
		Threshold: 0,
		query: `SELECT count(*) FROM outreach_orders o
			WHERE o.status = 'campaign_setup' AND o.updated_at > now() - interval '24 hours' AND o.payment_made_at IS NULL
			  AND NOT EXISTS (SELECT 1 FROM user_credits w WHERE w.user_id = o.user_id AND w.total_credits > 0)`,
	},
	{
		Name:      "fresh_campaigns_low_reply_rate",
		What:      "campaigns launched 10-40 days ago whose first 100 emails got replies from under 1.5% of recipients",
		Fix:       "deliverability (seed-test inbox placement, open pixel), copy and lead quality; see audit PS-N03",
		Threshold: 2,
		query: `SELECT count(*) FROM (
			SELECT c.id, count(*) FILTER (WHERE x.status = 'replied' OR x.reply_received_at IS NOT NULL)::float / count(*) AS rate
			FROM campaigns c JOIN LATERAL (
				SELECT e.status, e.reply_received_at FROM emails_sent e
				WHERE e.campaign_id = c.id AND e.followup_number = 0 AND coalesce(e.is_test, false) = false AND e.sent_at IS NOT NULL
				ORDER BY e.sent_at LIMIT 100) x ON true
			WHERE c.started_at BETWEEN now() - interval '40 days' AND now() - interval '10 days'
			GROUP BY c.id HAVING count(*) = 100) r WHERE r.rate < 0.015`,
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
