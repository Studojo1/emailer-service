package store

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Campaign represents a bulk email campaign
type Campaign struct {
	ID              string     `json:"id"`
	Name            string     `json:"name"`
	TemplateName    string     `json:"template_name"`
	Status          string     `json:"status"` // draft | running | completed | failed
	FilterDays      int        `json:"filter_days"` // 0 = all users
	TotalRecipients int        `json:"total_recipients"`
	SentCount       int        `json:"sent_count"`
	OpenCount       int        `json:"open_count"`
	CreatedAt       time.Time  `json:"created_at"`
	SentAt          *time.Time `json:"sent_at,omitempty"`
}

// CreateCampaign inserts a new campaign in draft state
func (s *PostgresStore) CreateCampaign(ctx context.Context, name, templateName string, filterDays int) (*Campaign, error) {
	id := uuid.New().String()
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO email_campaigns (id, name, template_name, status, filter_days, created_at)
		VALUES ($1, $2, $3, 'draft', $4, $5)
	`, id, name, templateName, filterDays, now)
	if err != nil {
		return nil, err
	}
	return &Campaign{
		ID:           id,
		Name:         name,
		TemplateName: templateName,
		Status:       "draft",
		FilterDays:   filterDays,
		CreatedAt:    now,
	}, nil
}

// ListCampaigns returns all campaigns ordered newest first
func (s *PostgresStore) ListCampaigns(ctx context.Context) ([]Campaign, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, template_name, status, filter_days,
		       total_recipients, sent_count, open_count, created_at, sent_at
		FROM email_campaigns
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var campaigns []Campaign
	for rows.Next() {
		var c Campaign
		if err := rows.Scan(&c.ID, &c.Name, &c.TemplateName, &c.Status, &c.FilterDays,
			&c.TotalRecipients, &c.SentCount, &c.OpenCount, &c.CreatedAt, &c.SentAt); err == nil {
			campaigns = append(campaigns, c)
		}
	}
	return campaigns, nil
}

// GetCampaign returns a single campaign by ID
func (s *PostgresStore) GetCampaign(ctx context.Context, id string) (*Campaign, error) {
	var c Campaign
	err := s.db.QueryRowContext(ctx, `
		SELECT id, name, template_name, status, filter_days,
		       total_recipients, sent_count, open_count, created_at, sent_at
		FROM email_campaigns
		WHERE id = $1
	`, id).Scan(&c.ID, &c.Name, &c.TemplateName, &c.Status, &c.FilterDays,
		&c.TotalRecipients, &c.SentCount, &c.OpenCount, &c.CreatedAt, &c.SentAt)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// IsUserPaid reports whether the user has paid through ANY channel, so paying
// customers are never nagged with "finish paying" / marketing sequences.
//
// Historically this only checked outreach_orders, so a student who paid through
// any other channel kept receiving conversion email (audit J1) — the worst
// possible audience for it. We now also check payment_orders when that table is
// present. to_regclass makes the extra source optional: these tables belong to
// the main platform's schema, so on a deployment where payment_orders does not
// exist the check degrades to outreach_orders instead of erroring on every call.
//
// Fails soft — callers log and proceed if this returns an error.
func (s *PostgresStore) IsUserPaid(ctx context.Context, userID string) (bool, error) {
// The two sources are queried SEPARATELY on purpose: Postgres resolves every
// table reference at parse time, so folding an optional table into one statement
// would raise "relation does not exist" on deployments without it — turning this
// into a permanent error and nagging paid users even harder.
func (s *PostgresStore) IsUserPaid(ctx context.Context, userID string) (bool, error) {
	var paid bool
	if err := s.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM outreach_orders
			WHERE user_id = $1 AND status NOT IN ('created','failed')
		)`, userID).Scan(&paid); err != nil {
		return false, err
	}
	if paid {
		return true, nil
	}

	// Optional second source. Skipped entirely when the table is absent.
	var hasTable bool
	if err := s.db.QueryRowContext(ctx,
		`SELECT to_regclass('public.payment_orders') IS NOT NULL`).Scan(&hasTable); err != nil || !hasTable {
		return false, nil // outreach_orders already said "not paid"
	}
	if err := s.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM payment_orders
			WHERE user_id = $1 AND status NOT IN ('created','failed')
		)`, userID).Scan(&paid); err != nil {
		return false, nil // optional source failed; trust the primary result
	}
	return paid, nil
}

// UpdateCampaignStatus updates status, sent_at, and recipient counts
func (s *PostgresStore) UpdateCampaignStatus(ctx context.Context, id, status string, sentAt *time.Time, total, sent int) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE email_campaigns
		SET status = $1, sent_at = $2, total_recipients = $3, sent_count = $4
		WHERE id = $5
	`, status, sentAt, total, sent, id)
	return err
}
