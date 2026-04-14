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

// UpdateCampaignStatus updates status, sent_at, and recipient counts
func (s *PostgresStore) UpdateCampaignStatus(ctx context.Context, id, status string, sentAt *time.Time, total, sent int) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE email_campaigns
		SET status = $1, sent_at = $2, total_recipients = $3, sent_count = $4
		WHERE id = $5
	`, status, sentAt, total, sent, id)
	return err
}
