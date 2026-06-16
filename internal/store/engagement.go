package store

import (
	"context"
	"strings"
)

// Engagement is the SINGLE source of truth for "what has this person done",
// assembled once from the authoritative signal tables (email_opens, email_clicks,
// tool_used). Gates, attribution, and the dashboard should read from here rather
// than each re-deriving "engaged" with its own ad-hoc query. Keyed on email,
// which is the identity the pixel/click/used paths all carry.
type Engagement struct {
	Email          string            `json:"email"`
	OpenedTypes    map[string]bool   `json:"opened_types"`    // email_type -> opened
	ClickedTypes   map[string]bool   `json:"clicked_types"`   // email_type -> clicked
	ToolsUsed      map[string]string `json:"tools_used"`      // tool -> source ("email"|"direct")
	AnyOpen        bool              `json:"any_open"`
	AnyClick       bool              `json:"any_click"`
	AnyToolUsed    bool              `json:"any_tool_used"`
	AnyReply       bool              `json:"any_reply"` // replied to ANY email — highest intent
}

// GetEngagement assembles a user's full engagement state from the signal tables
// in one place. Cheap (3 indexed lookups by email); callers that only need a
// boolean should use the narrower helpers below.
func (s *PostgresStore) GetEngagement(ctx context.Context, email string) (*Engagement, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	e := &Engagement{
		Email:        email,
		OpenedTypes:  map[string]bool{},
		ClickedTypes: map[string]bool{},
		ToolsUsed:    map[string]string{},
	}

	oRows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT email_type FROM email_opens WHERE lower(email) = $1`, email)
	if err == nil {
		for oRows.Next() {
			var t string
			if oRows.Scan(&t) == nil {
				e.OpenedTypes[t] = true
				e.AnyOpen = true
			}
		}
		oRows.Close()
	}

	cRows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT email_type FROM email_clicks WHERE lower(email) = $1`, email)
	if err == nil {
		for cRows.Next() {
			var t string
			if cRows.Scan(&t) == nil {
				e.ClickedTypes[t] = true
				e.AnyClick = true
			}
		}
		cRows.Close()
	}

	tRows, err := s.db.QueryContext(ctx,
		`SELECT tool, source FROM tool_used WHERE lower(email) = $1`, email)
	if err == nil {
		for tRows.Next() {
			var tool, source string
			if tRows.Scan(&tool, &source) == nil {
				e.ToolsUsed[tool] = source
				e.AnyToolUsed = true
			}
		}
		tRows.Close()
	}

	var replies int
	if s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM email_replies WHERE lower(email) = $1`, email).Scan(&replies) == nil {
		e.AnyReply = replies > 0
	}

	return e, nil
}

// EngagedWithType reports whether the user opened OR clicked a given email_type
// (e.g. a welcome). This is the canonical engagement check the gate uses.
func (e *Engagement) EngagedWithType(emailType string) bool {
	return e.OpenedTypes[emailType] || e.ClickedTypes[emailType]
}
