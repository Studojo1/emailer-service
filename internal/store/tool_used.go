package store

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// toolEmailPrefixes maps a tool to the email_type prefixes of ITS flow only, so
// attribution ("did they click one of THIS tool's emails before using it") is
// specific. Coach must NOT use a bare "cc-" — that matches every flow and makes
// every tool look email-driven; it gets its own concrete prefixes instead.
var toolEmailPrefixes = map[string][]string{
	"outreach":   {"cc-outreach"},
	"resume":     {"cc-rm"},
	"internship": {"cc-id"},
	"coach":      {"cc-welcome", "cc-nudge", "cc-dna", "cc-roadmap", "cc-checkin", "cc-upskill", "cc-coupon", "cc-dormant", "cc-to-outreach", "cc-returning", "cc-profiling"},
}

// RecordToolUsed logs the FIRST use of a tool by a user, with attribution.
// source is "email" when the user clicked one of THAT tool's flow emails STRICTLY
// BEFORE this use, otherwise "direct". Deduped to one row per (user, tool) so the
// first use is the attributed one. Records used_at first so the click comparison
// is genuinely time-ordered (not "clicked ever").
func (s *PostgresStore) RecordToolUsed(ctx context.Context, userID, email, tool string) error {
	email = strings.ToLower(strings.TrimSpace(email))
	usedAt := time.Now().UTC()

	// Already recorded this tool for this user? Keep the first attribution.
	var existing int
	_ = s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM tool_used WHERE user_id = $1 AND tool = $2`,
		userID, tool,
	).Scan(&existing)
	if existing > 0 {
		return nil
	}

	// Attribution: did they click one of this tool's flow emails before now?
	// Time-ordered (clicked_at <= used_at) so a later click can't back-date.
	source := "direct"
	if prefixes, ok := toolEmailPrefixes[tool]; ok && email != "" {
		// Build an ANY(LIKE) match over this tool's prefixes.
		clause := make([]string, 0, len(prefixes))
		args := []interface{}{email, usedAt}
		for i, p := range prefixes {
			clause = append(clause, "email_type LIKE $"+strconv.Itoa(3+i))
			args = append(args, p+"%")
		}
		q := `SELECT COUNT(*) FROM email_clicks
		      WHERE lower(email) = $1 AND clicked_at <= $2 AND (` + strings.Join(clause, " OR ") + `)`
		var clicks int
		_ = s.db.QueryRowContext(ctx, q, args...).Scan(&clicks)
		if clicks > 0 {
			source = "email"
		}
	}

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO tool_used (id, user_id, email, tool, source, used_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		uuid.New().String(), userID, email, tool, source, usedAt,
	)
	return err
}

// ToolUsedStat is per-tool usage with attribution split.
type ToolUsedStat struct {
	Tool     string `json:"tool"`
	Total    int    `json:"total"`
	ViaEmail int    `json:"via_email"`
	Direct   int    `json:"direct"`
}

// GetToolUsedStats returns per-tool used counts split by attribution source.
func (s *PostgresStore) GetToolUsedStats(ctx context.Context) (map[string]ToolUsedStat, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT tool,
		       COUNT(*) AS total,
		       COUNT(*) FILTER (WHERE source = 'email')  AS via_email,
		       COUNT(*) FILTER (WHERE source = 'direct') AS direct
		FROM tool_used GROUP BY tool`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]ToolUsedStat{}
	for rows.Next() {
		var st ToolUsedStat
		if err := rows.Scan(&st.Tool, &st.Total, &st.ViaEmail, &st.Direct); err == nil {
			out[st.Tool] = st
		}
	}
	return out, nil
}
