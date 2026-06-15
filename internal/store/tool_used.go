package store

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
)

// toolEmailPrefix maps a tool name to the email_type prefix of its flow, so we
// can tell whether the user clicked one of that flow's emails before using the
// tool (= attributed to email) vs used it cold (= direct).
var toolEmailPrefix = map[string]string{
	"outreach":   "cc-outreach",
	"resume":     "cc-rm",
	"internship": "cc-id",
	"coach":      "cc-",
}

// RecordToolUsed logs a tool-use event with attribution. source is "email" when
// the user clicked an email from that tool's flow at any time before this use,
// otherwise "direct". Deduped to one row per (user, tool) so repeat uses don't
// inflate counts — the FIRST use is the one that matters for attribution.
func (s *PostgresStore) RecordToolUsed(ctx context.Context, userID, email, tool string) error {
	email = strings.ToLower(strings.TrimSpace(email))

	// Already recorded this tool for this user? Keep the first attribution.
	var existing int
	_ = s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM tool_used WHERE user_id = $1 AND tool = $2`,
		userID, tool,
	).Scan(&existing)
	if existing > 0 {
		return nil
	}

	// Attribution: did they click any email from this tool's flow before now?
	source := "direct"
	if prefix, ok := toolEmailPrefix[tool]; ok && email != "" {
		var clicks int
		_ = s.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM email_clicks
			WHERE lower(email) = $1 AND email_type LIKE $2`,
			email, prefix+"%",
		).Scan(&clicks)
		if clicks > 0 {
			source = "email"
		}
	}

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO tool_used (id, user_id, email, tool, source, used_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		uuid.New().String(), userID, email, tool, source, time.Now().UTC(),
	)
	return err
}

// ToolUsedStat is per-tool usage with attribution split.
type ToolUsedStat struct {
	Tool   string `json:"tool"`
	Total  int    `json:"total"`
	ViaEmail int  `json:"via_email"`
	Direct int    `json:"direct"`
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
