package store

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"strings"
)

// CohortBucket returns the deterministic 0-99 bucket for an email. Shared by the
// scheduler's cohort gate and the metrics so "who is in the variant" has exactly
// one definition. (Mirrors inVariantCohort in the scheduler package.)
func CohortBucket(email string) int {
	h := sha1.Sum([]byte(strings.ToLower(strings.TrimSpace(email))))
	return int(binary.BigEndian.Uint32(h[:4]) % 100)
}

// BehavioralCohortStat is the engagement summary for one experiment cohort.
type BehavioralCohortStat struct {
	Cohort   string `json:"cohort"` // "variant" | "control"
	Users    int    `json:"users"`
	Opened   int    `json:"opened"`
	Clicked  int    `json:"clicked"`
	UsedTool int    `json:"used_tool"`
	Replied  int    `json:"replied"`
}

// GetBehavioralExperiment splits recently-active users (last `withinDays`) into
// variant vs control at the given pct and reports each cohort's engagement, so
// the dashboard can show whether behavioral routing is beating the fixed chains.
func (s *PostgresStore) GetBehavioralExperiment(ctx context.Context, withinDays, pct int) ([]BehavioralCohortStat, error) {
	users, err := s.ListUsersBySignupDate(ctx, withinDays)
	if err != nil {
		return nil, err
	}
	variant := &BehavioralCohortStat{Cohort: "variant"}
	control := &BehavioralCohortStat{Cohort: "control"}
	for _, u := range users {
		stat := control
		if pct > 0 && CohortBucket(u.Email) < pct {
			stat = variant
		}
		stat.Users++
		eng, err := s.GetEngagement(ctx, u.Email)
		if err != nil {
			continue
		}
		if eng.AnyOpen {
			stat.Opened++
		}
		if eng.AnyClick {
			stat.Clicked++
		}
		if eng.AnyToolUsed {
			stat.UsedTool++
		}
		if eng.AnyReply {
			stat.Replied++
		}
	}
	return []BehavioralCohortStat{*variant, *control}, nil
}
