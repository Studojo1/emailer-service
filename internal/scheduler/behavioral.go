package scheduler

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/studojo/emailer-service/internal/store"
)

// Behavioral routing (Idea D) — an EXPERIMENT, not a replacement for the fixed
// chains. It runs alongside them: for a small, deterministic cohort, on a slow
// tick, it looks at the user's full engagement state and decides the next-best
// email (or nothing). It is OFF by default and SHADOW by default, so it cannot
// mis-send at scale until you deliberately turn it on.
//
// Controls (env):
//   BEHAVIORAL_ROUTING_PCT   percent of users in the variant cohort (default 0 = off)
//   BEHAVIORAL_ROUTING_SHADOW "true" (default) logs decisions without sending
//
// The fixed-chain scheduler remains the control. Compare cohorts on the
// dashboard before ever widening the cohort or leaving shadow mode.

func behavioralPct() int {
	if v := os.Getenv("BEHAVIORAL_ROUTING_PCT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 && n <= 100 {
			return n
		}
	}
	return 0
}

func behavioralShadow() bool {
	// Default true: observe-only until explicitly set to "false".
	return strings.ToLower(os.Getenv("BEHAVIORAL_ROUTING_SHADOW")) != "false"
}

// inVariantCohort deterministically buckets a user (by email) into the variant
// at the configured percent. Uses store.CohortBucket so the scheduler and the
// experiment metrics agree on exactly who is in the variant.
func inVariantCohort(email string, pct int) bool {
	if pct <= 0 {
		return false
	}
	if pct >= 100 {
		return true
	}
	return store.CohortBucket(email) < pct
}

// runBehavioral is the decision-tick. Hooked into Run() on a slow ticker. It is
// a no-op unless BEHAVIORAL_ROUTING_PCT > 0. For each variant-cohort user with a
// pending decision, it ranks candidates and either logs (shadow) or acts.
func (sc *Scheduler) runBehavioral(ctx context.Context) {
	pct := behavioralPct()
	if pct == 0 {
		return // experiment off — fixed chains handle everyone
	}
	shadow := behavioralShadow()

	// Candidate population: users who signed up in the last 14 days (reuse the
	// existing surface; kept small so the tick stays cheap).
	users, err := sc.Store.ListUsersBySignupDate(ctx, 14)
	if err != nil {
		slog.Error("behavioral: list users failed", "err", err)
		return
	}

	considered, decided := 0, 0
	for _, u := range users {
		if !inVariantCohort(u.Email, pct) {
			continue // control cohort — fixed chains only
		}
		considered++

		eng, err := sc.Store.GetEngagement(ctx, u.Email)
		if err != nil {
			continue
		}

		// Highest-intent rule: a reply means stop all automation, flag hot.
		if eng.AnyReply {
			slog.Info("behavioral: replier — suppress all automation (hot lead)", "email", u.Email, "shadow", shadow)
			continue
		}

		choice := rankNextBest(eng)
		if choice == "" {
			continue // nothing better than silence right now
		}
		decided++

		if shadow {
			slog.Info("behavioral SHADOW: would send", "email", u.Email, "choice", choice)
			continue
		}

		// Live mode: honour the frequency cap, then schedule the chosen email now.
		recent, _ := sc.Store.CountMarketingSentSince(ctx, u.Email, 7*24*time.Hour)
		if recent >= maxMarketingPerWeek() {
			slog.Info("behavioral: choice suppressed by frequency cap", "email", u.Email, "choice", choice)
			continue
		}
		if err := sc.Store.CreateScheduledEmail(ctx, u.ID, choice, time.Now().UTC()); err != nil {
			slog.Error("behavioral: schedule failed", "email", u.Email, "choice", choice, "err", err)
		} else {
			slog.Info("behavioral: scheduled next-best", "email", u.Email, "choice", choice)
		}
	}
	if considered > 0 {
		slog.Info("behavioral tick", "cohort_pct", pct, "shadow", shadow, "considered", considered, "decided", decided)
	}
}

// rankNextBest is the (transparent, hand-written) ranking function. Given the
// user's full engagement state it returns the email_type to send next, or "" for
// "nothing is better than silence". Rules are intentionally simple and auditable;
// a model could replace this later, but only after the rules prove out.
func rankNextBest(eng *store.Engagement) string {
	used := eng.ToolsUsed

	// 1. Used a tool but no DNA/coach engagement → nudge toward direction.
	if len(used) > 0 {
		if _, coach := used["coach"]; !coach && !eng.OpenedTypes["cc-welcome"] {
			return "cc-welcome" // gently route to coach value
		}
	}

	// 2. Opened welcome, never clicked, never used → a different angle, not the
	//    next scripted nudge. (Single send; the chain's repetition is the problem.)
	if eng.AnyOpen && !eng.AnyClick && !eng.AnyToolUsed {
		return "cc-outreach-nudge-d2" // the "what one student got" angle
	}

	// 3. No signal at all yet → let the fixed welcome+gate do its job; don't pile on.
	return ""
}
