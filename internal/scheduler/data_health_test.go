package scheduler

import (
	"testing"

	"github.com/studojo/emailer-service/internal/store"
)

func TestDataHealthChecksAreWellFormed(t *testing.T) {
	seen := map[string]bool{}
	for _, c := range store.DataHealthChecks {
		if c.Name == "" || c.What == "" || c.Fix == "" {
			t.Errorf("check %q is missing a name, description or fix hint", c.Name)
		}
		if seen[c.Name] {
			t.Errorf("duplicate check name %q", c.Name)
		}
		seen[c.Name] = true
	}
	for _, want := range []string{"users_without_login", "resumes_without_profile", "duplicate_unused_candidates", "quiz_completed_without_roles",
		"sends_outside_window", "campaign_over_paid_credits", "emails_stuck_sending", "followup_after_reply",
		"reply_check_stale", "unpaid_campaign_setup", "fresh_campaigns_low_reply_rate",
		"launch_week_low_reply_rate", "paid_credits_idle", "paid_orders_no_delivery"} {
		if !seen[want] {
			t.Errorf("check %q was removed; it guards a failure that reached real students", want)
		}
	}
}

func TestOnlyProductionPages(t *testing.T) {
	if !isProductionFrontend("https://studojo.com") {
		t.Error("studojo.com must page")
	}
	for _, u := range []string{"https://studojo.pro", "http://localhost:3000", ""} {
		if isProductionFrontend(u) {
			t.Errorf("%q must not page", u)
		}
	}
}

// UC-Q16 (B2C audit 30 Sep): a swallowed funnel write used to leave only a log
// line; the stage_tracking_failed row it now writes must page.
func TestFunnelStageWriteFailuresPage(t *testing.T) {
	for _, c := range store.DataHealthChecks {
		if c.Name == "funnel_stage_write_failed" {
			if c.Threshold != 0 {
				t.Errorf("funnel_stage_write_failed must page on the first failure, threshold is %d", c.Threshold)
			}
			return
		}
	}
	t.Error("check funnel_stage_write_failed was removed; it guards a failure that reached real students")
}
