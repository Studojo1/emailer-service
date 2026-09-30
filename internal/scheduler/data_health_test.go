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
