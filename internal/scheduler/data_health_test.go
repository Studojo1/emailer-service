package scheduler

import (
	"strings"
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
		"launch_week_low_reply_rate", "paid_credits_idle", "paid_orders_no_delivery",
		"lead_quality_low", "duplicate_leads", "pods_crash_looping",
		"delivery_reports_missing", "transactional_bounce_rate_high"} {
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

// Recon of the 30 Sep audit: UC-Q07 (low lead quality), UC-Q36 (duplicate
// leads) and IN-N04 (pod crash loops) each wrote a signal that nobody read.
// These checks must keep reading the tables those signals land in.
func TestReconSignalsAreRead(t *testing.T) {
	want := map[string]string{
		"lead_quality_low":   "'lead_quality_low'",
		"duplicate_leads":    "FROM leads",
		"pods_crash_looping": "FROM ops_alerts",
	}
	for _, c := range store.DataHealthChecks {
		frag, ok := want[c.Name]
		if !ok {
			continue
		}
		if !strings.Contains(store.DataHealthQuery(c), frag) {
			t.Errorf("check %s no longer reads %s", c.Name, frag)
		}
		delete(want, c.Name)
	}
	for name := range want {
		t.Errorf("check %s was removed", name)
	}
}

// AR-A03 (areas audit 30 Sep): ACS delivery reports never arrived, so bounces
// were never suppressed. These checks must keep reading what the handler writes.
func TestDeliveryReportChecksReadTheReportTable(t *testing.T) {
	want := map[string]bool{"delivery_reports_missing": true, "transactional_bounce_rate_high": true}
	for _, c := range store.DataHealthChecks {
		if !want[c.Name] {
			continue
		}
		if !strings.Contains(store.DataHealthQuery(c), "FROM email_delivery_reports") {
			t.Errorf("check %s no longer reads email_delivery_reports", c.Name)
		}
		if c.Threshold != 0 {
			t.Errorf("check %s must page on the first violation", c.Name)
		}
		delete(want, c.Name)
	}
	for name := range want {
		t.Errorf("check %s was removed", name)
	}
}
