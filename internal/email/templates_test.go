package email

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The post-payment launch nudge and the founders' ops alert must parse, fill
// their fields, and escape alert text (it carries user emails and names).
func TestNewTemplatesRender(t *testing.T) {
	tr, err := NewTemplateRenderer("../../templates")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"outreach-launch-nudge", "ops-alert"} {
		if err := tr.LoadTemplate(name); err != nil {
			t.Fatalf("load %s: %v", name, err)
		}
	}
	html, err := tr.Render("outreach-launch-nudge", map[string]interface{}{"UserName": "Likhitha", "Credits": 500, "ActionURL": "https://studojo.com/outreach/campaign/setup"})
	if err != nil || !strings.Contains(html, "your 500 email credits are ready") || !strings.Contains(html, "https://studojo.com/outreach/campaign/setup") {
		t.Fatalf("nudge render: %v\n%s", err, html)
	}
	html, err = tr.Render("outreach-launch-nudge", map[string]interface{}{"UserName": "x", "Credits": 0, "ActionURL": "https://studojo.com/"})
	if err != nil || strings.Contains(html, "credits are ready") {
		t.Fatalf("nudge without credits should omit the count: %v", err)
	}
	html, err = tr.Render("ops-alert", map[string]interface{}{"Subject": "s", "Message": "a <b>\nline2"})
	if err != nil || !strings.Contains(html, "a &lt;b&gt;") {
		t.Fatalf("ops-alert must escape: %v\n%s", err, html)
	}
}

// Campaign lifecycle notices must parse and fill their numbers.
func TestCampaignNoticeTemplatesRender(t *testing.T) {
	tr, err := NewTemplateRenderer("../../templates")
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"outreach-gmail-reconnect":   "40 emails still waiting",
		"outreach-campaign-paused":   "40 emails you paid for",
		"outreach-campaign-stalled":  "40 still waiting",
		"outreach-campaign-finished": "180 of 200",
	}
	for name, want := range cases {
		if err := tr.LoadTemplate(name); err != nil {
			t.Fatalf("load %s: %v", name, err)
		}
		html, err := tr.Render(name, map[string]interface{}{
			"UserName": "Asha", "Credits": 40, "ActionURL": "https://studojo.com/outreach/campaign/dashboard",
			"Delivered": 180, "Total": 200, "Replied": 7,
		})
		if name == "outreach-campaign-finished" {
			want = "180 of 200"
		}
		if err != nil || !strings.Contains(html, want) || !strings.Contains(html, "Asha") {
			t.Fatalf("%s: %v (want %q)", name, err, want)
		}
	}
}

// Every template, scanned as a file: at most one greeting line (four notices
// said "Hi <name>," twice, audit PS-N09) and no link to /support, which
// 404s (audit ST-N06; /contact is the help page).
func TestTemplatesHaveOneGreetingAndNoDeadHelpLink(t *testing.T) {
	entries, err := os.ReadDir("../../templates")
	if err != nil {
		t.Fatal(err)
	}
	greeting := regexp.MustCompile(`(?m)^\s*<p>\s*(Hi|Hey|Hello)\b`)
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".html") {
			continue
		}
		b, err := os.ReadFile(filepath.Join("../../templates", e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if n := len(greeting.FindAll(b, -1)); n > 1 {
			t.Errorf("%s: %d greeting lines, want at most 1", e.Name(), n)
		}
		if strings.Contains(string(b), "studojo.com/support") {
			t.Errorf("%s: links to /support, which 404s; use /contact", e.Name())
		}
	}
}

// The outreach receipt (audit PS-N10) shows what was bought and links to
// Launch, not to the old LinkedIn/Internship Dojo copy.
func TestPaymentReceiptRenders(t *testing.T) {
	tr, err := NewTemplateRenderer("../../templates")
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.LoadTemplate("payment-thankyou"); err != nil {
		t.Fatal(err)
	}
	html, err := tr.Render("payment-thankyou", map[string]interface{}{
		"UserName": "Asha", "PlanName": "Pro", "Credits": 350, "Amount": "Rs 2,325", "OrderID": "order_9",
		"ActionURL": "https://studojo.com/outreach/campaign/setup",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Asha", "350", "Rs 2,325", "order_9", "https://studojo.com/outreach/campaign/setup"} {
		if !strings.Contains(html, want) {
			t.Errorf("receipt missing %q", want)
		}
	}
	if strings.Contains(html, "LinkedIn") || strings.Contains(html, "unsubscribe") {
		t.Error("receipt must be outreach-only and carry no unsubscribe link")
	}
}

// Coupon emails carry the code to the site, so a student who already has
// leads lands on them with the coupon filled in (audit NEW-07).
func TestCouponEmailsCarryTheCode(t *testing.T) {
	tr, err := NewTemplateRenderer("../../templates")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"cc-outreach-coupon", "cc-cart-goat"} {
		if err := tr.LoadTemplate(name); err != nil {
			t.Fatalf("load %s: %v", name, err)
		}
		html, err := tr.Render(name, map[string]interface{}{"UserName": "A", "CouponCode": "SAVE20"})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !strings.Contains(html, "outreach?coupon=SAVE20") {
			t.Errorf("%s: outreach link does not carry the coupon", name)
		}
	}
}
