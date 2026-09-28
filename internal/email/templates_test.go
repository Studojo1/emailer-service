package email

import (
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
