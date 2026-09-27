package email

import (
	"net/url"
	"strings"
	"testing"
)

func TestTagEmailLinks(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{
			"plain studojo link is tagged",
			`<a href="https://studojo.com/outreach">Go</a>`,
			`<a href="https://studojo.com/outreach?utm_campaign=cc_outreach_nudge_d1&amp;utm_medium=lifecycle&amp;utm_source=email">Go</a>`,
		},
		{
			"existing query and fragment kept",
			`<a href="https://studojo.com/dojos/internships?tab=new#top">Go</a>`,
			`<a href="https://studojo.com/dojos/internships?tab=new&amp;utm_campaign=cc_outreach_nudge_d1&amp;utm_medium=lifecycle&amp;utm_source=email#top">Go</a>`,
		},
		{
			"template's own tags win",
			`<a href="https://studojo.com/x?utm_source=partner">Go</a>`,
			`<a href="https://studojo.com/x?utm_source=partner">Go</a>`,
		},
		{
			"external link untouched",
			`<a href="https://meet.google.com/abc">Join</a>`,
			`<a href="https://meet.google.com/abc">Join</a>`,
		},
		{
			"api link untouched",
			`<a href="https://studojo.com/api/auth/verify-email?token=abc&amp;callbackURL=/">Verify</a>`,
			`<a href="https://studojo.com/api/auth/verify-email?token=abc&amp;callbackURL=/">Verify</a>`,
		},
		{
			"signed quick-register link untouched",
			`<a href="https://studojo.com/webinar/quick-register?t=abc.def">Yes</a>`,
			`<a href="https://studojo.com/webinar/quick-register?t=abc.def">Yes</a>`,
		},
		{
			"mailto untouched",
			`<a href="mailto:admin@studojo.com">Mail</a>`,
			`<a href="mailto:admin@studojo.com">Mail</a>`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := tagEmailLinks(c.in, "cc-outreach-nudge-d1")
			if got != c.want {
				t.Errorf("\n got: %s\nwant: %s", got, c.want)
			}
		})
	}
}

func TestTagEmailLinksClickTracked(t *testing.T) {
	dest := url.QueryEscape("https://studojo.com/outreach/onboarding/upload")
	in := `<a href="https://email.studojo.com/v1/email/click/cc-welcome__a@b.com__1?u=` + dest + `">Start</a>`
	got := tagEmailLinks(in, "cc-welcome")

	start := strings.Index(got, `href="`) + len(`href="`)
	href := strings.ReplaceAll(got[start:strings.Index(got[start:], `"`)+start], "&amp;", "&")
	u, err := url.Parse(href)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(u.Path, "/v1/email/click/cc-welcome__a@b.com__1") {
		t.Fatalf("tracker path changed: %s", u.Path)
	}
	d, err := url.Parse(u.Query().Get("u"))
	if err != nil {
		t.Fatal(err)
	}
	q := d.Query()
	if d.Path != "/outreach/onboarding/upload" || q.Get("utm_source") != "email" ||
		q.Get("utm_medium") != "lifecycle" || q.Get("utm_campaign") != "cc_welcome" {
		t.Errorf("destination not tagged correctly: %s", d.String())
	}
}
