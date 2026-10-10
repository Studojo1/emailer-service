package handlers

import (
	"context"
	"encoding/json"
	"html"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/studojo/emailer-service/internal/email"
	"github.com/studojo/emailer-service/internal/store"
)

// sendLeadsReady sends leads-ready, with the data the admin send builds, to a
// local stand-in for the mail provider and returns the HTML it received: the
// email as sent, tracking filled in and links tagged.
func sendLeadsReady(t *testing.T, trackingURL string) string {
	t.Helper()
	var mu sync.Mutex
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p struct {
			Content struct {
				HTML string `json:"html"`
			} `json:"content"`
		}
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &p)
		mu.Lock()
		got = p.Content.HTML
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	client, err := email.NewClient("endpoint="+srv.URL+";accesskey=a2V5", "no-reply@studojo.com")
	if err != nil {
		t.Fatal(err)
	}
	tr, err := email.NewTemplateRenderer("../../templates")
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.LoadTemplate("leads-ready"); err != nil {
		t.Fatal(err)
	}
	sender := email.NewSender(client, tr)
	sender.SetUnsubscribeSecret("test-secret", "https://email.studojo.com")
	if trackingURL != "" {
		sender.SetTrackingURL(trackingURL)
	}

	h := &Handler{EmailFrontendURL: "https://studojo.com"}
	name, data := h.buildTemplateData("leads_ready", &store.User{Name: "Asha"})
	if err := sender.SendTemplateEmail(context.Background(), "asha@example.com", name, data); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if got == "" {
		t.Fatal("nothing reached the provider")
	}
	return got
}

var leadsReadyButton = regexp.MustCompile(`<a href="([^"]*)" class="btn">`)

func buttonHref(t *testing.T, out string) string {
	t.Helper()
	m := leadsReadyButton.FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("no button in:\n%s", out)
	}
	return html.UnescapeString(m[1])
}

// The leads-ready button opens /outreach/results, which signs the student in
// and routes them to their leads. The /outreach landing page it used to open
// restarts resume upload. With tracking on, the click goes through the
// tracker, which redirects there; either way the visit keeps its email tags.
func TestLeadsReadyButtonOpensResults(t *testing.T) {
	const results = "https://studojo.com/outreach/results?utm_campaign=leads_ready&utm_medium=lifecycle&utm_source=email"

	if href := buttonHref(t, sendLeadsReady(t, "")); href != results {
		t.Errorf("untracked button = %q, want %q", href, results)
	}

	href := buttonHref(t, sendLeadsReady(t, "https://email.studojo.com"))
	u, err := url.Parse(href)
	if err != nil {
		t.Fatal(err)
	}
	if u.Host != "email.studojo.com" || !strings.HasPrefix(u.Path, "/v1/email/click/leads-ready__asha@example.com__") {
		t.Fatalf("button does not go through the click tracker: %s", href)
	}
	if dest := u.Query().Get("u"); dest != results {
		t.Fatalf("tracked destination = %q, want %q", dest, results)
	}

	// Called without the mux, so no track id is parsed and the nil Store is
	// never asked to record the click.
	w := httptest.NewRecorder()
	(&Handler{EmailFrontendURL: "https://studojo.com"}).HandleTrackClick(w, httptest.NewRequest(http.MethodGet, u.RequestURI(), nil))
	if w.Code != http.StatusFound || w.Header().Get("Location") != results {
		t.Fatalf("click redirect = %d %q, want 302 %q", w.Code, w.Header().Get("Location"), results)
	}
}

// Every line must hold for a student who has leads and has not paid:
// WELCOME10 has no end date, nothing is drafted before payment, and the
// email promises no setup time. No em or en dashes.
func TestLeadsReadyCopy(t *testing.T) {
	out := html.UnescapeString(sendLeadsReady(t, "https://email.studojo.com"))
	for _, gone := range []string{"expires in 24 hours", "messages we drafted", "under 3 minutes", "Finish setup"} {
		if strings.Contains(out, gone) {
			t.Errorf("leads-ready still says %q", gone)
		}
	}
	for _, want := range []string{"Hi Asha,", "1. Pick a pack.", "2. Connect your Gmail.", "3. Launch.", ">See your hiring managers</a>", "WELCOME10"} {
		if !strings.Contains(out, want) {
			t.Errorf("leads-ready is missing %q", want)
		}
	}
	if strings.ContainsAny(out, string([]rune{0x2013, 0x2014})) {
		t.Error("leads-ready contains an em or en dash")
	}
}
