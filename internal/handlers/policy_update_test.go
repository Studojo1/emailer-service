package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/studojo/emailer-service/internal/email"
	"github.com/studojo/emailer-service/internal/store"
)

// fakePolicyStore mimics scheduled_emails with its (user_id, email_type)
// unique index.
type fakePolicyStore struct {
	users []store.User
	rows  map[string]time.Time // user_id|email_type -> scheduled_at
}

func (f *fakePolicyStore) ListVerifiedUsers(ctx context.Context) ([]store.User, error) {
	return f.users, nil
}
func (f *fakePolicyStore) HasScheduledOrReceivedEmail(ctx context.Context, userID, emailType string) (bool, error) {
	_, ok := f.rows[userID+"|"+emailType]
	return ok, nil
}
func (f *fakePolicyStore) CreateScheduledEmail(ctx context.Context, userID, emailType string, at time.Time) error {
	k := userID + "|" + emailType
	if _, ok := f.rows[k]; !ok { // ON CONFLICT DO NOTHING
		f.rows[k] = at
	}
	return nil
}

func TestQueuePolicyUpdateDedupes(t *testing.T) {
	f := &fakePolicyStore{rows: map[string]time.Time{}}
	for _, id := range []string{"u1", "u2", "u3"} {
		f.users = append(f.users, store.User{ID: id, Email: id + "@example.com"})
	}
	eff := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

	q, s, failed, err := QueuePolicyUpdate(context.Background(), f, eff, now)
	if err != nil || q != 3 || s != 0 || failed != 0 {
		t.Fatalf("first run: queued=%d skipped=%d failed=%d err=%v", q, s, failed, err)
	}
	// Re-running for the same date queues nobody again.
	q, s, _, _ = QueuePolicyUpdate(context.Background(), f, eff, now)
	if q != 0 || s != 3 || len(f.rows) != 3 {
		t.Fatalf("re-run: queued=%d skipped=%d rows=%d", q, s, len(f.rows))
	}
	// A new signup is picked up on a re-run; nobody else is touched.
	f.users = append(f.users, store.User{ID: "u4", Email: "u4@example.com"})
	q, s, _, _ = QueuePolicyUpdate(context.Background(), f, eff, now)
	if q != 1 || s != 3 {
		t.Fatalf("re-run with new user: queued=%d skipped=%d", q, s)
	}
	// A different effective date is a separate notice.
	q, _, _, _ = QueuePolicyUpdate(context.Background(), f, eff.AddDate(0, 1, 0), now)
	if q != 4 {
		t.Fatalf("new date: queued=%d, want 4", q)
	}
	if _, ok := f.rows["u1|policy-update:2026-11-01"]; !ok {
		t.Fatalf("unexpected email_type keys: %v", f.rows)
	}
	if d, ok := ParsePolicyUpdateEmailType(PolicyUpdateEmailType(eff)); !ok || !d.Equal(eff) {
		t.Fatalf("round trip failed: %v %v", d, ok)
	}
	if FormatEffectiveDate(eff) != "1 November 2026" || FirstName("Asha Rao") != "Asha" || FirstName("") != "there" {
		t.Fatal("formatting helpers")
	}
	if email.IsMarketingTemplate("policy-update") {
		t.Fatal("policy-update must be a service email")
	}
}

// The trigger is internal-secret gated and refuses less than 7 days' notice.
func TestHandlePolicyUpdateGuards(t *testing.T) {
	t.Setenv("INTERNAL_SECRET", "s3cret")
	h := &Handler{}
	do := func(secret, body string) int {
		r := httptest.NewRequest(http.MethodPost, "/v1/email/policy-update", strings.NewReader(body))
		if secret != "" {
			r.Header.Set("X-Internal-Secret", secret)
		}
		w := httptest.NewRecorder()
		h.HandlePolicyUpdate(w, r)
		return w.Code
	}
	if c := do("", `{"effective_date":"2030-01-01"}`); c != http.StatusUnauthorized {
		t.Errorf("no secret: %d", c)
	}
	if c := do("wrong", `{"effective_date":"2030-01-01"}`); c != http.StatusUnauthorized {
		t.Errorf("wrong secret: %d", c)
	}
	soon := time.Now().UTC().AddDate(0, 0, 3).Format("2006-01-02")
	if c := do("s3cret", `{"effective_date":"`+soon+`"}`); c != http.StatusBadRequest {
		t.Errorf("3 days' notice: %d", c)
	}
	if c := do("s3cret", `{"effective_date":"next week"}`); c != http.StatusBadRequest {
		t.Errorf("bad date: %d", c)
	}
}

// GET shows the confirm page and never writes; a bad signature is rejected
// before anything else, on GET and POST alike.
func TestHandleUnsubscribeToken(t *testing.T) {
	const secret = "unsub-secret"
	h := &Handler{UnsubscribeSecret: secret} // nil Store: any write would panic
	good := email.UnsubscribeURL("https://email.studojo.com", secret, "user-1", "")
	path := strings.TrimPrefix(good, "https://email.studojo.com")

	w := httptest.NewRecorder()
	h.HandleUnsubscribe(w, httptest.NewRequest(http.MethodGet, path, nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "Unsubscribe from tips and offers?") ||
		!strings.Contains(w.Body.String(), `method="POST"`) {
		t.Fatalf("GET: %d %s", w.Code, w.Body.String())
	}

	for _, bad := range []string{
		strings.Replace(path, "uid=user-1", "uid=user-2", 1),
		path[:len(path)-4] + "0000",
		email.UnsubscribePath + "?uid=user-1",
		email.UnsubscribePath,
	} {
		for _, m := range []string{http.MethodGet, http.MethodPost} {
			w := httptest.NewRecorder()
			h.HandleUnsubscribe(w, httptest.NewRequest(m, bad, strings.NewReader("List-Unsubscribe=One-Click")))
			if w.Code != http.StatusBadRequest {
				t.Errorf("%s %s: %d, want 400", m, bad, w.Code)
			}
		}
	}
}
