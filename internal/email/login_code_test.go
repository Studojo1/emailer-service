package email

import (
	"strings"
	"testing"
)

func TestLoginCodeEmail(t *testing.T) {
	tr, err := NewTemplateRenderer("../../templates")
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.LoadAllTemplates(); err != nil {
		t.Fatal(err)
	}
	html, err := tr.Render("login-code", map[string]interface{}{"Code": "042917"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(html, "042917") || !strings.Contains(html, "10 minutes") {
		t.Errorf("login-code should show the code and how long it lasts")
	}
	s := &Sender{}
	subj, err := s.getSubject("login-code", map[string]interface{}{"Code": "042917"})
	if err != nil || subj != "042917 is your Studojo sign-in code" {
		t.Errorf("subject = %q, %v", subj, err)
	}
	if !transactionalTemplates["login-code"] {
		t.Error("login-code must never wait behind the marketing rate limit")
	}
	if !serviceTemplates["login-code"] {
		t.Error("login-code is account security: a service email, sent even to marketing opt-outs")
	}
}
