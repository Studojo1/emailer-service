package email

import (
	"bytes"
	"fmt"
	"html/template"
	"os"
	"path/filepath"
)

// TemplateRenderer handles email template rendering
type TemplateRenderer struct {
	templates map[string]*template.Template
	basePath  string
}

// NewTemplateRenderer creates a new template renderer
func NewTemplateRenderer(templateDir string) (*TemplateRenderer, error) {
	tr := &TemplateRenderer{
		templates: make(map[string]*template.Template),
		basePath:  templateDir,
	}
	return tr, nil
}

// LoadTemplate loads a template by name.
// If the template file contains "<!DOCTYPE html>" it is loaded standalone
// (no base.html wrapper), allowing plain personal-style emails.
func (tr *TemplateRenderer) LoadTemplate(name string) error {
	templatePath := filepath.Join(tr.basePath, name+".html")

	raw, err := os.ReadFile(templatePath)
	if err != nil {
		return fmt.Errorf("failed to read template %s: %w", name, err)
	}

	var tmpl *template.Template
	if bytes.Contains(raw, []byte("<!DOCTYPE html>")) || bytes.Contains(raw, []byte("<!doctype html>")) {
		// Standalone template — no base wrapper
		tmpl, err = template.ParseFiles(templatePath)
		if err != nil {
			return fmt.Errorf("failed to parse standalone template %s: %w", name, err)
		}
	} else {
		// Wrapped template — combine with base
		basePath := filepath.Join(tr.basePath, "base.html")
		tmpl, err = template.ParseFiles(basePath, templatePath)
		if err != nil {
			return fmt.Errorf("failed to parse template %s: %w", name, err)
		}
	}

	tr.templates[name] = tmpl
	return nil
}

// Render renders a template with the given data
func (tr *TemplateRenderer) Render(name string, data interface{}) (string, error) {
	tmpl, ok := tr.templates[name]
	if !ok {
		return "", fmt.Errorf("template %s not found", name)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("failed to execute template %s: %w", name, err)
	}

	return buf.String(), nil
}

// LoadAllTemplates loads all email templates
func (tr *TemplateRenderer) LoadAllTemplates() error {
	templates := []string{
		// ── Transactional (kept; not part of the retired engagement flow) ──
		"welcome", "forgot-password", "resume-optimized", "internship-applied",
		"password-changed", "contact-form", "payment-thankyou", "service-update",
		"leads-ready", "checkin-reminder",
		// ── New efficient flow (cc- prefix) — replaces the old funnel/nurture flow ──
		// Outreach Dojo flow
		"cc-welcome-new-user",
		"cc-outreach-nudge-d1", "cc-outreach-nudge-d2", "cc-outreach-nudge-d3", "cc-outreach-nudge-d4",
		"cc-outreach-push1", "cc-outreach-push2", "cc-outreach-push3",
		"cc-outreach-convert1", "cc-outreach-convert2",
		"cc-outreach-payment-page", "cc-outreach-coupon",
		// One-off pricing announcement (bulk send to recent users)
		"cc-outreach-pricing",
		// Webinar flow
		"cc-webinar-confirm", "cc-webinar-link",
		// Career Coach flow
		"cc-welcome",
		"cc-nudge-1", "cc-nudge-2", "cc-nudge-3",
		"cc-profiling-idle-1", "cc-profiling-idle-2", "cc-profiling-idle-3",
		"cc-dna-ready", "cc-dna-confirm-nudge", "cc-roadmap-delivered",
		"cc-checkin-1", "cc-checkin-2", "cc-checkin-3",
		"cc-upskill-nudge", "cc-coupon-unlock", "cc-dormant",
		"cc-to-outreach",
		"cc-returning-1", "cc-returning-2", "cc-returning-3",
		// Resume Maker flow
		"cc-rm-strong-1", "cc-rm-strong-2", "cc-rm-strong-3",
		"cc-rm-weak-1", "cc-rm-weak-2", "cc-rm-weak-3",
		// Internship Dojo flow
		"cc-id-two-tools", "cc-id-reengage-1", "cc-id-reengage-2",
		// Old / dormant user flow (tool-neutral: 3 stages + 3 CTA variants)
		"cc-old-s1-1", "cc-old-s1-2", "cc-old-s1-3",
		"cc-old-s2-1", "cc-old-s2-2", "cc-old-s2-3",
		"cc-old-s3-1", "cc-old-s3-2", "cc-old-s3-3",
		"cc-old-cta-outreach", "cc-old-cta-coach", "cc-old-cta-two-tool",
		// One-shot abandoned-cart coupon blast (GOAT10)
		"cc-cart-goat",
	}
	for _, name := range templates {
		if err := tr.LoadTemplate(name); err != nil {
			return fmt.Errorf("failed to load template %s: %w", name, err)
		}
	}
	return nil
}

