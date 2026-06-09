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
		// System
		"welcome", "forgot-password", "resume-optimized", "internship-applied",
		"password-changed", "contact-form",
		// Legacy nurture
		"nurture-day3", "nurture-day7", "nurture-day14", "nurture-day30",
		// Funnel — 01 Welcome
		"funnel-welcome-new", "funnel-welcome-existing",
		// Funnel — 02 Follow-up
		"funnel-followup-v1", "funnel-followup-v2", "funnel-followup-v3",
		// Funnel — 03 Segmentation
		"funnel-segmentation-v1", "funnel-segmentation-v2",
		// Funnel — 04 Exploration
		"funnel-exploration-v1", "funnel-exploration-v2",
		// Funnel — 05–06
		"funnel-congratulations", "funnel-comparison",
		// Funnel — 07 Pitching
		"funnel-pitching-v1", "funnel-pitching-v2", "funnel-pitching-v3",
		// Funnel — 08 Honest Question
		"funnel-honest-question-v1", "funnel-honest-question-v2", "funnel-honest-question-v3",
		// Funnel — 09–10
		"funnel-onboarding",
		"funnel-recognition-v1", "funnel-recognition-v2", "funnel-recognition-v3", "funnel-recognition-v4",
		// Funnel — 11–16
		"funnel-testimonial", "funnel-pricing", "funnel-case-study",
		"funnel-walkthrough", "funnel-educational", "funnel-winback",
		// Outreach funnel — order stage triggers
		"leads-ready",
		// Career Coach
		"checkin-reminder",
		// Priority transactional
		"signup-thankyou", "signup-followup", "payment-thankyou", "service-update",
		// Welcome variants (A/B testing)
		"signup-welcome-v1", "signup-welcome-v2", "signup-welcome-v3",
		"signup-welcome-v4", "signup-welcome-v5",
		// ── Career Coach / new efficient flows (cc- prefix) ──
		// Outreach Dojo flow
		"cc-welcome-new-user",
		"cc-outreach-nudge-d1", "cc-outreach-nudge-d2", "cc-outreach-nudge-d3", "cc-outreach-nudge-d4",
		"cc-outreach-push1", "cc-outreach-push2", "cc-outreach-push3",
		"cc-outreach-convert1", "cc-outreach-convert2",
		"cc-outreach-payment-page", "cc-outreach-coupon",
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
		// Old user re-engagement
		"cc-old-1", "cc-old-2", "cc-old-3",
	}
	for _, name := range templates {
		if err := tr.LoadTemplate(name); err != nil {
			return fmt.Errorf("failed to load template %s: %w", name, err)
		}
	}
	return nil
}

