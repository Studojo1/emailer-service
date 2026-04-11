package email

import (
	"bytes"
	"fmt"
	"html/template"
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

// LoadTemplate loads a template by name
func (tr *TemplateRenderer) LoadTemplate(name string) error {
	basePath := filepath.Join(tr.basePath, "base.html")
	templatePath := filepath.Join(tr.basePath, name+".html")

	// Parse both base and specific template together
	tmpl, err := template.ParseFiles(basePath, templatePath)
	if err != nil {
		return fmt.Errorf("failed to parse template %s: %w", name, err)
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
	}
	for _, name := range templates {
		if err := tr.LoadTemplate(name); err != nil {
			return fmt.Errorf("failed to load template %s: %w", name, err)
		}
	}
	return nil
}

