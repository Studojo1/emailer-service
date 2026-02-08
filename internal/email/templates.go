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
	templates := []string{"welcome", "forgot-password", "resume-optimized", "internship-applied", "password-changed"}
	for _, name := range templates {
		if err := tr.LoadTemplate(name); err != nil {
			return fmt.Errorf("failed to load template %s: %w", name, err)
		}
	}
	return nil
}

