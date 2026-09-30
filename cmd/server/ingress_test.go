package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// ingressPath is one entry of k8s/mail-ingress.yaml.
type ingressPath struct {
	path     string
	pathType string
}

// loadMailIngressPaths reads the public email.studojo.com ingress that the
// deploy workflow applies on every deploy.
func loadMailIngressPaths(t *testing.T) []ingressPath {
	t.Helper()
	b, err := os.ReadFile("../../k8s/mail-ingress.yaml")
	if err != nil {
		t.Fatal(err)
	}
	re := regexp.MustCompile(`(?m)^\s*- path:\s*(\S+)\s*\n\s*pathType:\s*(\S+)`)
	var out []ingressPath
	for _, m := range re.FindAllStringSubmatch(string(b), -1) {
		out = append(out, ingressPath{path: m[1], pathType: m[2]})
	}
	if len(out) == 0 {
		t.Fatal("no paths found in k8s/mail-ingress.yaml")
	}
	return out
}

// routable reports whether ingress-nginx would send reqPath to the emailer,
// using the Kubernetes Exact / Prefix (element-wise) matching rules.
func routable(paths []ingressPath, reqPath string) bool {
	for _, p := range paths {
		switch p.pathType {
		case "Exact":
			if reqPath == p.path {
				return true
			}
		case "Prefix":
			prefix := strings.TrimSuffix(p.path, "/")
			if prefix == "" || reqPath == prefix || strings.HasPrefix(reqPath, prefix+"/") {
				return true
			}
		default:
			// ImplementationSpecific is regex/prefix in nginx; treat as a catch-all
			// so the test fails closed.
			return true
		}
	}
	return false
}

// Audit AS-N02: the public host must not expose the service-to-service routes.
// Callers inside the cluster use http://emailer-service:8087, browsers go
// through the control-plane gateway.
func TestMailIngressHidesInternalRoutes(t *testing.T) {
	paths := loadMailIngressPaths(t)
	for _, p := range []string{
		"/v1/email/events",
		"/v1/email/send-template",
		"/v1/email/bulk-send",
		"/v1/email/bulk-send/preview",
		"/v1/email/policy-update",
		"/v1/email/checkin-reminder",
		"/v1/email/forgot-password",
		"/v1/email/reset-password",
		"/v1/email/change-password",
		"/v1/email/preferences/some-user",
		"/v1/emailx",
		"/anything-else",
	} {
		if routable(paths, p) {
			t.Errorf("%s is reachable on the public email.studojo.com ingress", p)
		}
	}
}

// The links inside every sent email, and the admin email dashboard, must keep
// working through the public host.
func TestMailIngressKeepsPublicRoutes(t *testing.T) {
	paths := loadMailIngressPaths(t)
	for _, p := range []string{
		"/v1/email/track/some-track-id",
		"/v1/email/click/some-track-id",
		"/v1/email/unsubscribe",
		"/v1/email/webinar-link-cron",
		"/v1/email/delivery-report",
		"/v1/email/inbound",
		"/mail/",
		"/mail/index.html",
		"/v1/admin/stats",
		"/",
		"/health",
	} {
		if !routable(paths, p) {
			t.Errorf("%s is no longer reachable on email.studojo.com", p)
		}
	}
}
