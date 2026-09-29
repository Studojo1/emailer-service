package email

import (
	"html"
	"net/url"
	"regexp"
	"strings"
)

// Every link from an email to a Studojo page is tagged
// utm_source=email&utm_medium=lifecycle&utm_campaign=<template>, so a visit that
// starts in an email is credited to the email that sent it rather than showing
// up as direct traffic. This runs on the rendered HTML, so no template has to
// remember to do it and a new template is tagged automatically.
//
// Left untouched:
//   - links that already carry a utm_source (a template chose its own tags)
//   - anything that is not a Studojo page (Google Meet, mailto:, fonts)
//   - /api/ and /v1/ paths and anything with a token-like parameter, because
//     those are signed or single-use (reset, verify, unsubscribe, quick
//     register) and re-encoding their query string is not worth the risk
//
// Click-tracked links (/v1/email/click/<id>?u=<dest>) have their destination
// tagged, so the tracker still records the click and the visit still arrives
// tagged.

var hrefRe = regexp.MustCompile(`href="([^"]*)"`)

// Parameters that mark a link as signed or single-use.
var tokenParams = []string{"token", "t", "sig", "signature", "code", "uid"}

func tagEmailLinks(htmlContent, templateName string) string {
	campaign := strings.ReplaceAll(strings.ToLower(templateName), "-", "_")
	return hrefRe.ReplaceAllStringFunc(htmlContent, func(m string) string {
		raw := html.UnescapeString(m[len(`href="`) : len(m)-1])
		tagged, ok := tagEmailURL(raw, campaign)
		if !ok {
			return m
		}
		return `href="` + html.EscapeString(tagged) + `"`
	})
}

func tagEmailURL(raw, campaign string) (string, bool) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return raw, false
	}

	if strings.Contains(u.Path, "/v1/email/click/") {
		q := u.Query()
		dest := q.Get("u")
		if dest == "" {
			return raw, false
		}
		taggedDest, ok := tagEmailURL(dest, campaign)
		if !ok {
			return raw, false
		}
		q.Set("u", taggedDest)
		u.RawQuery = q.Encode()
		return u.String(), true
	}

	if !isStudojoHost(u.Hostname()) {
		return raw, false
	}
	if strings.HasPrefix(u.Path, "/api/") || strings.HasPrefix(u.Path, "/v1/") ||
		strings.Contains(u.Path, "unsubscribe") {
		return raw, false
	}
	q := u.Query()
	if q.Get("utm_source") != "" {
		return raw, false
	}
	for _, p := range tokenParams {
		if q.Has(p) {
			return raw, false
		}
	}

	q.Set("utm_source", "email")
	q.Set("utm_medium", "lifecycle")
	q.Set("utm_campaign", campaign)
	u.RawQuery = q.Encode()
	return u.String(), true
}

func isStudojoHost(h string) bool {
	h = strings.ToLower(h)
	return h == "studojo.com" || strings.HasSuffix(h, ".studojo.com") ||
		h == "studojo.pro" || strings.HasSuffix(h, ".studojo.pro")
}
