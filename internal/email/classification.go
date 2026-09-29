package email

// Email classification (Privacy Policy v2.0 §14).
//
//   - Marketing emails are tips, reminders and offers, including lifecycle
//     nurture sequences. Every one carries the unsubscribe footer and the
//     RFC 8058 List-Unsubscribe headers, and is skipped for anyone who has
//     opted out.
//   - Service emails are receipts, confirmations, campaign updates, reconnect
//     requests, security notices and policy notices. They carry no unsubscribe
//     link or header and are never skipped because of a marketing opt-out.
//
// Every file in templates/ must appear in exactly one of the three sets below
// (TestEveryTemplateIsClassified enforces it), so a new template cannot ship
// without someone deciding which kind it is.

// serviceTemplates are sent because of something the user did or bought, or
// because we must tell them about their account.
var serviceTemplates = map[string]bool{
	// Account security
	"forgot-password":  true,
	"verify-email":     true,
	"password-changed": true,
	// Receipts and confirmations of the user's own action
	"payment-thankyou":   true,
	"resume-optimized":   true,
	"internship-applied": true,
	"cc-dna-ready":       true, // the analysis the user asked the coach to build
	"cc-webinar-confirm": true, // "you're registered"
	"cc-webinar-link":    true, // join link for a webinar they registered for
	"cc-webinar-toolkit": true, // day-before email that carries the join link
	// Outreach Dojo campaign updates for a paid campaign
	"outreach-launch-nudge":      true,
	"outreach-gmail-reconnect":   true,
	"outreach-campaign-paused":   true,
	"outreach-campaign-stalled":  true,
	"outreach-campaign-finished": true,
	// Notices about the service itself
	"service-update": true,
	"policy-update":  true,
	// Internal: contact-form copies and founders' ops alerts
	"contact-form": true,
	"ops-alert":    true,
}

// marketingTemplates are tips, reminders, offers and nurture sequences.
var marketingTemplates = map[string]bool{
	// Onboarding / lifecycle
	"welcome":             true,
	"cc-welcome":          true,
	"cc-welcome-new-user": true,
	"leads-ready":         true, // "finish setup" nudge with a WELCOME10 offer
	"checkin-reminder":    true,
	// Outreach Dojo nurture and offers
	"cc-outreach-nudge-d1": true, "cc-outreach-nudge-d2": true,
	"cc-outreach-nudge-d3": true, "cc-outreach-nudge-d4": true,
	"cc-outreach-push1": true, "cc-outreach-push2": true, "cc-outreach-push3": true,
	"cc-outreach-convert1": true, "cc-outreach-convert2": true,
	"cc-outreach-payment-page": true, "cc-outreach-coupon": true,
	"cc-outreach-pricing": true, "cc-cart-goat": true,
	// Webinar promotion (the confirmation and join-link emails are service)
	"cc-webinar-toolkit-recap": true,
	"cc-webinar-funnel-all":    true, "cc-webinar-funnel-outreach": true,
	"cc-webinar-funnel-coach": true, "cc-webinar-funnel-resume": true,
	// Career Coach nurture
	"cc-nudge-1": true, "cc-nudge-2": true, "cc-nudge-3": true,
	"cc-profiling-idle-1": true, "cc-profiling-idle-2": true, "cc-profiling-idle-3": true,
	"cc-dna-confirm-nudge": true, "cc-roadmap-delivered": true,
	"cc-checkin-1": true, "cc-checkin-2": true, "cc-checkin-3": true,
	"cc-upskill-nudge": true, "cc-coupon-unlock": true, "cc-dormant": true,
	"cc-to-outreach": true,
	"cc-returning-1": true, "cc-returning-2": true, "cc-returning-3": true,
	// Resume Maker nurture
	"cc-rm-strong-1": true, "cc-rm-strong-2": true, "cc-rm-strong-3": true,
	"cc-rm-weak-1": true, "cc-rm-weak-2": true, "cc-rm-weak-3": true,
	// Internship Dojo nurture
	"cc-id-two-tools": true, "cc-id-reengage-1": true, "cc-id-reengage-2": true,
	// Old / dormant user re-engagement
	"cc-old-1": true, "cc-old-2": true, "cc-old-3": true,
	"cc-old-s1-1": true, "cc-old-s1-2": true, "cc-old-s1-3": true,
	"cc-old-s2-1": true, "cc-old-s2-2": true, "cc-old-s2-3": true,
	"cc-old-s3-1": true, "cc-old-s3-2": true, "cc-old-s3-3": true,
}

// templateFragments are not emails: base.html is the wrapper layout and the
// cc-old-cta-* files are reference blocks for the dormant-user closing CTA.
var templateFragments = map[string]bool{
	"base":                true,
	"cc-old-cta-coach":    true,
	"cc-old-cta-outreach": true,
	"cc-old-cta-two-tool": true,
}

// IsMarketingTemplate reports whether a send of this template is marketing
// (opt-out gated, List-Unsubscribe headers). Anything not explicitly listed as
// a service email counts, including the CTA fragments if someone sends one
// directly, so an unclassified template errs towards honouring opt-outs.
func IsMarketingTemplate(name string) bool {
	return !serviceTemplates[name]
}

// IsServiceTemplate reports whether a template is a service email.
func IsServiceTemplate(name string) bool {
	return serviceTemplates[name]
}
