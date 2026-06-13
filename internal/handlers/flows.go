package handlers

import (
	"net/http"
	"strings"
)

// flowStep / flowDef describe one campaign sequence for the dashboard Flows view.
// Templates use the hyphen form (matching email_send_log.template_name).
type flowStep struct {
	Label    string
	Template string
	Delay    string
}
type flowDef struct {
	ID      string
	Title   string
	Trigger string
	Kind    string // instant | deferred | spread | coach
	Steps   []flowStep
}

// dashboardFlows mirrors the wired sequences in events.go (kept in sync by hand).
var dashboardFlows = []flowDef{
	{ID: "outreach-not", Title: "Outreach Dojo · not used", Trigger: "event.cc.welcome_new_user", Kind: "instant", Steps: []flowStep{
		{"Welcome", "cc-welcome-new-user", "instant"},
		{"Nudge d1", "cc-outreach-nudge-d1", "+7h"},
		{"Nudge d2", "cc-outreach-nudge-d2", "+31h"},
		{"Nudge d3", "cc-outreach-nudge-d3", "+63h"},
		{"Nudge d4", "cc-outreach-nudge-d4", "+96h"},
	}},
	{ID: "outreach-used", Title: "Outreach Dojo · used", Trigger: "event.cc.outreach_used", Kind: "instant", Steps: []flowStep{
		{"Push 1", "cc-outreach-push1", "instant"},
		{"Push 2", "cc-outreach-push2", "+24h"},
		{"Push 3", "cc-outreach-push3", "+50h"},
		{"Convert 1", "cc-outreach-convert1", "+60h"},
		{"Convert 2", "cc-outreach-convert2", "+75h"},
	}},
	{ID: "abandoned", Title: "Abandoned checkout", Trigger: "event.cc.outreach_payment_page", Kind: "deferred", Steps: []flowStep{
		{"Payment page", "cc-outreach-payment-page", "+2h"},
		{"Founder coupon", "cc-outreach-coupon", "+6h"},
	}},
	{ID: "cc-welcome", Title: "Career Coach · not started", Trigger: "event.cc.welcome", Kind: "instant", Steps: []flowStep{
		{"Welcome", "cc-welcome", "instant"},
		{"Nudge 1", "cc-nudge-1", "+8h"},
		{"Nudge 2", "cc-nudge-2", "+32h"},
		{"Nudge 3", "cc-nudge-3", "+56h"},
	}},
	{ID: "dna", Title: "Post-DNA", Trigger: "event.cc.dna_ready", Kind: "instant", Steps: []flowStep{
		{"DNA ready", "cc-dna-ready", "instant"},
		{"Confirm nudge", "cc-dna-confirm-nudge", "+2d"},
		{"Check-in 1", "cc-checkin-1", "+4d"},
		{"Check-in 2", "cc-checkin-2", "+7d"},
		{"Check-in 3", "cc-checkin-3", "+10d"},
	}},
	{ID: "roadmap", Title: "Roadmap", Trigger: "event.cc.roadmap_delivered", Kind: "instant", Steps: []flowStep{
		{"Roadmap", "cc-roadmap-delivered", "instant"},
		{"Upskill nudge", "cc-upskill-nudge", "+7d"},
		{"Coupon unlock", "cc-coupon-unlock", "+9d"},
		{"Dormant", "cc-dormant", "+11d"},
		{"To outreach", "cc-to-outreach", "+14d"},
	}},
	{ID: "resume-strong", Title: "Resume Maker · strong", Trigger: "event.cc.resume_strong", Kind: "instant", Steps: []flowStep{
		{"Strong 1", "cc-rm-strong-1", "instant"},
		{"Strong 2", "cc-rm-strong-2", "+2d"},
		{"Strong 3", "cc-rm-strong-3", "+3d"},
	}},
	{ID: "resume-weak", Title: "Resume Maker · weak", Trigger: "event.cc.resume_weak", Kind: "instant", Steps: []flowStep{
		{"Weak 1", "cc-rm-weak-1", "instant"},
		{"Weak 2", "cc-rm-weak-2", "+2d"},
		{"Weak 3", "cc-rm-weak-3", "+3d"},
	}},
	{ID: "internship", Title: "Internship Dojo", Trigger: "event.cc.id_two_tools", Kind: "instant", Steps: []flowStep{
		{"Two tools", "cc-id-two-tools", "instant"},
		{"Re-engage 1", "cc-id-reengage-1", "+3d"},
		{"Re-engage 2", "cc-id-reengage-2", "+7d"},
	}},
	{ID: "old-s1", Title: "Old users · S1 (nearly finished)", Trigger: "event.cc.old_s1", Kind: "spread", Steps: []flowStep{
		{"S1 · 1", "cc-old-s1-1", "slot"},
		{"S1 · 2", "cc-old-s1-2", "+5d"},
		{"S1 · 3", "cc-old-s1-3", "+9d"},
	}},
	{ID: "old-s2", Title: "Old users · S2 (stalled)", Trigger: "event.cc.old_s2", Kind: "spread", Steps: []flowStep{
		{"S2 · 1", "cc-old-s2-1", "slot"},
		{"S2 · 2", "cc-old-s2-2", "+6d"},
		{"S2 · 3", "cc-old-s2-3", "+9d"},
	}},
	{ID: "old-s3", Title: "Old users · S3 (cold)", Trigger: "event.cc.old_s3", Kind: "spread", Steps: []flowStep{
		{"S3 · 1", "cc-old-s3-1", "slot"},
		{"S3 · 2", "cc-old-s3-2", "+7d"},
		{"S3 · 3", "cc-old-s3-3", "+14d"},
	}},
	{ID: "coach-returning", Title: "Returning (coach cron)", Trigger: "send-template", Kind: "coach", Steps: []flowStep{
		{"Returning 1", "cc-returning-1", "cron"},
		{"Returning 2", "cc-returning-2", "cron"},
		{"Returning 3", "cc-returning-3", "cron"},
	}},
}

type flowOutStep struct {
	Label    string  `json:"label"`
	Template string  `json:"template"`
	Delay    string  `json:"delay"`
	Sent     int     `json:"sent"`
	Opened   int     `json:"opened"`
	Pending  int     `json:"pending"`
	OpenRate float64 `json:"open_rate"`
	Drop     float64 `json:"drop"` // % fall in sent vs the previous step
	Choke    bool    `json:"choke"`
}
type flowOut struct {
	ID      string        `json:"id"`
	Title   string        `json:"title"`
	Trigger string        `json:"trigger"`
	Kind    string        `json:"kind"`
	Entered int           `json:"entered"` // sent count of the first step
	Steps   []flowOutStep `json:"steps"`
}

// HandleAdminFlows handles GET /v1/admin/flows — per-sequence funnel with the
// drop-off (choke point) between steps, computed from the authoritative send log.
func (h *Handler) HandleAdminFlows(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	counts, err := h.Store.GetSendCountsByTemplate(ctx)
	if err != nil {
		writeError(w, "failed to load send counts", http.StatusInternalServerError)
		return
	}
	pending, _ := h.Store.GetPendingByType(ctx)

	out := make([]flowOut, 0, len(dashboardFlows))
	for _, f := range dashboardFlows {
		fo := flowOut{ID: f.ID, Title: f.Title, Trigger: f.Trigger, Kind: f.Kind}
		prevSent := 0
		chokeIdx, maxDrop := -1, 0.0
		fo.Steps = make([]flowOutStep, len(f.Steps))
		for i, st := range f.Steps {
			c := counts[st.Template]
			os := flowOutStep{
				Label: st.Label, Template: st.Template, Delay: st.Delay,
				Sent: c.Sent, Opened: c.Opened,
				Pending: pending[strings.ReplaceAll(st.Template, "-", "_")],
			}
			if c.Sent > 0 {
				os.OpenRate = float64(c.Opened) / float64(c.Sent) * 100
			}
			if i == 0 {
				fo.Entered = c.Sent
			} else if prevSent > 0 {
				os.Drop = float64(prevSent-c.Sent) / float64(prevSent) * 100
				if os.Drop > maxDrop {
					maxDrop = os.Drop
					chokeIdx = i
				}
			}
			prevSent = c.Sent
			fo.Steps[i] = os
		}
		// Flag the worst drop as the choke point (only if a real fall happened).
		if chokeIdx >= 0 && maxDrop >= 15 {
			fo.Steps[chokeIdx].Choke = true
		}
		out = append(out, fo)
	}
	writeJSON(w, map[string]interface{}{"flows": out}, http.StatusOK)
}
