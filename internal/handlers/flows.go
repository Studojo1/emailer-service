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
		{"Nudge 1", "cc-nudge-1", "+7h (if no open/click)"},
		{"Nudge 2", "cc-nudge-2", "+31h"},
		{"Nudge 3", "cc-nudge-3", "+55h"},
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
	Label     string  `json:"label"`
	Template  string  `json:"template"`
	Delay     string  `json:"delay"`
	Sent      int     `json:"sent"`
	Opened    int     `json:"opened"`
	Clicked   int     `json:"clicked"`
	Pending   int     `json:"pending"`
	OpenRate  float64 `json:"open_rate"`
	ClickRate float64 `json:"click_rate"`
	Drop      float64 `json:"drop"` // % fall in sent vs the previous step
	Choke     bool    `json:"choke"`
}
type flowOut struct {
	ID      string        `json:"id"`
	Title   string        `json:"title"`
	Trigger string        `json:"trigger"`
	Kind    string        `json:"kind"`
	Entered int           `json:"entered"` // sent count of the first step
	Health  string        `json:"health"`  // idle | healthy | choke
	Steps   []flowOutStep `json:"steps"`
	// Flow-wide tracking summary (across all steps) for the dashboard.
	TotalSent     int     `json:"total_sent"`
	TotalOpened   int     `json:"total_opened"`
	TotalClicked  int     `json:"total_clicked"`
	TotalIgnored  int     `json:"total_ignored"` // sent but never opened
	TotalPending  int     `json:"total_pending"` // still in flight
	OpenRate      float64 `json:"open_rate"`
	ClickRate     float64 `json:"click_rate"`
	IgnoredRate   float64 `json:"ignored_rate"`
}

// HandleAdminFlows handles GET /v1/admin/flows — per-sequence funnel with opens,
// clicks, and a single meaningful choke point. A step is only a choke if real
// people fell out (the previous step had sends, this step is much lower, and the
// gap is NOT just emails still waiting in the queue).
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
			pend := pending[strings.ReplaceAll(st.Template, "-", "_")]
			os := flowOutStep{
				Label: st.Label, Template: st.Template, Delay: st.Delay,
				Sent: c.Sent, Opened: c.Opened, Clicked: c.Clicked, Pending: pend,
			}
			if c.Sent > 0 {
				os.OpenRate = float64(c.Opened) / float64(c.Sent) * 100
				os.ClickRate = float64(c.Clicked) / float64(c.Sent) * 100
			}
			// Flow-wide totals.
			fo.TotalSent += c.Sent
			fo.TotalOpened += c.Opened
			fo.TotalClicked += c.Clicked
			fo.TotalPending += pend
			if i == 0 {
				fo.Entered = c.Sent
			} else if prevSent > 0 {
				os.Drop = float64(prevSent-c.Sent) / float64(prevSent) * 100
				// Real choke only: the shortfall isn't explained by emails still
				// queued. If (sent + pending) ~ prevSent, people are mid-flight,
				// not dropping out.
				inFlightExplains := (c.Sent + pend) >= prevSent
				if os.Drop > maxDrop && !inFlightExplains {
					maxDrop = os.Drop
					chokeIdx = i
				}
			}
			prevSent = c.Sent
			fo.Steps[i] = os
		}
		// Ignored = sent but never opened; rates across the whole flow.
		fo.TotalIgnored = fo.TotalSent - fo.TotalOpened
		if fo.TotalIgnored < 0 {
			fo.TotalIgnored = 0
		}
		if fo.TotalSent > 0 {
			fo.OpenRate = float64(fo.TotalOpened) / float64(fo.TotalSent) * 100
			fo.ClickRate = float64(fo.TotalClicked) / float64(fo.TotalSent) * 100
			fo.IgnoredRate = float64(fo.TotalIgnored) / float64(fo.TotalSent) * 100
		}
		if chokeIdx >= 0 && maxDrop >= 25 {
			fo.Steps[chokeIdx].Choke = true
			fo.Health = "choke"
		} else if fo.Entered > 0 {
			fo.Health = "healthy"
		} else {
			fo.Health = "idle"
		}
		out = append(out, fo)
	}
	writeJSON(w, map[string]interface{}{"flows": out}, http.StatusOK)
}

// HandleAdminSignups handles GET /v1/admin/signups — signup counts by rolling
// window, plus per-flow entry counts (first-step sends) so the dashboard can show
// how many people signed up and which trigger flow they entered.
func (h *Handler) HandleAdminSignups(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	type firstStep struct{ id, title, tpl string }
	firsts := make([]firstStep, 0, len(dashboardFlows))
	tpls := make([]string, 0, len(dashboardFlows))
	for _, f := range dashboardFlows {
		if len(f.Steps) == 0 {
			continue
		}
		firsts = append(firsts, firstStep{f.ID, f.Title, f.Steps[0].Template})
		tpls = append(tpls, f.Steps[0].Template)
	}

	windows, rows, err := h.Store.GetSignupStats(ctx, tpls)
	if err != nil {
		writeError(w, "failed to load signup stats", http.StatusInternalServerError)
		return
	}

	type outRow struct {
		ID      string `json:"id"`
		Title   string `json:"title"`
		Total   int    `json:"total"`
		Last24h int    `json:"last_24h"`
		Last7d  int    `json:"last_7d"`
		Last30d int    `json:"last_30d"`
	}
	byTpl := map[string]int{}
	for i, fs := range firsts {
		byTpl[fs.tpl] = i
	}
	out := make([]outRow, 0, len(rows))
	for _, row := range rows {
		i := byTpl[row.Template]
		out = append(out, outRow{
			ID: firsts[i].id, Title: firsts[i].title,
			Total: row.Total, Last24h: row.Last24h, Last7d: row.Last7d, Last30d: row.Last30d,
		})
	}

	// Per-tool usage with attribution (used via email vs direct).
	toolStats, _ := h.Store.GetToolUsedStats(ctx)

	writeJSON(w, map[string]interface{}{
		"signups":   windows,
		"flows":     out,
		"tool_used": toolStats,
	}, http.StatusOK)
}
