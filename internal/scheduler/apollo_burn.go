package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
)

// apolloBurnThreshold is the per-IST-day Apollo reveal count above which ops are
// paged. Normal burn is ~160/day; default tripwire is 400. Override with
// APOLLO_BURN_ALERT_THRESHOLD.
func apolloBurnThreshold() int {
	if v := os.Getenv("APOLLO_BURN_ALERT_THRESHOLD"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 400
}

// apolloBurnRecipients are the addresses paged on a burn alert. Override with a
// comma-separated APOLLO_BURN_ALERT_RECIPIENTS.
func apolloBurnRecipients() []string {
	raw := os.Getenv("APOLLO_BURN_ALERT_RECIPIENTS")
	if raw == "" {
		raw = "jeremy.zac@gmail.com,businessconnect.pranav@gmail.com"
	}
	var out []string
	for _, e := range strings.Split(raw, ",") {
		if e = strings.TrimSpace(e); e != "" {
			out = append(out, e)
		}
	}
	return out
}

// checkApolloBurn pages ops (at most once per IST day) when Apollo email reveals
// cross the configured threshold. The reveal counter (apollo_usage_daily) is
// written by job-outreach-svc on every successful /people/match reveal.
func (sc *Scheduler) checkApolloBurn(ctx context.Context) {
	threshold := apolloBurnThreshold()
	reveals, alerted, err := sc.Store.GetApolloBurnToday(ctx)
	if err != nil {
		slog.Error("apollo-burn: failed to read usage", "error", err)
		return
	}
	if reveals <= threshold || alerted {
		return
	}

	subject := fmt.Sprintf("⚠️ Apollo burn alert: %d credits today (limit %d)", reveals, threshold)
	html := fmt.Sprintf(`<div style="font-family:system-ui,Arial,sans-serif;font-size:15px;color:#19202b;line-height:1.5">
<h2 style="margin:0 0 12px">⚠️ Apollo credit-burn alert</h2>
<p><b>%d</b> Apollo email reveals have been burned today — over the <b>%d</b>/day tripwire.</p>
<p>Normal is ~160/day. If this looks wrong, check the Apollo dashboard (Settings &rarr; Usage &rarr; Enrichment requests) and the campaign worker for a runaway enrichment loop.</p>
<p style="color:#6b7280;font-size:13px;margin-top:16px">Automated tripwire from emailer-service. You get one alert per day.</p>
</div>`, reveals, threshold)

	sent := false
	for _, to := range apolloBurnRecipients() {
		if err := sc.Sender.SendOpsAlert(ctx, to, subject, html); err != nil {
			slog.Error("apollo-burn: send failed", "to", to, "error", err)
			continue
		}
		sent = true
		slog.Info("apollo-burn: alert sent", "to", to, "reveals", reveals, "threshold", threshold)
	}
	if sent {
		if err := sc.Store.MarkApolloBurnAlerted(ctx); err != nil {
			slog.Error("apollo-burn: failed to mark alerted", "error", err)
		}
	}
}
