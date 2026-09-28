package scheduler

import (
	"context"
	"fmt"
	"html"
	"log/slog"
	"strings"
	"time"

	"github.com/studojo/emailer-service/internal/store"
)

// checkDataHealth runs every store.DataHealthCheck and pages ops when one is
// broken, at most once per check per IST day. Same recipients as the Apollo
// burn alert (APOLLO_BURN_ALERT_RECIPIENTS).
//
// These are the failures the September audits found only weeks after they
// started hurting students: locked-out sign-ups, missing resume profiles,
// duplicate candidate rows, and quizzes "completed" with no targeting. Each is
// a cheap count over the last 24 hours, so a regression is reported the day
// it ships instead of in the next audit.
func (sc *Scheduler) checkDataHealth(ctx context.Context) {
	day := time.Now().In(istLocation()).Format("2006-01-02")
	for _, c := range store.DataHealthChecks {
		n, err := sc.Store.CountDataHealthViolations(ctx, c)
		if err != nil {
			slog.Error("data-health: check failed", "check", c.Name, "error", err)
			continue
		}
		if n <= c.Threshold {
			continue
		}
		slog.Warn("data-health: invariant broken", "check", c.Name, "count", n, "threshold", c.Threshold)
		// Page only from production. Staging is full of test sign-ups and
		// re-uploads; there the log line above is enough.
		if !isProductionFrontend(sc.FrontendURL) || sc.dataHealthAlerted[c.Name] == day {
			continue
		}
		if sc.sendDataHealthAlert(ctx, c, n) {
			sc.dataHealthAlerted[c.Name] = day
		}
	}
}

func (sc *Scheduler) sendDataHealthAlert(ctx context.Context, c store.DataHealthCheck, n int) bool {
	subject := fmt.Sprintf("⚠️ Data health: %d %s in the last 24h", n, c.Name)
	body := fmt.Sprintf(`<div style="font-family:system-ui,Arial,sans-serif;font-size:15px;color:#19202b;line-height:1.5">
<h2 style="margin:0 0 12px">⚠️ Data health alert: %s</h2>
<p><b>%d</b> %s in the last 24 hours (alert above %d).</p>
<p><b>Look first at:</b> %s</p>
<p style="color:#6b7280;font-size:13px;margin-top:16px">Automated check from emailer-service. One alert per check per day.</p>
</div>`, html.EscapeString(c.Name), n, html.EscapeString(c.What), c.Threshold, html.EscapeString(c.Fix))
	sent := false
	for _, to := range apolloBurnRecipients() {
		if err := sc.Sender.SendOpsAlert(ctx, to, subject, body); err != nil {
			slog.Error("data-health: send failed", "to", to, "error", err)
			continue
		}
		sent = true
	}
	return sent
}

func istLocation() *time.Location {
	if loc, err := time.LoadLocation("Asia/Kolkata"); err == nil {
		return loc
	}
	return time.FixedZone("IST", 5*3600+1800)
}

func isProductionFrontend(u string) bool {
	return strings.Contains(u, "studojo.com")
}
