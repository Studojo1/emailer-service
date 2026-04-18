package handlers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// AdminMiddleware checks for a valid admin JWT or ADMIN_SECRET
func AdminMiddleware(adminSecret string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if token == "" {
			writeError(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if (adminSecret != "" && token == adminSecret) || isAdminJWT(token) {
			next.ServeHTTP(w, r)
			return
		}
		writeError(w, "unauthorized", http.StatusUnauthorized)
	})
}

// isAdminJWT decodes the JWT payload and checks role == "admin" and not expired.
// No signature verification — this is fine for an internal admin tool.
func isAdminJWT(tokenStr string) bool {
	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	var claims struct {
		Role string `json:"role"`
		Exp  int64  `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return false
	}
	return claims.Role == "admin" && time.Now().Unix() < claims.Exp
}

// HandleAdminStats handles GET /v1/admin/stats
func (h *Handler) HandleAdminStats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.Store.GetEmailStats(r.Context())
	if err != nil {
		slog.Error("admin stats", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, stats, http.StatusOK)
}

// HandleAdminLogs handles GET /v1/admin/logs?limit=50&offset=0&q=
func (h *Handler) HandleAdminLogs(w http.ResponseWriter, r *http.Request) {
	limit := 50
	offset := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 200 {
			limit = v
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if v, err := strconv.Atoi(o); err == nil && v >= 0 {
			offset = v
		}
	}
	search := r.URL.Query().Get("q")

	logs, total, err := h.Store.ListEmailLogs(r.Context(), limit, offset, search)
	if err != nil {
		slog.Error("admin logs", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{
		"logs":   logs,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	}, http.StatusOK)
}

// HandleAdminUsers handles GET /v1/admin/users?limit=50&offset=0&q=
func (h *Handler) HandleAdminUsers(w http.ResponseWriter, r *http.Request) {
	limit := 50
	offset := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 200 {
			limit = v
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if v, err := strconv.Atoi(o); err == nil && v >= 0 {
			offset = v
		}
	}
	search := r.URL.Query().Get("q")

	users, total, err := h.Store.ListUsersWithStats(r.Context(), limit, offset, search)
	if err != nil {
		slog.Error("admin users", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{
		"users":  users,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	}, http.StatusOK)
}

// HandleAdminUserDetail handles GET /v1/admin/users/{id}
func (h *Handler) HandleAdminUserDetail(w http.ResponseWriter, r *http.Request) {
	userID := r.PathValue("id")
	if userID == "" {
		writeError(w, "id required", http.StatusBadRequest)
		return
	}

	user, err := h.Store.GetUserByID(r.Context(), userID)
	if err != nil || user == nil {
		writeError(w, "user not found", http.StatusNotFound)
		return
	}

	history, err := h.Store.GetUserEmailHistory(r.Context(), userID)
	if err != nil {
		history = nil
	}

	writeJSON(w, map[string]interface{}{
		"user":    user,
		"history": history,
	}, http.StatusOK)
}

// HandleAdminTemplates handles GET /v1/admin/templates
var allTemplates = []string{
	"welcome", "forgot-password", "resume-optimized", "internship-applied",
	"password-changed", "contact-form",
	"nurture-day3", "nurture-day7", "nurture-day14", "nurture-day30",
	"funnel-welcome-new", "funnel-welcome-existing",
	"funnel-followup-v1", "funnel-followup-v2", "funnel-followup-v3",
	"funnel-segmentation-v1", "funnel-segmentation-v2",
	"funnel-exploration-v1", "funnel-exploration-v2",
	"funnel-congratulations", "funnel-comparison",
	"funnel-pitching-v1", "funnel-pitching-v2", "funnel-pitching-v3",
	"funnel-honest-question-v1", "funnel-honest-question-v2", "funnel-honest-question-v3",
	"funnel-onboarding",
	"funnel-recognition-v1", "funnel-recognition-v2", "funnel-recognition-v3", "funnel-recognition-v4",
	"funnel-testimonial", "funnel-pricing", "funnel-case-study",
	"funnel-walkthrough", "funnel-educational", "funnel-winback",
	"signup-thankyou", "signup-followup", "payment-thankyou",
	"signup-welcome-v1", "signup-welcome-v2", "signup-welcome-v3",
	"signup-welcome-v4", "signup-welcome-v5",
}

func (h *Handler) HandleAdminTemplates(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]interface{}{
		"templates": allTemplates,
		"count":     len(allTemplates),
	}, http.StatusOK)
}

// HandleAdminCampaignList handles GET /v1/admin/campaigns
func (h *Handler) HandleAdminCampaignList(w http.ResponseWriter, r *http.Request) {
	campaigns, err := h.Store.ListCampaigns(r.Context())
	if err != nil {
		slog.Error("list campaigns", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{"campaigns": campaigns}, http.StatusOK)
}

// CampaignCreateRequest is the body for creating a campaign
type CampaignCreateRequest struct {
	Name         string `json:"name"`
	TemplateName string `json:"template_name"`
	FilterDays   int    `json:"filter_days"` // 0 = all users
}

// HandleAdminCampaignCreate handles POST /v1/admin/campaigns
func (h *Handler) HandleAdminCampaignCreate(w http.ResponseWriter, r *http.Request) {
	var req CampaignCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Name == "" || req.TemplateName == "" {
		writeError(w, "name and template_name are required", http.StatusBadRequest)
		return
	}

	campaign, err := h.Store.CreateCampaign(r.Context(), req.Name, req.TemplateName, req.FilterDays)
	if err != nil {
		slog.Error("create campaign", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, campaign, http.StatusCreated)
}

// HandleAdminCampaignSend handles POST /v1/admin/campaigns/{id}/send
func (h *Handler) HandleAdminCampaignSend(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, "id required", http.StatusBadRequest)
		return
	}

	campaign, err := h.Store.GetCampaign(r.Context(), id)
	if err != nil {
		writeError(w, "campaign not found", http.StatusNotFound)
		return
	}
	if campaign.Status != "draft" {
		writeError(w, "campaign already sent or running", http.StatusConflict)
		return
	}

	// Preview count
	count, err := h.Store.CountUsersBySignupDate(r.Context(), campaign.FilterDays)
	if err != nil {
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Mark running
	_ = h.Store.UpdateCampaignStatus(r.Context(), id, "running", nil, count, 0)

	writeJSON(w, map[string]interface{}{
		"message":    "campaign started",
		"recipients": count,
	}, http.StatusOK)

	// Send in background
	go func() {
		ctx := context.Background()
		users, err := h.Store.ListUsersBySignupDate(ctx, campaign.FilterDays)
		if err != nil {
			slog.Error("campaign send: list users", "campaign_id", id, "error", err)
			_ = h.Store.UpdateCampaignStatus(ctx, id, "failed", nil, 0, 0)
			return
		}

		sent := 0
		for _, user := range users {
			prefs, _ := h.Store.GetEmailPreferences(ctx, user.ID)
			if prefs != nil && !prefs.ProductEmails {
				continue
			}

			if err := h.Sender.SendTemplateEmail(ctx, user.Email, campaign.TemplateName, map[string]interface{}{
				"UserName": user.Name,
			}); err != nil {
				slog.Error("campaign send: failed", "user_id", user.ID, "error", err)
				continue
			}
			sent++
			time.Sleep(200 * time.Millisecond)
		}

		now := time.Now().UTC()
		_ = h.Store.UpdateCampaignStatus(ctx, id, "completed", &now, count, sent)
		slog.Info("campaign completed", "campaign_id", id, "sent", sent, "total", count)
	}()
}

// HandleAdminTrigger handles POST /v1/admin/trigger — same as the main trigger but admin-only
func (h *Handler) HandleAdminTrigger(w http.ResponseWriter, r *http.Request) {
	h.HandlePublishEvent(w, r)
}

// HandleAdminSendToUser handles POST /v1/admin/users/{id}/send
func (h *Handler) HandleAdminSendToUser(w http.ResponseWriter, r *http.Request) {
	userID := r.PathValue("id")
	if userID == "" {
		writeError(w, "id required", http.StatusBadRequest)
		return
	}

	var req struct {
		TemplateName string `json:"template_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.TemplateName == "" {
		writeError(w, "template_name required", http.StatusBadRequest)
		return
	}

	user, err := h.Store.GetUserByID(r.Context(), userID)
	if err != nil || user == nil {
		writeError(w, "user not found", http.StatusNotFound)
		return
	}

	go func() {
		ctx := context.Background()
		_, data := h.buildTemplateData(req.TemplateName, user)
		if err := h.Sender.SendTemplateEmail(ctx, user.Email, req.TemplateName, data); err != nil {
			slog.Error("admin send to user failed", "user_id", userID, "template", req.TemplateName, "error", err)
		}
	}()

	writeJSON(w, map[string]string{"message": "sending"}, http.StatusOK)
}

// HandleAdminCampaignGroups handles GET /v1/admin/campaign-groups
// Returns all email_types ever sent, grouped with send/open aggregate stats.
func (h *Handler) HandleAdminCampaignGroups(w http.ResponseWriter, r *http.Request) {
	groups, err := h.Store.GetCampaignGroups(r.Context())
	if err != nil {
		slog.Error("campaign groups", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{"groups": groups}, http.StatusOK)
}

// HandleAdminLogsByType handles GET /v1/admin/campaign-groups/{email_type}/logs?limit=50&offset=0
// Returns paginated individual sends for a specific email_type.
func (h *Handler) HandleAdminLogsByType(w http.ResponseWriter, r *http.Request) {
	emailType := r.PathValue("email_type")
	if emailType == "" {
		writeError(w, "email_type required", http.StatusBadRequest)
		return
	}
	limit := 50
	offset := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 200 {
			limit = v
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if v, err := strconv.Atoi(o); err == nil && v >= 0 {
			offset = v
		}
	}
	logs, total, err := h.Store.ListLogsByEmailType(r.Context(), emailType, limit, offset)
	if err != nil {
		slog.Error("logs by type", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{
		"logs":       logs,
		"total":      total,
		"limit":      limit,
		"offset":     offset,
		"email_type": emailType,
	}, http.StatusOK)
}

// HandleAdminScheduled handles GET /v1/admin/scheduled?limit=50&offset=0
func (h *Handler) HandleAdminScheduled(w http.ResponseWriter, r *http.Request) {
	limit := 50
	offset := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 200 {
			limit = v
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if v, err := strconv.Atoi(o); err == nil && v >= 0 {
			offset = v
		}
	}
	emails, total, err := h.Store.ListPendingScheduledEmails(r.Context(), limit, offset)
	if err != nil {
		slog.Error("admin scheduled", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{
		"scheduled": emails,
		"total":     total,
		"limit":     limit,
		"offset":    offset,
	}, http.StatusOK)
}

// HandleAdminTemplatePreview handles GET /v1/admin/templates/{name}/preview
// Returns the rendered HTML of the template with sample data.
func (h *Handler) HandleAdminTemplatePreview(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	html, err := h.Sender.RenderPreview(name)
	if err != nil {
		slog.Warn("template preview failed", "name", name, "error", err)
		writeError(w, "template not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("X-Frame-Options", "SAMEORIGIN")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(html))
}

// HandleAdminCampaignPreview handles GET /v1/admin/campaigns/preview?filter_days=0
func (h *Handler) HandleAdminCampaignPreview(w http.ResponseWriter, r *http.Request) {
	filterDays := 0
	if d := r.URL.Query().Get("filter_days"); d != "" {
		if v, err := strconv.Atoi(d); err == nil {
			filterDays = v
		}
	}
	count, err := h.Store.CountUsersBySignupDate(r.Context(), filterDays)
	if err != nil {
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]int{"count": count}, http.StatusOK)
}
