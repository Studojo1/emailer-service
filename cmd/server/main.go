package main

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	"github.com/studojo/emailer-service/internal/auth"
	"github.com/studojo/emailer-service/internal/email"
	"github.com/studojo/emailer-service/internal/handlers"
	"github.com/studojo/emailer-service/internal/messaging"
	"github.com/studojo/emailer-service/internal/middleware"
	"github.com/studojo/emailer-service/internal/scheduler"
	"github.com/studojo/emailer-service/internal/store"
)

//go:embed dashboard
var dashboardFS embed.FS

// ensureSSLMode appends sslmode=disable to the DSN if no sslmode is set.
func ensureSSLMode(dsn string) string {
	if strings.Contains(strings.ToLower(dsn), "sslmode=") {
		return dsn
	}
	if strings.Contains(dsn, "?") {
		return dsn + "&sslmode=disable"
	}
	return dsn + "?sslmode=disable"
}

func main() {
	// Database configuration
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgresql://studojo:studojo@localhost:5432/postgres?sslmode=disable"
	}
	dbURL = ensureSSLMode(dbURL)

	// RabbitMQ configuration
	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@localhost:5672/"
	}

	// Email provider priority: Resend → ACS → MailHog (dev)
	azureConnStr := os.Getenv("RESEND_API_KEY")
	if azureConnStr != "" {
		slog.Info("using Resend for email")
	}
	if azureConnStr == "" {
		azureConnStr = os.Getenv("AZURE_EMAIL_CONNECTION_STRING")
		if azureConnStr != "" {
			slog.Info("using Azure Communication Services for email")
		}
	}
	if azureConnStr == "" {
		azureConnStr = os.Getenv("AZURE_COMMUNICATION_SERVICE_CONNECTION_STRING")
		if azureConnStr != "" {
			slog.Info("using Azure Communication Services for email (legacy env)")
		}
	}
	if azureConnStr == "" {
		mailhogURL := os.Getenv("MAILHOG_URL")
		if mailhogURL == "" {
			mailhogURL = "http://mailhog:8025"
		}
		azureConnStr = fmt.Sprintf("endpoint=%s", mailhogURL)
		slog.Info("using MailHog for email (development mode)", "url", mailhogURL)
	}

	senderEmail := os.Getenv("AZURE_EMAIL_SENDER_ADDRESS")
	if senderEmail == "" {
		senderEmail = "no-reply@studojo.com"
	}

	// Frontend URL for internal service-to-service calls (e.g., http://frontend:3000)
	frontendURL := os.Getenv("FRONTEND_URL")
	if frontendURL == "" {
		frontendURL = "http://frontend:3000" // Default to service name for Docker
	}
	
	// Frontend URL for email links that users click (e.g., http://localhost:3000)
	emailFrontendURL := os.Getenv("EMAIL_FRONTEND_URL")
	if emailFrontendURL == "" {
		emailFrontendURL = "http://localhost:3000" // Default to localhost for email links
	}

	// Template directory
	templateDir := os.Getenv("TEMPLATE_DIR")
	if templateDir == "" {
		templateDir = "/app/templates"
	}

	// HTTP port
	port := os.Getenv("HTTP_PORT")
	if port == "" {
		port = "8087"
	}

	// Admin secret for the mail dashboard
	adminSecret := os.Getenv("ADMIN_SECRET")

	// CORS configuration
	corsOrigins := strings.Split(os.Getenv("CORS_ORIGINS"), ",")
	if len(corsOrigins) == 0 || (len(corsOrigins) == 1 && corsOrigins[0] == "") {
		// Default to allowing localhost for development
		corsOrigins = []string{
			"http://localhost:3000", "http://127.0.0.1:3000",
			"https://mail.studojo.com", "https://admin.studojo.com",
		}
	}

	// Connect to database
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		slog.Error("db open failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		slog.Error("db ping failed", "error", err)
		os.Exit(1)
	}

	// Initialize Azure email client
	emailClient, err := email.NewClient(azureConnStr, senderEmail)
	if err != nil {
		slog.Error("failed to create azure email client", "error", err)
		os.Exit(1)
	}

	// Register additional ACS resources for round-robin (increases hourly quota).
	// Set AZURE_EMAIL_CONNECTION_STRING_2, _3, etc. to add more resources.
	for i := 2; ; i++ {
		extra := os.Getenv(fmt.Sprintf("AZURE_EMAIL_CONNECTION_STRING_%d", i))
		if extra == "" {
			break
		}
		if err := emailClient.AddACSResource(extra); err != nil {
			slog.Warn("failed to add extra ACS resource", "index", i, "error", err)
		}
	}

	// Initialize template renderer
	renderer, err := email.NewTemplateRenderer(templateDir)
	if err != nil {
		slog.Error("failed to create template renderer", "error", err)
		os.Exit(1)
	}

	// Load all templates
	if err := renderer.LoadAllTemplates(); err != nil {
		slog.Error("failed to load templates", "error", err)
		os.Exit(1)
	}

	// Initialize email sender
	sender := email.NewSender(emailClient, renderer)

	// Per-category sender addresses
	supportSender := os.Getenv("EMAIL_SENDER_SUPPORT")
	if supportSender == "" {
		supportSender = senderEmail
	}
	welcomeSender := os.Getenv("EMAIL_SENDER_WELCOME")
	if welcomeSender == "" {
		welcomeSender = senderEmail
	}
	promotionsSender := os.Getenv("EMAIL_SENDER_PROMOTIONS")
	if promotionsSender == "" {
		promotionsSender = senderEmail
	}
	sender.SetSenderAddresses(supportSender, welcomeSender, promotionsSender)
	slog.Info("email senders configured",
		"support", supportSender,
		"welcome", welcomeSender,
		"promotions", promotionsSender,
	)

	// Set tracking URL for open rate pixels
	trackingBaseURL := os.Getenv("TRACKING_BASE_URL")
	if trackingBaseURL == "" {
		trackingBaseURL = "https://api.studojo.com"
	}
	sender.SetTrackingURL(trackingBaseURL)

	// Initialize stores
	pgStore := store.NewPostgresStore(db)
	tokenStore := auth.NewTokenStore(db)

	// Wire logger into sender so every send is recorded in email_send_log
	sender.SetLogger(pgStore)

	// Ensure tables exist
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS email_send_log (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			user_id TEXT NOT NULL DEFAULT '',
			user_name TEXT NOT NULL DEFAULT '',
			email_to TEXT NOT NULL,
			template_name TEXT NOT NULL,
			from_address TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT 'sent',
			sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			opened_at TIMESTAMPTZ
		);
		CREATE INDEX IF NOT EXISTS idx_email_send_log_email_to ON email_send_log(email_to);
		CREATE INDEX IF NOT EXISTS idx_email_send_log_template ON email_send_log(template_name);
		CREATE INDEX IF NOT EXISTS idx_email_send_log_sent_at ON email_send_log(sent_at DESC);
		CREATE INDEX IF NOT EXISTS idx_email_send_log_user_id ON email_send_log(user_id);

		CREATE TABLE IF NOT EXISTS email_campaigns (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			name TEXT NOT NULL,
			template_name TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'draft',
			filter_days INT NOT NULL DEFAULT 0,
			total_recipients INT NOT NULL DEFAULT 0,
			sent_count INT NOT NULL DEFAULT 0,
			open_count INT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			sent_at TIMESTAMPTZ
		);
	`)
	if err != nil {
		slog.Error("failed to create admin tables", "error", err)
		os.Exit(1)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS email_preferences (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			user_id TEXT NOT NULL UNIQUE,
			product_emails BOOLEAN NOT NULL DEFAULT true,
			resume_emails BOOLEAN NOT NULL DEFAULT true,
			internship_emails BOOLEAN NOT NULL DEFAULT true,
			security_emails BOOLEAN NOT NULL DEFAULT true,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE INDEX IF NOT EXISTS idx_email_preferences_user_id ON email_preferences(user_id);
		CREATE TABLE IF NOT EXISTS scheduled_emails (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			user_id TEXT NOT NULL,
			email_type TEXT NOT NULL,
			scheduled_at TIMESTAMPTZ NOT NULL,
			sent_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE INDEX IF NOT EXISTS idx_scheduled_emails_due
			ON scheduled_emails (scheduled_at) WHERE sent_at IS NULL;
		CREATE INDEX IF NOT EXISTS idx_scheduled_emails_user_type
			ON scheduled_emails (user_id, email_type);
		CREATE TABLE IF NOT EXISTS email_opens (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			track_id TEXT NOT NULL UNIQUE,
			user_id TEXT NOT NULL DEFAULT '',
			email_type TEXT NOT NULL DEFAULT '',
			opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			user_agent TEXT NOT NULL DEFAULT ''
		);
		CREATE INDEX IF NOT EXISTS idx_email_opens_email_type ON email_opens (email_type);
		CREATE INDEX IF NOT EXISTS idx_email_opens_user_id ON email_opens (user_id);
	`)
	if err != nil {
		slog.Error("failed to create tables", "error", err)
		os.Exit(1)
	}

	// Data integrity migration — idempotent, safe to run on every startup.
	//
	// 1. Rename legacy routing-key records to template names so dedup checks match.
	// 2. Remove any pending rows where a sent row already exists for the same user+type
	//    (email already delivered; the pending row would cause a duplicate send).
	// 3. Deduplicate remaining sent rows, keeping the oldest per user+type.
	// 4. Deduplicate remaining pending rows, keeping the earliest scheduled per user+type.
	// 5. Add a UNIQUE constraint so future duplicates are physically impossible.
	//    CreateScheduledEmail and RecordSentEmail both use ON CONFLICT DO NOTHING,
	//    so callers never see an error from a harmless duplicate attempt.
	for _, stmt := range []string{
		`UPDATE scheduled_emails SET email_type = 'funnel-segmentation-v1'
		 WHERE email_type = 'event.funnel.segmentation_v1'`,

		`UPDATE scheduled_emails SET email_type = 'funnel-segmentation-v2'
		 WHERE email_type = 'event.funnel.segmentation_v2'`,

		// Remove pending rows superseded by a sent row
		`DELETE FROM scheduled_emails
		 WHERE sent_at IS NULL
		   AND (user_id, email_type) IN (
		     SELECT user_id, email_type FROM scheduled_emails WHERE sent_at IS NOT NULL
		   )`,

		// Deduplicate sent rows — keep oldest sent_at per user+type
		`DELETE FROM scheduled_emails
		 WHERE sent_at IS NOT NULL
		   AND id NOT IN (
		     SELECT DISTINCT ON (user_id, email_type) id
		     FROM scheduled_emails WHERE sent_at IS NOT NULL
		     ORDER BY user_id, email_type, sent_at ASC
		   )`,

		// Deduplicate pending rows — keep earliest scheduled_at per user+type
		`DELETE FROM scheduled_emails
		 WHERE sent_at IS NULL
		   AND id NOT IN (
		     SELECT DISTINCT ON (user_id, email_type) id
		     FROM scheduled_emails WHERE sent_at IS NULL
		     ORDER BY user_id, email_type, scheduled_at ASC
		   )`,

		// Unique constraint — makes all future duplicate inserts safe no-ops
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_scheduled_emails_unique_user_type
		 ON scheduled_emails (user_id, email_type)`,

		// Restore nurture emails that were incorrectly marked as sent by a prior
		// migration (bulk-mark-done) but were never actually delivered — i.e. no
		// corresponding row exists in email_send_log. Reset sent_at to NULL so
		// the scheduler picks them up and sends them.
		`UPDATE scheduled_emails se SET sent_at = NULL
		 WHERE se.email_type LIKE 'nurture%'
		   AND se.sent_at IS NOT NULL
		   AND NOT EXISTS (
		     SELECT 1 FROM email_send_log esl
		     WHERE esl.user_id = se.user_id
		       AND esl.template_name = REPLACE(se.email_type, '_', '-')
		   )`,

		// Cancel all pending emails for admin accounts so they don't get
		// spammed during backlog drain.
		`UPDATE scheduled_emails SET sent_at = NOW()
		 WHERE sent_at IS NULL
		   AND user_id IN (
		     SELECT id FROM "user"
		     WHERE email IN ('jeremy.zac@gmail.com', 'studojo@gmail.com')
		   )`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			slog.Warn("data migration step failed (non-fatal)", "error", err, "stmt", stmt[:60])
		}
	}
	slog.Info("data migration: scheduled_emails dedup and unique index complete")

	// Initialize handlers
	eventHandler := handlers.NewEventHandler(pgStore, sender, emailFrontendURL)

	httpHandler := &handlers.Handler{
		Store:            pgStore,
		Sender:           sender,
		TokenStore:       tokenStore,
		EventHandler:     eventHandler,
		FrontendURL:      frontendURL,
		EmailFrontendURL: emailFrontendURL,
	}

	// Serve the embedded dashboard SPA
	dashSub, _ := fs.Sub(dashboardFS, "dashboard")
	dashFileServer := http.FileServer(http.FS(dashSub))

	// Setup HTTP routes
	mux := http.NewServeMux()

	// Dashboard — serve at /mail/ and redirect / to /mail/
	mux.Handle("/mail/", http.StripPrefix("/mail", dashFileServer))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/mail/", http.StatusFound)
			return
		}
		http.NotFound(w, r)
	})

	// Public email routes
	mux.HandleFunc("GET /health", httpHandler.HandleHealth)
	mux.HandleFunc("GET /v1/email/track/{track_id}", httpHandler.HandleTrackOpen)
	mux.HandleFunc("POST /v1/email/forgot-password", httpHandler.HandleForgotPassword)
	mux.HandleFunc("POST /v1/email/reset-password", httpHandler.HandleResetPassword)
	mux.HandleFunc("POST /v1/email/change-password", httpHandler.HandleChangePassword)
	mux.HandleFunc("GET /v1/email/preferences/{user_id}", httpHandler.HandleGetEmailPreferences)
	mux.HandleFunc("PUT /v1/email/preferences/{user_id}", httpHandler.HandleUpdateEmailPreferences)
	mux.HandleFunc("POST /v1/email/events", httpHandler.HandlePublishEvent)
	mux.HandleFunc("GET /v1/email/bulk-send/preview", httpHandler.HandleBulkSendPreview)
	mux.HandleFunc("POST /v1/email/bulk-send", httpHandler.HandleBulkSend)

	// Admin API routes (JWT or ADMIN_SECRET protected)
	adminMux := http.NewServeMux()
	adminMux.HandleFunc("GET /v1/admin/stats", httpHandler.HandleAdminStats)
	adminMux.HandleFunc("GET /v1/admin/logs", httpHandler.HandleAdminLogs)
	adminMux.HandleFunc("GET /v1/admin/users", httpHandler.HandleAdminUsers)
	adminMux.HandleFunc("GET /v1/admin/users/{id}", httpHandler.HandleAdminUserDetail)
	adminMux.HandleFunc("POST /v1/admin/users/{id}/send", httpHandler.HandleAdminSendToUser)
	adminMux.HandleFunc("POST /v1/admin/users/{id}/cancel-scheduled", httpHandler.HandleAdminCancelUserScheduled)
	adminMux.HandleFunc("POST /v1/admin/send-oneoff", httpHandler.HandleAdminSendOneOff)
	adminMux.HandleFunc("GET /v1/admin/templates", httpHandler.HandleAdminTemplates)
	adminMux.HandleFunc("GET /v1/admin/campaigns", httpHandler.HandleAdminCampaignList)
	adminMux.HandleFunc("POST /v1/admin/campaigns", httpHandler.HandleAdminCampaignCreate)
	adminMux.HandleFunc("GET /v1/admin/campaigns/preview", httpHandler.HandleAdminCampaignPreview)
	adminMux.HandleFunc("POST /v1/admin/campaigns/{id}/send", httpHandler.HandleAdminCampaignSend)
	adminMux.HandleFunc("GET /v1/admin/campaign-groups", httpHandler.HandleAdminCampaignGroups)
	adminMux.HandleFunc("GET /v1/admin/campaign-groups/{email_type}/logs", httpHandler.HandleAdminLogsByType)
	adminMux.HandleFunc("POST /v1/admin/trigger", httpHandler.HandleAdminTrigger)
	adminMux.HandleFunc("GET /v1/admin/scheduled", httpHandler.HandleAdminScheduled)
	adminMux.HandleFunc("GET /v1/admin/templates/{name}/preview", httpHandler.HandleAdminTemplatePreview)

	mux.Handle("/v1/admin/", handlers.AdminMiddleware(adminSecret, adminMux))

	// Wrap mux with a handler that intercepts OPTIONS before routing
	wrappedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			middleware.CORS(corsOrigins)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})).ServeHTTP(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	})

	// Apply CORS middleware
	handler := middleware.CORS(corsOrigins)(wrappedHandler)

	// Start RabbitMQ consumer
	msgCfg := messaging.DefaultConfig(rabbitURL)
	consumer := messaging.NewConsumer(msgCfg, eventHandler)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	go func() {
		messaging.RunWithRetry(ctx, consumer, 5*time.Second)
	}()

	// Start nurture email scheduler
	sched := scheduler.NewScheduler(pgStore, sender, emailFrontendURL)
	go sched.Run(ctx)

	// Start HTTP server
	addr := ":" + port
	srv := &http.Server{Addr: addr, Handler: handler}

	go func() {
		slog.Info("emailer-service listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("listen failed", "error", err)
			os.Exit(1)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	stop()
	if err := srv.Shutdown(context.Background()); err != nil {
		slog.Error("shutdown failed", "error", err)
		os.Exit(1)
	}

	fmt.Println("emailer-service stopped")
}
