package main

import (
	"context"
	"database/sql"
	"fmt"
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
	"github.com/studojo/emailer-service/internal/store"
)

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

	// Azure Email configuration (or MailHog for development)
	azureConnStr := os.Getenv("AZURE_COMMUNICATION_SERVICE_CONNECTION_STRING")
	if azureConnStr == "" {
		// Try MailHog for development
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

	// CORS configuration
	corsOrigins := strings.Split(os.Getenv("CORS_ORIGINS"), ",")
	if len(corsOrigins) == 0 || (len(corsOrigins) == 1 && corsOrigins[0] == "") {
		// Default to allowing localhost for development
		corsOrigins = []string{"http://localhost:3000", "http://127.0.0.1:3000"}
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

	// Initialize stores
	store := store.NewPostgresStore(db)
	tokenStore := auth.NewTokenStore(db)

	// Initialize handlers
	httpHandler := &handlers.Handler{
		Store:            store,
		Sender:           sender,
		TokenStore:       tokenStore,
		FrontendURL:      frontendURL,
		EmailFrontendURL: emailFrontendURL,
	}

	eventHandler := handlers.NewEventHandler(store, sender, emailFrontendURL)

	// Setup HTTP routes
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", httpHandler.HandleHealth)
	mux.HandleFunc("POST /v1/email/forgot-password", httpHandler.HandleForgotPassword)
	mux.HandleFunc("POST /v1/email/reset-password", httpHandler.HandleResetPassword)
	mux.HandleFunc("POST /v1/email/change-password", httpHandler.HandleChangePassword)
	mux.HandleFunc("GET /v1/email/preferences/{user_id}", httpHandler.HandleGetEmailPreferences)
	mux.HandleFunc("PUT /v1/email/preferences/{user_id}", httpHandler.HandleUpdateEmailPreferences)
	mux.HandleFunc("POST /v1/email/events", httpHandler.HandlePublishEvent)

	// Wrap mux with a handler that intercepts OPTIONS before routing
	// This is needed because Go's ServeMux rejects OPTIONS if no route matches
	wrappedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Handle OPTIONS requests before they reach the mux
		if r.Method == http.MethodOptions {
			// Apply CORS headers directly (middleware will also add them, but this ensures they're set)
			middleware.CORS(corsOrigins)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})).ServeHTTP(w, r)
			return
		}
		// For all other requests, pass to mux
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
