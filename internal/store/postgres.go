package store

import (
	"database/sql"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

// PostgresStore implements database operations for emailer service
type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore returns a new PostgresStore
func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

// EmailPreferences represents user email preferences
type EmailPreferences struct {
	ID             uuid.UUID
	UserID         string
	ProductEmails  bool
	ResumeEmails   bool
	InternshipEmails bool
	SecurityEmails bool
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// PasswordResetToken represents a password reset token
type PasswordResetToken struct {
	ID        uuid.UUID
	UserID    string
	Token     string
	ExpiresAt time.Time
	UsedAt    *time.Time
	CreatedAt time.Time
}

// User represents a user (minimal fields needed)
type User struct {
	ID    string
	Email string
	Name  string
}

