package store

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"time"
)

// Per-recipient founder-coupon support.
//
// Coupon emails (cc-outreach-coupon, cc-cart-goat) used to render a single
// static code (DEFAULT_COUPON_CODE / STUDOJO20) that never expired. We now mint
// a UNIQUE single-use code per recipient and start a 10-hour expiry clock the
// moment they OPEN the email — not when it is sent.
//
// How the clock works: at send time we insert a coupons row (shared table,
// owned by job-outreach-svc) with valid_until = NULL — the code is valid but the
// clock has not started. We also record a coupon_issuance row keyed by
// (email_type, email) so the open pixel can find the code later. On the first
// open we set valid_until = now + 10h. The Outreach checkout already rejects
// expired / over-limit / inactive codes (job-outreach-svc routes_payment.py), so
// no validation change is needed there beyond binding the code to the buyer.

// couponCodeAlphabet excludes easily-confused characters (0/O, 1/I/L) so a code
// copied by hand from an email is unambiguous.
const couponCodeAlphabet = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"

// generateCouponSuffix returns an n-char random suffix from couponCodeAlphabet.
func generateCouponSuffix(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	out := make([]byte, n)
	for i := range b {
		out[i] = couponCodeAlphabet[int(b[i])%len(couponCodeAlphabet)]
	}
	return string(out), nil
}

// CreatePerRecipientCoupon mints a unique single-use coupon for one recipient and
// records its issuance so the open pixel can later start the expiry clock.
//
//   - prefix:        human-readable label, e.g. "JEREMY" -> "JEREMY-AB2C9KQ7"
//   - userID/email:  recipient identity (userID may be "" for raw-email sends)
//   - emailType:     the TEMPLATE name (e.g. "cc-outreach-coupon"); together with
//     email it forms the lookup key the open handler uses
//   - discountPct:   percentage off (e.g. 10)
//
// The coupon is inserted with valid_until = NULL (clock not started) and
// max_uses = 1. user_id binds the code to the buyer so a leaked code can't be
// redeemed by anyone else (enforced at checkout in job-outreach-svc).
func (s *PostgresStore) CreatePerRecipientCoupon(ctx context.Context, prefix, userID, email, emailType string, discountPct float64) (string, error) {
	prefix = strings.ToUpper(strings.TrimSpace(prefix))
	if prefix == "" {
		prefix = "STUDOJO"
	}

	// Retry on the (unlikely) chance of a code collision against the UNIQUE index.
	var code string
	for attempt := 0; attempt < 5; attempt++ {
		suffix, err := generateCouponSuffix(8)
		if err != nil {
			return "", fmt.Errorf("generate coupon suffix: %w", err)
		}
		candidate := prefix + "-" + suffix

		_, err = s.db.ExecContext(ctx, `
			INSERT INTO coupons
				(code, discount_type, discount_value, max_uses, uses,
				 valid_from, valid_until, distributor_name, is_active, user_id, created_at)
			VALUES ($1, 'percent', $2, 1, 0, NOW(), NULL, $3, TRUE, $4, NOW())`,
			candidate, discountPct, "founder-coupon-email", nullableUserID(userID),
		)
		if err != nil {
			if isUniqueViolation(err) {
				continue // collision — pick a new suffix
			}
			return "", fmt.Errorf("insert coupon: %w", err)
		}
		code = candidate
		break
	}
	if code == "" {
		return "", fmt.Errorf("could not mint unique coupon code after retries")
	}

	// Record issuance so HandleTrackOpen can resolve email_type+email -> code.
	// ON CONFLICT keeps the FIRST code issued for this (email_type, email): if a
	// recipient somehow gets the same coupon email twice, the original code (and
	// its open-clock) wins and we don't strand a second code.
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO coupon_issuance (email_type, email, code, user_id, issued_at)
		VALUES ($1, $2, $3, $4, NOW())
		ON CONFLICT (email_type, email) DO NOTHING`,
		emailType, strings.ToLower(strings.TrimSpace(email)), code, userID,
	); err != nil {
		return "", fmt.Errorf("record coupon issuance: %w", err)
	}

	return code, nil
}

// ActivateCouponExpiryOnOpen starts the expiry clock for the coupon issued to
// (emailType, email), the first time the email is opened. It sets
// valid_until = now + ttl, but ONLY if it is still NULL — a second open does not
// extend the window (idempotent). No-op if no coupon was issued for this pair.
func (s *PostgresStore) ActivateCouponExpiryOnOpen(ctx context.Context, emailType, email string, ttl time.Duration) error {
	email = strings.ToLower(strings.TrimSpace(email))
	if emailType == "" || email == "" {
		return nil
	}
	until := time.Now().UTC().Add(ttl)
	_, err := s.db.ExecContext(ctx, `
		UPDATE coupons
		   SET valid_until = $1
		 WHERE valid_until IS NULL
		   AND code = (
		       SELECT code FROM coupon_issuance
		        WHERE email_type = $2 AND email = $3
		        LIMIT 1
		   )`,
		until, emailType, email,
	)
	return err
}

// nullableUserID returns the userID or NULL so the coupons.user_id FK is not set
// to an empty string for raw-email sends with no user.
func nullableUserID(userID string) interface{} {
	if strings.TrimSpace(userID) == "" {
		return nil
	}
	return userID
}

// isUniqueViolation reports whether err is a Postgres unique-constraint violation
// (SQLSTATE 23505). We match on the message to avoid a hard dependency on the
// pq error type at this layer.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "23505") ||
		strings.Contains(msg, "duplicate key value") ||
		strings.Contains(msg, "unique constraint")
}
