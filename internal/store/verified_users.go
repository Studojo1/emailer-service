package store

import "context"

// ListVerifiedUsers returns every account with a verified email address, oldest
// first. Used for service notices that must reach everyone (policy-update), so
// unlike the marketing lists it has no row ceiling.
func (s *PostgresStore) ListVerifiedUsers(ctx context.Context) ([]User, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, email, COALESCE(name, '')
		FROM "user"
		WHERE email_verified = true AND COALESCE(TRIM(email), '') <> ''
		ORDER BY created_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var users []User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.Email, &u.Name); err != nil {
			return nil, err
		}
		users = append(users, u)
	}
	return users, rows.Err()
}
