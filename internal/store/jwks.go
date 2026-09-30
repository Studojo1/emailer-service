package store

import "context"

// JWKSPublicKeys returns the JWK public keys that better-auth keeps in the
// shared jwks table, newest first, skipping expired keys. The admin
// middleware verifies dashboard JWT signatures against these.
func (s *PostgresStore) JWKSPublicKeys(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT public_key FROM jwks
		WHERE expires_at IS NULL OR expires_at > NOW()
		ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}
