package store

import (
	"context"
	"database/sql"
)

// schedulerLockKey is the Postgres advisory-lock key that elects the one pod
// allowed to run the scheduler. Arbitrary but fixed ("emailer" in ASCII).
const schedulerLockKey int64 = 0x656d61696c6572

// TrySchedulerLock takes the scheduler advisory lock on a dedicated
// connection without waiting. The lock lives as long as that connection, so
// it is released automatically when the pod exits or the connection drops.
// Returns (nil, false, nil) when another pod holds it.
func (s *PostgresStore) TrySchedulerLock(ctx context.Context) (*sql.Conn, bool, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, false, err
	}
	var ok bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, schedulerLockKey).Scan(&ok); err != nil {
		_ = conn.Close()
		return nil, false, err
	}
	if !ok {
		_ = conn.Close()
		return nil, false, nil
	}
	return conn, true, nil
}
