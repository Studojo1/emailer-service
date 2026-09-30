package scheduler

import (
	"context"
	"log/slog"
)

// heldLock is the connection that holds the scheduler advisory lock.
type heldLock interface {
	PingContext(ctx context.Context) error
	Close() error
}

// isLeader reports whether this pod may run scheduled work right now.
//
// Deploys start the new pod before stopping the old one (maxSurge=1, audit
// IN-N03), so for a short while two pods run. Without an election both would
// pick up the same due scheduled_emails rows and send them twice. Only the pod
// holding the Postgres advisory lock runs the scheduler; the lock is released
// when the old pod exits, and the new pod takes over on its next tick.
func (sc *Scheduler) isLeader(ctx context.Context) bool {
	if sc.lock != nil {
		if err := sc.lock.PingContext(ctx); err == nil {
			return true
		}
		// The connection died, and the lock with it. Try to take it again.
		_ = sc.lock.Close()
		sc.lock = nil
		slog.Warn("scheduler: lost the leader lock connection")
	}
	try := sc.tryLock
	if try == nil {
		try = func(ctx context.Context) (heldLock, bool, error) {
			conn, ok, err := sc.Store.TrySchedulerLock(ctx)
			if !ok || err != nil {
				return nil, ok, err
			}
			return conn, true, nil
		}
	}
	lock, ok, err := try(ctx)
	if err != nil {
		slog.Error("scheduler: leader lock", "error", err)
		return false
	}
	if !ok {
		return false
	}
	sc.lock = lock
	slog.Info("scheduler: this pod is now the scheduler leader")
	return true
}
