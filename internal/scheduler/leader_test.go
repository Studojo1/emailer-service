package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// fakeAdvisoryLock mimics pg_try_advisory_lock: one holder at a time, released
// when the holder's connection closes or dies.
type fakeAdvisoryLock struct {
	mu     sync.Mutex
	holder *fakeConn
}

type fakeConn struct {
	l    *fakeAdvisoryLock
	dead bool
}

func (c *fakeConn) PingContext(context.Context) error {
	if c.dead {
		return errors.New("connection reset")
	}
	return nil
}

func (c *fakeConn) Close() error {
	c.l.mu.Lock()
	defer c.l.mu.Unlock()
	if c.l.holder == c {
		c.l.holder = nil
	}
	return nil
}

func (l *fakeAdvisoryLock) try(context.Context) (heldLock, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.holder != nil && !l.holder.dead {
		return nil, false, nil
	}
	c := &fakeConn{l: l}
	l.holder = c
	return c, true, nil
}

// During a rolling deploy (maxSurge=1, audit IN-N03) the old and new pods
// overlap. Only one may run the scheduler, or due emails go out twice.
func TestOnlyOnePodRunsTheScheduler(t *testing.T) {
	ctx := context.Background()
	lock := &fakeAdvisoryLock{}
	oldPod := &Scheduler{tryLock: lock.try}
	newPod := &Scheduler{tryLock: lock.try}

	if !oldPod.isLeader(ctx) {
		t.Fatal("first pod should become leader")
	}
	if newPod.isLeader(ctx) {
		t.Fatal("second pod must not run the scheduler while the first holds the lock")
	}
	if !oldPod.isLeader(ctx) {
		t.Fatal("leader should keep the lock across ticks")
	}

	// Old pod exits: its connection closes and the lock is released.
	_ = oldPod.lock.Close()
	if !newPod.isLeader(ctx) {
		t.Fatal("new pod should take over once the old pod is gone")
	}

	// If the leader's connection dies, it must stop acting as leader, and
	// another pod can take over.
	newPod.lock.(*fakeConn).dead = true
	other := &Scheduler{tryLock: lock.try}
	if !other.isLeader(ctx) {
		t.Fatal("a pod should take over a lock whose connection died")
	}
	if newPod.isLeader(ctx) {
		t.Fatal("a pod whose lock connection died must not keep running the scheduler")
	}
}
