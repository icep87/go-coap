package inactivity

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type keepAliveTestConn struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func newKeepAliveTestConn() *keepAliveTestConn {
	ctx, cancel := context.WithCancel(context.Background())
	return &keepAliveTestConn{ctx: ctx, cancel: cancel}
}

func (c *keepAliveTestConn) Context() context.Context { return c.ctx }
func (c *keepAliveTestConn) Close() error {
	c.cancel()
	return nil
}

func awaitKeepAliveSignal(t *testing.T, signal <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func TestCoalescedKeepAliveCoalescesPendingAdmission(t *testing.T) {
	conn := newKeepAliveTestConn()
	t.Cleanup(func() { _ = conn.Close() })
	senderStarted := make(chan struct{}, 1)
	releaseSender := make(chan struct{})
	var releaseOnce sync.Once
	var senderCalls int
	var senderMu sync.Mutex
	sendPing := func(*keepAliveTestConn, func()) (func(), error) {
		senderMu.Lock()
		senderCalls++
		senderMu.Unlock()
		senderStarted <- struct{}{}
		<-releaseSender
		return func() {}, nil
	}
	onInactive := func(*keepAliveTestConn) {}

	// The baseline RED used the existing synchronous constructor; switch it to
	// the new constructor once the prompt-return behavior is implemented.
	newKeepAlive := func() *KeepAlive[*keepAliveTestConn] {
		return NewCoalescedKeepAlive[*keepAliveTestConn](3, onInactive, sendPing)
	}
	m := newKeepAlive()
	firstReturned := make(chan struct{})
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseSender) })
		select {
		case <-firstReturned:
		case <-time.After(time.Second):
			t.Errorf("OnInactive goroutine did not exit")
		}
		m.mu.Lock()
		attemptDone := m.attemptDone
		m.mu.Unlock()
		if attemptDone != nil {
			select {
			case <-attemptDone:
			case <-time.After(time.Second):
				t.Errorf("sender worker did not exit")
			}
		}
	})
	go func() {
		m.OnInactive(conn)
		close(firstReturned)
	}()

	awaitKeepAliveSignal(t, senderStarted, "sender start")
	awaitKeepAliveSignal(t, firstReturned, "prompt OnInactive return")
	for range 3 {
		returned := make(chan struct{})
		go func() {
			m.OnInactive(conn)
			close(returned)
		}()
		awaitKeepAliveSignal(t, returned, "coalesced OnInactive return")
	}
	m.mu.Lock()
	attemptDone := m.attemptDone
	m.mu.Unlock()
	require.Zero(t, m.numFails.Load(), "the coalesced path must not count a pending sender")
	// Always release and join the sender, including on assertion failure.
	releaseOnce.Do(func() { close(releaseSender) })
	awaitKeepAliveSignal(t, attemptDone, "attempt cleanup completion")

	senderMu.Lock()
	calls := senderCalls
	senderMu.Unlock()
	require.Equal(t, 1, calls)
	require.EqualValues(t, 1, m.numFails.Load(), "one completed ping must consume one retry")
}

func currentAttempt(m *KeepAlive[*keepAliveTestConn]) <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.attemptDone
}

func TestCoalescedKeepAliveRetryBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name       string
		maxRetries uint32
		sendErr    error
	}{
		{name: "max zero", maxRetries: 0},
		{name: "successes", maxRetries: 3},
		{name: "non-closure errors", maxRetries: 1, sendErr: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := newKeepAliveTestConn()
			t.Cleanup(func() { _ = conn.Close() })
			var sends, evictions, cancellations atomic.Int32
			m := NewCoalescedKeepAlive[*keepAliveTestConn](tc.maxRetries,
				func(*keepAliveTestConn) { evictions.Add(1) },
				func(*keepAliveTestConn, func()) (func(), error) {
					sends.Add(1)
					return func() { cancellations.Add(1) }, tc.sendErr
				})

			for attempt := uint32(0); attempt < tc.maxRetries; attempt++ {
				m.OnInactive(conn)
				awaitKeepAliveSignal(t, currentAttempt(m), "completed send attempt")
				require.EqualValues(t, attempt+1, sends.Load())
				require.EqualValues(t, attempt+1, m.numFails.Load())
				require.Zero(t, evictions.Load())
			}

			m.OnInactive(conn)
			awaitKeepAliveSignal(t, currentAttempt(m), "eviction callback completion")
			require.EqualValues(t, tc.maxRetries, sends.Load(), "retry limit is number of completed sends")
			require.EqualValues(t, 1, evictions.Load())
			require.EqualValues(t, tc.maxRetries, m.numFails.Load())
			require.EqualValues(t, tc.maxRetries, cancellations.Load(), "each prior or final ping is cancelled once")
		})
	}
}

func TestCoalescedKeepAlivePongBeforeSendCompletion(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sendErr error
	}{
		{name: "successful return"},
		{name: "error return", sendErr: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := newKeepAliveTestConn()
			t.Cleanup(func() { _ = conn.Close() })
			started := make(chan func(), 1)
			releaseReturn := make(chan struct{})
			var releaseOnce sync.Once
			var cancellations atomic.Int32
			m := NewCoalescedKeepAlive[*keepAliveTestConn](3, func(*keepAliveTestConn) {},
				func(_ *keepAliveTestConn, receivePong func()) (func(), error) {
					started <- receivePong
					<-releaseReturn
					return func() { cancellations.Add(1) }, tc.sendErr
				})
			t.Cleanup(func() { releaseOnce.Do(func() { close(releaseReturn) }) })

			m.OnInactive(conn)
			var receivePong func()
			select {
			case receivePong = <-started:
			case <-time.After(time.Second):
				t.Fatal("sender did not start")
			}
			receivePong()
			require.Zero(t, m.numFails.Load(), "matching Pong resets failures immediately")
			releaseOnce.Do(func() { close(releaseReturn) })
			awaitKeepAliveSignal(t, currentAttempt(m), "sender completion")
			require.Zero(t, m.numFails.Load(), "late sender error must not undo matching Pong")
			require.EqualValues(t, 1, cancellations.Load(), "completed ping no longer needs a cancel handle")
		})
	}
}

func TestCoalescedKeepAliveStalePongCannotResetCurrentAttempt(t *testing.T) {
	conn := newKeepAliveTestConn()
	t.Cleanup(func() { _ = conn.Close() })
	callbacks := make(chan func(), 2)
	secondStarted := make(chan struct{})
	releaseSecond := make(chan struct{})
	var releaseOnce sync.Once
	var sends atomic.Int32
	m := NewCoalescedKeepAlive[*keepAliveTestConn](3, func(*keepAliveTestConn) {},
		func(_ *keepAliveTestConn, receivePong func()) (func(), error) {
			attempt := sends.Add(1)
			callbacks <- receivePong
			if attempt == 2 {
				close(secondStarted)
				<-releaseSecond
			}
			return func() {}, nil
		})
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseSecond) }) })

	m.OnInactive(conn)
	oldPong := <-callbacks
	awaitKeepAliveSignal(t, currentAttempt(m), "first attempt completion")
	require.EqualValues(t, 1, m.numFails.Load())

	m.OnInactive(conn)
	awaitKeepAliveSignal(t, secondStarted, "second attempt start")
	oldPong()
	require.EqualValues(t, 1, m.numFails.Load(), "stale feedback must not reset the new attempt")
	releaseOnce.Do(func() { close(releaseSecond) })
	newPong := <-callbacks
	awaitKeepAliveSignal(t, currentAttempt(m), "second attempt completion")
	newPong()
	require.Zero(t, m.numFails.Load(), "matching feedback resets the current generation")
}

func TestCoalescedKeepAliveCloseCancelsPendingAdmission(t *testing.T) {
	conn := newKeepAliveTestConn()
	started := make(chan struct{})
	cancelled := make(chan struct{})
	releaseSender := make(chan struct{})
	var releaseOnce sync.Once
	var sends atomic.Int32
	m := NewCoalescedKeepAlive[*keepAliveTestConn](3, func(*keepAliveTestConn) {},
		func(cc *keepAliveTestConn, _ func()) (func(), error) {
			sends.Add(1)
			close(started)
			<-cc.Context().Done()
			close(cancelled)
			<-releaseSender
			return nil, cc.Context().Err()
		})
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseSender) })
		_ = conn.Close()
	})

	m.OnInactive(conn)
	m.mu.Lock()
	attemptDone, closeDone := m.attemptDone, m.closeDone
	m.mu.Unlock()
	awaitKeepAliveSignal(t, started, "pending sender")
	require.NoError(t, conn.Close())
	awaitKeepAliveSignal(t, cancelled, "sender observing connection close")
	select {
	case <-closeDone:
		t.Fatal("close completion preceded pending sender cleanup")
	default:
	}
	releaseOnce.Do(func() { close(releaseSender) })
	awaitKeepAliveSignal(t, attemptDone, "pending sender completion")
	awaitKeepAliveSignal(t, closeDone, "connection close cleanup")
	require.Zero(t, m.numFails.Load(), "connection cancellation is not a failed ping")
	m.mu.Lock()
	pending := m.pending
	asyncCancel := m.asyncCancel
	m.mu.Unlock()
	require.False(t, pending)
	require.Nil(t, asyncCancel)
	m.OnInactive(conn)
	require.EqualValues(t, 1, sends.Load(), "closed connections do not schedule another sender")
}

func TestCoalescedKeepAliveCloseCleansOutstandingPing(t *testing.T) {
	conn := newKeepAliveTestConn()
	t.Cleanup(func() { _ = conn.Close() })
	var m *KeepAlive[*keepAliveTestConn]
	var cancellations atomic.Int32
	m = NewCoalescedKeepAlive[*keepAliveTestConn](3, func(*keepAliveTestConn) {},
		func(*keepAliveTestConn, func()) (func(), error) {
			return func() {
				cancellations.Add(1)
				m.OnInactive(conn) // Reentrant cancellation observes closed state without deadlocking.
			}, nil
		})
	m.OnInactive(conn)
	awaitKeepAliveSignal(t, currentAttempt(m), "successful sender completion")
	require.EqualValues(t, 1, m.numFails.Load())
	m.mu.Lock()
	closeDone := m.closeDone
	m.mu.Unlock()
	require.NoError(t, conn.Close())
	awaitKeepAliveSignal(t, closeDone, "outstanding ping cancellation")
	require.EqualValues(t, 1, cancellations.Load())
	require.EqualValues(t, 1, m.numFails.Load(), "close does not add a retry failure")
	m.mu.Lock()
	asyncCancel := m.asyncCancel
	pending := m.pending
	m.mu.Unlock()
	require.Nil(t, asyncCancel)
	require.False(t, pending)
}

func TestCoalescedKeepAliveCloseBeforeSenderEntry(t *testing.T) {
	conn := newKeepAliveTestConn()
	t.Cleanup(func() { _ = conn.Close() })
	previousCancelEntered := make(chan struct{})
	releasePreviousCancel := make(chan struct{})
	var releaseOnce sync.Once
	secondSenderStarted := make(chan struct{}, 1)
	var sends atomic.Int32
	m := NewCoalescedKeepAlive[*keepAliveTestConn](3, func(*keepAliveTestConn) {},
		func(_ *keepAliveTestConn, _ func()) (func(), error) {
			attempt := sends.Add(1)
			if attempt == 2 {
				secondSenderStarted <- struct{}{}
			}
			return func() {
				if attempt == 1 {
					close(previousCancelEntered)
					<-releasePreviousCancel
				}
			}, nil
		})
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releasePreviousCancel) })
		_ = conn.Close()
		if attemptDone := currentAttempt(m); attemptDone != nil {
			select {
			case <-attemptDone:
			case <-time.After(time.Second):
				t.Errorf("sender worker did not finish during cleanup")
			}
		}
	})
	m.OnInactive(conn)
	awaitKeepAliveSignal(t, currentAttempt(m), "first sender completion")
	m.OnInactive(conn)
	m.mu.Lock()
	attemptDone, closeDone := m.attemptDone, m.closeDone
	m.mu.Unlock()
	awaitKeepAliveSignal(t, previousCancelEntered, "previous ping cancellation")
	require.NoError(t, conn.Close())
	select {
	case <-closeDone:
		t.Fatal("close completion preceded pending cancellation cleanup")
	default:
	}
	releaseOnce.Do(func() { close(releasePreviousCancel) })
	awaitKeepAliveSignal(t, attemptDone, "cancelled before sender entry")
	awaitKeepAliveSignal(t, closeDone, "connection close cleanup")
	select {
	case <-secondSenderStarted:
		t.Fatal("sender started after the connection closed")
	default:
	}
	require.EqualValues(t, 1, sends.Load())
	require.EqualValues(t, 1, m.numFails.Load(), "closing the next attempt does not count another retry")
}

func TestCoalescedKeepAliveConcurrentChecksCoalesce(t *testing.T) {
	conn := newKeepAliveTestConn()
	t.Cleanup(func() { _ = conn.Close() })
	started := make(chan struct{})
	releaseSender := make(chan struct{})
	var releaseOnce sync.Once
	var sends atomic.Int32
	m := NewCoalescedKeepAlive[*keepAliveTestConn](4, func(*keepAliveTestConn) {},
		func(*keepAliveTestConn, func()) (func(), error) {
			sends.Add(1)
			close(started)
			<-releaseSender
			return nil, nil
		})
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseSender) })
		if attemptDone := currentAttempt(m); attemptDone != nil {
			select {
			case <-attemptDone:
			case <-time.After(time.Second):
				t.Errorf("sender worker did not finish during cleanup")
			}
		}
	})

	const checks = 12
	var callers sync.WaitGroup
	for range checks {
		callers.Add(1)
		go func() {
			defer callers.Done()
			m.OnInactive(conn)
		}()
	}
	joined := make(chan struct{})
	go func() { callers.Wait(); close(joined) }()
	awaitKeepAliveSignal(t, joined, "concurrent inactivity checks")
	awaitKeepAliveSignal(t, started, "single coalesced sender")
	require.EqualValues(t, 1, sends.Load())
	releaseOnce.Do(func() { close(releaseSender) })
	awaitKeepAliveSignal(t, currentAttempt(m), "coalesced sender completion")
	require.EqualValues(t, 1, m.numFails.Load())
}

func TestKeepAliveSynchronousController(t *testing.T) {
	conn := newKeepAliveTestConn()
	t.Cleanup(func() { _ = conn.Close() })
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	m := NewKeepAlive[*keepAliveTestConn](2, func(*keepAliveTestConn) {},
		func(*keepAliveTestConn, func()) (func(), error) {
			close(started)
			<-release
			return func() {}, nil
		})
	returned := make(chan struct{})
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		select {
		case <-returned:
		case <-time.After(time.Second):
			t.Errorf("synchronous sender did not return during cleanup")
		}
	})
	go func() { m.OnInactive(conn); close(returned) }()
	awaitKeepAliveSignal(t, started, "synchronous sender")
	select {
	case <-returned:
		t.Fatal("NewKeepAlive must retain synchronous dispatch")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	awaitKeepAliveSignal(t, returned, "synchronous sender return")
}
