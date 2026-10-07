package inactivity

import (
	"context"
	"sync"
	"unsafe"

	"go.uber.org/atomic"
)

type cancelPingFunc func()

type KeepAlive[C Conn] struct {
	pongToken  atomic.Uint64
	onInactive OnInactiveFunc[C]

	sendPing   func(cc C, receivePong func()) (func(), error)
	cancelPing atomic.UnsafePointer
	numFails   atomic.Uint32

	maxRetries uint32

	coalesced        bool
	mu               sync.Mutex
	pending          bool
	closed           bool
	closeCleanupDone bool
	responded        bool
	generation       uint64
	asyncCancel      func()
	attemptDone      chan struct{}
	closeDone        chan struct{}
}

func NewKeepAlive[C Conn](maxRetries uint32, onInactive OnInactiveFunc[C], sendPing func(cc C, receivePong func()) (func(), error)) *KeepAlive[C] {
	return &KeepAlive[C]{
		maxRetries: maxRetries,
		sendPing:   sendPing,
		onInactive: onInactive,
	}
}

func NewCoalescedKeepAlive[C Conn](maxRetries uint32, onInactive OnInactiveFunc[C], sendPing func(cc C, receivePong func()) (func(), error)) *KeepAlive[C] {
	return &KeepAlive[C]{
		maxRetries: maxRetries,
		sendPing:   sendPing,
		onInactive: onInactive,
		coalesced:  true,
	}
}

func (m *KeepAlive[C]) checkCancelPing() {
	cancelPingPtr := m.cancelPing.Swap(nil)
	if cancelPingPtr != nil {
		cancelPing := *(*cancelPingFunc)(cancelPingPtr)
		cancelPing()
	}
}

func (m *KeepAlive[C]) OnInactive(cc C) {
	if m.coalesced {
		m.onInactiveCoalesced(cc)
		return
	}
	v := m.incrementFails()
	m.checkCancelPing()
	if v > m.maxRetries {
		m.onInactive(cc)
		return
	}
	pongToken := m.pongToken.Add(1)
	cancel, err := m.sendPing(cc, func() {
		if m.pongToken.Load() == pongToken {
			m.resetFails()
		}
	})
	if err != nil {
		return
	}
	m.cancelPing.Store(unsafe.Pointer(&cancel))
}

func (m *KeepAlive[C]) onInactiveCoalesced(cc C) {
	ctx := cc.Context()
	m.mu.Lock()
	if m.closed || ctx.Err() != nil || m.pending {
		m.mu.Unlock()
		return
	}

	evict := m.numFails.Load() >= m.maxRetries
	m.pending = true
	m.responded = false
	m.generation++
	generation := m.generation
	attemptDone := make(chan struct{})
	m.attemptDone = attemptDone
	previousCancel := m.asyncCancel
	m.asyncCancel = nil
	registerClose := m.closeDone == nil
	if registerClose {
		m.closeDone = make(chan struct{})
	}
	m.mu.Unlock()

	if registerClose {
		context.AfterFunc(ctx, m.onCoalescedClose)
	}
	go m.runCoalescedAttempt(cc, ctx, generation, attemptDone, previousCancel, evict)
}

func (m *KeepAlive[C]) runCoalescedAttempt(cc C, ctx context.Context, generation uint64, attemptDone chan struct{}, previousCancel func(), evict bool) {
	if previousCancel != nil {
		previousCancel()
	}
	if evict {
		if ctx.Err() == nil {
			m.onInactive(cc)
		}
		m.finishCoalescedAttempt(generation, attemptDone)
		return
	}

	var cancel func()
	var err error
	if ctx.Err() == nil {
		cancel, err = m.sendPing(cc, func() { m.coalescedPong(generation) })
	} else {
		err = ctx.Err()
	}

	m.mu.Lock()
	current := generation == m.generation
	closed := m.closed || ctx.Err() != nil
	responded := current && m.responded
	cleanupCancel := cancel
	if current && !closed {
		if responded {
			m.numFails.Store(0)
		} else {
			m.numFails.Inc()
			if err == nil {
				m.asyncCancel = cancel
				cleanupCancel = nil
			}
		}
	} else if current {
		m.asyncCancel = nil
	}
	m.mu.Unlock()

	if cleanupCancel != nil {
		cleanupCancel()
	}
	m.finishCoalescedAttempt(generation, attemptDone)
}

func (m *KeepAlive[C]) finishCoalescedAttempt(generation uint64, attemptDone chan struct{}) {
	m.mu.Lock()
	if generation == m.generation {
		m.pending = false
	}
	closeDone := m.maybeFinishCoalescedCloseLocked()
	m.mu.Unlock()
	if closeDone != nil {
		close(closeDone)
	}
	close(attemptDone)
}

func (m *KeepAlive[C]) coalescedPong(generation uint64) {
	m.mu.Lock()
	if generation == m.generation && !m.closed {
		m.responded = true
		m.numFails.Store(0)
	}
	m.mu.Unlock()
}

func (m *KeepAlive[C]) onCoalescedClose() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	cancel := m.asyncCancel
	m.asyncCancel = nil
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	m.mu.Lock()
	m.closeCleanupDone = true
	done := m.maybeFinishCoalescedCloseLocked()
	m.mu.Unlock()
	if done != nil {
		close(done)
	}
}

// maybeFinishCoalescedCloseLocked returns the close completion channel exactly
// once, after both the context callback and any reserved sender have finished.
func (m *KeepAlive[C]) maybeFinishCoalescedCloseLocked() chan struct{} {
	if !m.closed || !m.closeCleanupDone || m.pending || m.closeDone == nil {
		return nil
	}
	done := m.closeDone
	m.closeDone = nil
	return done
}

func (m *KeepAlive[C]) incrementFails() uint32 {
	return m.numFails.Add(1)
}

func (m *KeepAlive[C]) resetFails() {
	m.numFails.Store(0)
}
