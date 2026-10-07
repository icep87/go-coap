package client

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/message/pool"
	coapNet "github.com/plgd-dev/go-coap/v3/net"
	"github.com/plgd-dev/go-coap/v3/net/monitor/inactivity"
	"github.com/plgd-dev/go-coap/v3/net/qblock"
	"github.com/plgd-dev/go-coap/v3/pkg/connections"
	"github.com/stretchr/testify/require"
)

type keepAliveWriteSnapshot struct {
	typ    message.Type
	code   codes.Code
	mid    int32
	ctxNil bool
}

func snapshotKeepAliveWrite(msg *pool.Message) keepAliveWriteSnapshot {
	return keepAliveWriteSnapshot{
		typ:    msg.Type(),
		code:   msg.Code(),
		mid:    msg.MessageID(),
		ctxNil: msg.Context() == nil,
	}
}

type keepAliveTestSession struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	writeStarted  chan *pool.Message
	writeEntered  chan *pool.Message
	writeResults  chan error
	beforeWrite   chan struct{}
	releaseFirst  chan struct{}
	firstSnapshot chan keepAliveWriteSnapshot

	writeMu    sync.Mutex
	writeCalls int
	writes     []keepAliveWriteSnapshot

	hookMu sync.Mutex
	hooks  []EventFunc
	once   sync.Once
}

func newKeepAliveTestSession() *keepAliveTestSession {
	ctx, cancel := context.WithCancel(context.Background())
	return &keepAliveTestSession{
		ctx:           ctx,
		cancel:        cancel,
		done:          make(chan struct{}),
		writeStarted:  make(chan *pool.Message, 8),
		writeEntered:  make(chan *pool.Message, 8),
		writeResults:  make(chan error, 8),
		releaseFirst:  make(chan struct{}),
		firstSnapshot: make(chan keepAliveWriteSnapshot, 1),
	}
}

func (s *keepAliveTestSession) Context() context.Context { return s.ctx }
func (s *keepAliveTestSession) Close() error {
	s.cancel()
	return nil
}
func (*keepAliveTestSession) MaxMessageSize() uint32 { return 2048 }
func (*keepAliveTestSession) RemoteAddr() net.Addr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5683}
}
func (*keepAliveTestSession) LocalAddr() net.Addr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5684}
}
func (*keepAliveTestSession) NetConn() net.Conn { return nil }
func (s *keepAliveTestSession) WriteMessage(msg *pool.Message) error {
	s.writeMu.Lock()
	s.writeCalls++
	call := s.writeCalls
	s.writeMu.Unlock()
	if s.beforeWrite != nil {
		s.writeEntered <- msg
		select {
		case <-s.ctx.Done():
			err := s.ctx.Err()
			s.writeResults <- err
			return err
		case <-s.beforeWrite:
		}
	}
	if call == 1 {
		s.writeStarted <- msg
		select {
		case <-s.ctx.Done():
			err := s.ctx.Err()
			s.writeResults <- err
			return err
		case <-s.releaseFirst:
		}
		s.firstSnapshot <- snapshotKeepAliveWrite(msg)
	}
	s.writeMu.Lock()
	s.writes = append(s.writes, snapshotKeepAliveWrite(msg))
	s.writeMu.Unlock()
	s.writeResults <- nil
	return nil
}
func (*keepAliveTestSession) WriteMulticastMessage(*pool.Message, *net.UDPAddr, ...coapNet.MulticastOption) error {
	return nil
}
func (s *keepAliveTestSession) Run(*Conn) error {
	<-s.ctx.Done()
	s.once.Do(func() {
		s.hookMu.Lock()
		hooks := append([]EventFunc(nil), s.hooks...)
		s.hookMu.Unlock()
		for _, f := range hooks {
			f()
		}
		close(s.done)
	})
	return nil
}
func (s *keepAliveTestSession) AddOnClose(f EventFunc) {
	s.hookMu.Lock()
	s.hooks = append(s.hooks, f)
	s.hookMu.Unlock()
}
func (*keepAliveTestSession) SetContextValue(interface{}, interface{}) {}
func (s *keepAliveTestSession) Done() <-chan struct{}                  { return s.done }

func (s *keepAliveTestSession) closeFirstWrite() {
	select {
	case <-s.releaseFirst:
	default:
		close(s.releaseFirst)
	}
}

func (s *keepAliveTestSession) writesSnapshot() []keepAliveWriteSnapshot {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return append([]keepAliveWriteSnapshot(nil), s.writes...)
}

type pendingAdmissionSweepMonitor struct {
	keepalive interface{ OnInactive(*Conn) }
	returned  chan struct{}
	resume    <-chan struct{}
	once      sync.Once
}

func (m *pendingAdmissionSweepMonitor) Notify() {}
func (m *pendingAdmissionSweepMonitor) CheckInactivity(_ time.Time, cc *Conn) {
	m.keepalive.OnInactive(cc)
	m.once.Do(func() { close(m.returned) })
	<-m.resume
}

type keepAliveSweepConnection struct {
	ctx   context.Context
	addr  net.Addr
	check func(time.Time)
}

func (c *keepAliveSweepConnection) Context() context.Context { return c.ctx }
func (c *keepAliveSweepConnection) CheckExpirations(now time.Time) {
	c.check(now)
}
func (*keepAliveSweepConnection) Close() error           { return nil }
func (c *keepAliveSweepConnection) RemoteAddr() net.Addr { return c.addr }

func TestQBlockKeepAlivePendingAdmissionDoesNotBlockSweep(t *testing.T) {
	clock := newFakeQBlockClock(time.Now())
	domain := newQBlockEndpointDomain(clock, 1, 4, 8)
	var holdGateDeadlineReset atomic.Bool
	gateDeadlineTimerReset := make(chan struct{})
	allowGateDeadlineTimerReset := make(chan struct{})
	var gateDeadlineTimerResetOnce sync.Once
	var allowGateDeadlineTimerResetOnce sync.Once
	clock.onReset = func(_ uint32, _ time.Time, _ time.Duration, deadline time.Time) {
		if !holdGateDeadlineReset.Load() {
			return
		}
		domain.mu.Lock()
		isGateDeadline := false
		for _, state := range domain.peers {
			if state.gate.state == qblockProbeWaiting && state.gate.deadline.Equal(deadline) {
				isGateDeadline = true
				break
			}
		}
		domain.mu.Unlock()
		if isGateDeadline {
			gateDeadlineTimerResetOnce.Do(func() { close(gateDeadlineTimerReset) })
			<-allowGateDeadlineTimerReset
		}
	}
	monitorReturned := make(chan struct{})
	resumeSweep := make(chan struct{})
	var resumeOnce sync.Once
	var evictions atomic.Int32
	var sendCalls atomic.Int32
	keepalive := inactivity.NewCoalescedKeepAlive[*Conn](3, func(*Conn) { evictions.Add(1) }, func(cc *Conn, receivePong func()) (func(), error) {
		sendCalls.Add(1)
		return cc.AsyncPing(receivePong)
	})
	ownerMonitor := &pendingAdmissionSweepMonitor{keepalive: keepalive, returned: monitorReturned, resume: resumeSweep}
	ownerSession := newKeepAliveTestSession()
	ownerSession.closeFirstWrite()
	var (
		owner          *Conn
		later          *Conn
		laterSession   *keepAliveTestSession
		ownerRunDone   chan struct{}
		ownerWriteDone chan struct{}
		sweepDone      chan struct{}
		releaseRequest = func() {}
	)
	t.Cleanup(func() {
		resumeOnce.Do(func() { close(resumeSweep) })
		allowGateDeadlineTimerResetOnce.Do(func() { close(allowGateDeadlineTimerReset) })
		_ = ownerSession.Close()
		if sweepDone != nil {
			select {
			case <-sweepDone:
			case <-time.After(time.Second):
				t.Errorf("expiration sweep goroutine did not exit")
			}
		}
		if ownerWriteDone != nil {
			select {
			case <-ownerWriteDone:
			case <-time.After(time.Second):
				t.Errorf("gate owner request did not exit after close")
			}
		}
		if ownerRunDone != nil {
			select {
			case <-ownerSession.done:
			case <-time.After(time.Second):
				t.Errorf("owner session hooks did not finish")
			}
			select {
			case <-ownerRunDone:
			case <-time.After(time.Second):
				t.Errorf("owner session Run goroutine did not exit")
			}
		}
		releaseRequest()
		if laterSession != nil {
			_ = laterSession.Close()
		}
	})
	ownerCfg := DefaultConfig
	ownerCfg.MessagePool = pool.New(8, 2048)
	ownerCfg.TransmissionMaxRetransmit = 4
	ownerCfg.Errors = func(error) {}
	owner = NewConnWithOpts(ownerSession, &ownerCfg,
		withQBlockClient(qblockClientConfig{Manager: qblock.DefaultManagerConfig(), Clock: clock, Endpoint: domain, ScheduleMode: qblockScheduleManual}),
		WithInactivityMonitor(ownerMonitor))
	ownerRunDone = make(chan struct{})
	go func() {
		_ = ownerSession.Run(owner)
		close(ownerRunDone)
	}()

	later, laterSession, _ = newKeepAliveTestConn(t, 4)
	request := owner.AcquireMessage(owner.Context())
	request.SetType(message.Confirmable)
	request.SetCode(codes.GET)
	request.SetToken(message.Token{0x71})
	var requestReleaseOnce sync.Once
	releaseRequest = func() { requestReleaseOnce.Do(func() { owner.ReleaseMessage(request) }) }
	ownerWriteDone = make(chan struct{})
	var ownerWriteErr error
	go func() {
		ownerWriteErr = owner.writeMessage(request)
		close(ownerWriteDone)
	}()
	firstKeepAliveWrite(t, ownerSession)
	mid := request.MessageID()
	ownerMID, ok := owner.midHandlerContainer.Load(mid)
	require.True(t, ok, "ordinary confirmable request owns the gate")
	sweepNow := time.Now().Add(2 * time.Second)
	ownerMID.deadline = sweepNow.Add(-time.Second)

	domain.mu.Lock()
	state := owner.qblockClient.endpoint.stateLocked()
	var stateOwner uint64
	var stateGate qblockProbeState
	var stateAttempts uint32
	if state != nil {
		stateOwner = state.owner
		stateGate = state.gate.state
		stateAttempts = state.attempts
	}
	domain.mu.Unlock()
	require.NotNil(t, state)
	require.NotNil(t, ownerMID.ordinary)
	require.Equal(t, ownerMID.ordinary.member.id, stateOwner)
	require.Equal(t, qblockProbeActive, stateGate)
	attemptFinished := func(member *qblockEndpointMember) bool {
		domain.mu.Lock()
		defer domain.mu.Unlock()
		state := member.stateLocked()
		return state != nil && state.owner == member.id && state.attempts == 0
	}
	require.Eventually(t, func() bool { return attemptFinished(ownerMID.ordinary.member) }, time.Second, time.Millisecond,
		"the gate owner's initial write must finish before checking the pending ping")
	domain.mu.Lock()
	state = owner.qblockClient.endpoint.stateLocked()
	if state != nil {
		stateAttempts = state.attempts
	}
	domain.mu.Unlock()
	require.Zero(t, stateAttempts)

	var callIndex atomic.Uint32
	var eventMu sync.Mutex
	events := make([]string, 0, 4)
	record := func(event string) {
		eventMu.Lock()
		events = append(events, event)
		eventMu.Unlock()
	}
	connectionsToCheck := connections.New()
	check := func(now time.Time) {
		switch callIndex.Add(1) {
		case 1:
			owner.CheckExpirations(now)
			if _, present := owner.midHandlerContainer.Load(mid); !present {
				record("expired MID removed")
			}
		case 2:
			record("later connection checked")
			later.CheckExpirations(now)
		default:
			t.Errorf("unexpected additional connection expiration check")
		}
	}
	connectionsToCheck.Store(&keepAliveSweepConnection{ctx: context.Background(), addr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 7001}, check: check})
	connectionsToCheck.Store(&keepAliveSweepConnection{ctx: context.Background(), addr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 7002}, check: check})
	sweepDone = make(chan struct{})
	go func() {
		connectionsToCheck.CheckExpirations(sweepNow)
		record("sweep returned")
		close(sweepDone)
	}()

	select {
	case <-monitorReturned:
	case <-time.After(time.Second):
		t.Fatal("keep-alive admission blocked the expiration checker before MID maintenance")
	}
	require.Eventually(t, func() bool {
		domain.mu.Lock()
		defer domain.mu.Unlock()
		state := owner.qblockClient.endpoint.stateLocked()
		return state != nil && len(state.waiters) == 1
	}, time.Second, time.Millisecond, "one pending ping admission must be visible")
	record("pending admission")
	for range 100 {
		keepalive.OnInactive(owner)
	}
	domain.mu.Lock()
	state = owner.qblockClient.endpoint.stateLocked()
	pendingWaiters := 0
	if state != nil {
		pendingWaiters = len(state.waiters)
	}
	domain.mu.Unlock()
	require.Equal(t, 1, pendingWaiters, "many checks retain one admission waiter")
	require.Len(t, ownerSession.writesSnapshot(), 1, "pending admission emits no ping")
	require.Zero(t, evictions.Load(), "pending admission does not consume retries or evict")
	holdGateDeadlineReset.Store(true)
	resumeOnce.Do(func() { close(resumeSweep) })
	select {
	case <-sweepDone:
	case <-time.After(time.Second):
		t.Fatal("expiration sweep did not finish after resuming the keep-alive monitor")
	}
	eventMu.Lock()
	gotEvents := append([]string(nil), events...)
	eventMu.Unlock()
	require.Equal(t, []string{"pending admission", "expired MID removed", "later connection checked", "sweep returned"}, gotEvents)

	// The sweep has returned while one ordinary Q admission is still pending.
	// Advance only now, after the owner expiry and all connection checks.
	domain.mu.Lock()
	state = owner.qblockClient.endpoint.stateLocked()
	var readyAt time.Time
	var waitingState qblockProbeState
	var waitingCount int
	if state != nil {
		readyAt = state.gate.deadline
		waitingState = state.gate.state
		waitingCount = len(state.waiters)
	}
	domain.mu.Unlock()
	require.NotNil(t, state)
	require.Equal(t, qblockProbeWaiting, waitingState)
	require.Equal(t, 1, waitingCount)
	require.True(t, readyAt.After(clock.Now()), "the expired owner's charged bytes retain pacing debt")
	select {
	case <-gateDeadlineTimerReset:
	case <-time.After(time.Second):
		t.Fatal("the pacing timer was not armed at the gate deadline")
	}
	require.Equal(t, readyAt, clock.deadline(), "the timer reset must target the exact gate deadline")
	clock.Advance(readyAt.Sub(clock.Now()))
	allowGateDeadlineTimerResetOnce.Do(func() { close(allowGateDeadlineTimerReset) })
	admissionWaitDeadline := time.Now().Add(time.Second)
	for len(ownerSession.writesSnapshot()) != 2 && time.Now().Before(admissionWaitDeadline) {
		time.Sleep(time.Millisecond)
	}
	if len(ownerSession.writesSnapshot()) != 2 {
		require.Len(t, ownerSession.writesSnapshot(), 2, "the pending keep-alive ping should be admitted once pacing expires")
	}
	writes := ownerSession.writesSnapshot()
	require.Equal(t, codes.GET, writes[0].code)
	require.Equal(t, message.Confirmable, writes[0].typ)
	require.Equal(t, codes.Empty, writes[1].code)
	require.Equal(t, message.Confirmable, writes[1].typ)
	pingMID := writes[1].mid
	pingElem, ok := owner.midHandlerContainer.Load(pingMID)
	require.True(t, ok)
	require.NotNil(t, pingElem.ordinary)
	require.Eventually(t, func() bool { return attemptFinished(pingElem.ordinary.member) }, time.Second, time.Millisecond,
		"the keep-alive initial write must finish before matching feedback")
	pingMessage := owner.AcquireMessage(owner.Context())
	pingMessage.SetType(message.Confirmable)
	pingMessage.SetCode(codes.Empty)
	pingMessage.SetMessageID(pingMID)
	pingBytes, err := qblockDatagramSize(pingMessage)
	owner.ReleaseMessage(pingMessage)
	require.NoError(t, err)
	domain.mu.Lock()
	state = owner.qblockClient.endpoint.stateLocked()
	var pingOwner uint64
	var pingGate qblockProbeState
	var chargedBytes uint64
	if state != nil {
		pingOwner = state.owner
		pingGate = state.gate.state
		chargedBytes = state.gate.bytes
	}
	domain.mu.Unlock()
	require.NotNil(t, state)
	require.Equal(t, pingElem.ordinary.member.id, pingOwner)
	require.Equal(t, qblockProbeActive, pingGate)
	require.Equal(t, pingBytes, chargedBytes, "only the encoded ping datagram is charged after admission")

	ack := owner.AcquireMessage(owner.Context())
	ack.SetType(message.Acknowledgement)
	ack.SetCode(codes.Empty)
	ack.SetMessageID(pingMID)
	owner.handleSpecialMessages(ack)
	owner.ReleaseMessage(ack)
	_, pingMIDPresent := owner.midHandlerContainer.Load(pingMID)
	require.False(t, pingMIDPresent)
	domain.mu.Lock()
	state = owner.qblockClient.endpoint.stateLocked()
	var settled bool
	if state != nil {
		settled = state.owner == 0 && state.gate.state == qblockProbeOpen && state.gate.bytes == 0 && len(state.waiters) == 0
	}
	domain.mu.Unlock()
	require.True(t, settled, "matching feedback settles the keep-alive permit without disturbing the connection member")

	// A new local probe can be cancelled while a following probe waits behind its
	// charged debt. This exercises cancellation settlement after admission.
	blockSecondWrite := make(chan struct{})
	ownerSession.beforeWrite = blockSecondWrite
	var secondWriterMessage *pool.Message
	require.Eventually(t, func() bool {
		keepalive.OnInactive(owner)
		select {
		case secondWriterMessage = <-ownerSession.writeEntered:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond, "a new probe should start after the prior worker finishes")
	_, firstPingReappeared := owner.midHandlerContainer.Load(pingMID)
	require.False(t, firstPingReappeared, "a completed Pong must not republish its MID during the next probe")
	secondPingMID := secondWriterMessage.MessageID()
	secondPing, ok := owner.midHandlerContainer.Load(secondPingMID)
	require.True(t, ok)
	require.NotNil(t, secondPing.ordinary)
	close(blockSecondWrite)
	require.Eventually(t, func() bool { return attemptFinished(secondPing.ordinary.member) }, time.Second, time.Millisecond,
		"the second keep-alive initial write must finish before testing its cancellation")
	require.Len(t, ownerSession.writesSnapshot(), 3)
	require.Eventually(t, func() bool {
		keepalive.OnInactive(owner)
		if _, stillPresent := owner.midHandlerContainer.Load(secondPingMID); stillPresent {
			return false
		}
		domain.mu.Lock()
		defer domain.mu.Unlock()
		state := owner.qblockClient.endpoint.stateLocked()
		return state != nil && state.gate.state == qblockProbeWaiting && state.gate.bytes == pingBytes && len(state.waiters) == 1
	}, time.Second, time.Millisecond, "cancelling the admitted ping must retain its byte debt and one following waiter")
	require.Len(t, ownerSession.writesSnapshot(), 3, "pending checks must not write another ping")
	_, firstPingReappeared = owner.midHandlerContainer.Load(pingMID)
	require.False(t, firstPingReappeared, "a stale ping cancel must not recreate its MID")

	// Cleanup is explicit here so the test also verifies pending Q admission is
	// cancelled by connection close without retaining its MID or ordinary permit.
	_ = ownerSession.Close()
	select {
	case <-ownerRunDone:
	case <-time.After(time.Second):
		t.Fatal("owner close did not finish")
	}
	select {
	case <-ownerWriteDone:
	case <-time.After(time.Second):
		t.Fatal("owner request did not finish after close")
	}
	require.Error(t, ownerWriteErr)
	require.Eventually(t, func() bool {
		domain.mu.Lock()
		defer domain.mu.Unlock()
		state := domain.peers[owner.qblockClient.endpoint.peer]
		return len(domain.members) == 0 && (state == nil || len(state.waiters) == 0)
	}, time.Second, time.Millisecond, "close must release permits and pending admission")
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && owner.midHandlerContainer.Length() != 0 {
		time.Sleep(time.Millisecond)
	}
	var remainingMIDs []int32
	owner.midHandlerContainer.Range(func(mid int32, _ *midElement) bool {
		remainingMIDs = append(remainingMIDs, mid)
		return true
	})
	require.Empty(t, remainingMIDs, "pending ping cleanup must remove its MID after close")
	owner.ordinaryMu.Lock()
	ordinaryCount := len(owner.ordinary)
	owner.ordinaryMu.Unlock()
	require.Zero(t, ordinaryCount)
	releaseRequest()
}

func TestQBlockKeepAliveCloseCancelsAfterAdmission(t *testing.T) {
	for _, tc := range []struct {
		name         string
		waitForWrite bool
	}{
		{name: "immediately after admission"},
		{name: "during context-aware initial write", waitForWrite: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := newFakeQBlockClock(time.Now())
			domain := newQBlockEndpointDomain(clock, 1, 4, 8)
			session := newKeepAliveTestSession()
			session.beforeWrite = make(chan struct{})
			if tc.waitForWrite {
				close(session.beforeWrite)
			}
			cc, session, runDone := newKeepAliveTestConnWithSession(t, session, 4,
				withQBlockClient(qblockClientConfig{Manager: qblock.DefaultManagerConfig(), Clock: clock, Endpoint: domain, ScheduleMode: qblockScheduleManual}))
			senderReturned := make(chan error, 1)
			keepalive := inactivity.NewCoalescedKeepAlive[*Conn](3, func(*Conn) {}, func(cc *Conn, receivePong func()) (func(), error) {
				cancel, err := cc.AsyncPing(receivePong)
				senderReturned <- err
				return cancel, err
			})
			keepalive.OnInactive(cc)
			if tc.waitForWrite {
				firstKeepAliveWrite(t, session)
			} else {
				select {
				case <-session.writeEntered:
				case <-time.After(time.Second):
					t.Fatal("admitted ping did not reach the pre-write barrier")
				}
			}

			domain.mu.Lock()
			state := cc.qblockClient.endpoint.stateLocked()
			var gateState qblockProbeState
			var attempts uint32
			var charged uint64
			if state != nil {
				gateState = state.gate.state
				attempts = state.attempts
				charged = state.gate.bytes
			}
			domain.mu.Unlock()
			require.NotNil(t, state)
			require.Equal(t, qblockProbeActive, gateState)
			require.EqualValues(t, 1, attempts)
			require.Positive(t, charged)

			require.NoError(t, session.Close())
			select {
			case err := <-session.writeResults:
				require.Error(t, err, "close cancels the context-aware transport write")
			case <-time.After(time.Second):
				t.Fatal("initial ping write did not return after close")
			}
			select {
			case err := <-senderReturned:
				require.Error(t, err, "AsyncPing returns after its context-aware write is cancelled")
			case <-time.After(time.Second):
				t.Fatal("AsyncPing did not return after close")
			}
			select {
			case <-runDone:
			case <-time.After(time.Second):
				t.Fatal("session close hooks did not complete")
			}
			require.Empty(t, session.writesSnapshot(), "a write cancelled before success is not recorded")
			require.Eventually(t, func() bool {
				domain.mu.Lock()
				defer domain.mu.Unlock()
				peerState := domain.peers[cc.qblockClient.endpoint.peer]
				return len(domain.members) == 0 && (peerState == nil || (peerState.attempts == 0 && len(peerState.waiters) == 0))
			}, time.Second, time.Millisecond, "close settles the admitted permit and removes endpoint membership")
			require.Eventually(t, func() bool { return cc.midHandlerContainer.Length() == 0 }, time.Second, time.Millisecond)
			cc.ordinaryMu.Lock()
			ordinaryCount := len(cc.ordinary)
			cc.ordinaryMu.Unlock()
			require.Zero(t, ordinaryCount)
		})
	}
}

func newKeepAliveTestConn(t *testing.T, maxRetransmit uint32, qEnabled ...bool) (*Conn, *keepAliveTestSession, <-chan struct{}) {
	t.Helper()
	session := newKeepAliveTestSession()
	var opts []Option
	if len(qEnabled) > 0 && qEnabled[0] {
		clock := newFakeQBlockClock(time.Now())
		domain := newQBlockEndpointDomain(clock, 1, 4, 8)
		opts = append(opts, withQBlockClient(qblockClientConfig{
			Manager:      qblock.DefaultManagerConfig(),
			Clock:        clock,
			Endpoint:     domain,
			ScheduleMode: qblockScheduleManual,
		}))
	}
	return newKeepAliveTestConnWithSession(t, session, maxRetransmit, opts...)
}

func newKeepAliveTestConnWithSession(t *testing.T, session *keepAliveTestSession, maxRetransmit uint32, opts ...Option) (*Conn, *keepAliveTestSession, <-chan struct{}) {
	t.Helper()
	cfg := DefaultConfig
	cfg.MessagePool = pool.New(8, 2048)
	cfg.TransmissionMaxRetransmit = maxRetransmit
	cfg.Errors = func(error) {}
	cc := NewConnWithOpts(session, &cfg, opts...)
	runDone := make(chan struct{})
	go func() {
		_ = session.Run(cc)
		close(runDone)
	}()
	t.Cleanup(func() {
		_ = session.Close()
		session.closeFirstWrite()
		select {
		case <-session.done:
		case <-time.After(time.Second):
			t.Errorf("session Run did not finish")
		}
		select {
		case <-runDone:
		case <-time.After(time.Second):
			t.Errorf("session Run goroutine did not exit")
		}
	})
	return cc, session, runDone
}

func startKeepAlivePing(cc *Conn, callbacks ...func()) <-chan struct {
	cancel func()
	err    error
} {
	receivedPong := func() {}
	if len(callbacks) > 0 {
		receivedPong = callbacks[0]
	}
	done := make(chan struct {
		cancel func()
		err    error
	}, 1)
	go func() {
		cancel, err := cc.AsyncPing(receivedPong)
		done <- struct {
			cancel func()
			err    error
		}{cancel: cancel, err: err}
	}()
	return done
}

func firstKeepAliveWrite(t *testing.T, s *keepAliveTestSession) *pool.Message {
	t.Helper()
	select {
	case msg := <-s.writeStarted:
		return msg
	case <-time.After(time.Second):
		t.Fatal("initial ping write did not start")
		return nil
	}
}

func awaitKeepAlivePing(t *testing.T, done <-chan struct {
	cancel func()
	err    error
}) struct {
	cancel func()
	err    error
} {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(time.Second):
		t.Fatal("AsyncPing did not return")
		return struct {
			cancel func()
			err    error
		}{}
	}
}

func TestKeepAlivePingInitialWriteDoesNotBlockMaintenance(t *testing.T) {
	cc, session, _ := newKeepAliveTestConn(t, 4)
	done := startKeepAlivePing(cc)
	firstKeepAliveWrite(t, session)

	const unrelatedMID = int32(123)
	_, loaded := cc.storeMIDHandler(unrelatedMID, &midElement{deadline: time.Now().Add(-time.Second)})
	require.False(t, loaded)

	checked := make(chan struct{})
	go func() {
		cc.CheckExpirations(time.Now().Add(5 * time.Second))
		close(checked)
	}()
	select {
	case <-checked:
	case <-time.After(time.Second):
		t.Fatal("expiration maintenance blocked by the initial ping write")
	}

	session.writeMu.Lock()
	writeCalls := session.writeCalls
	session.writeMu.Unlock()
	require.Equal(t, 1, writeCalls, "MID maintenance must not retransmit before initial write completion")
	_, stillPresent := cc.midHandlerContainer.Load(unrelatedMID)
	require.False(t, stillPresent, "the sweep must continue through unrelated expired MIDs")

	session.closeFirstWrite()
	result := awaitKeepAlivePing(t, done)
	require.NoError(t, result.err)
	result.cancel()
}

func TestKeepAlivePingInitialWriterSurvivesTerminalCleanup(t *testing.T) {
	for _, tc := range []struct {
		name          string
		maxRetransmit uint32
		feedback      message.Type
	}{
		{name: "ack", maxRetransmit: 4, feedback: message.Acknowledgement},
		{name: "reset", maxRetransmit: 4, feedback: message.Reset},
		{name: "expiry", maxRetransmit: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cc, session, _ := newKeepAliveTestConn(t, tc.maxRetransmit)
			pongEntered := make(chan struct{})
			releasePong := make(chan struct{})
			var releasePongOnce sync.Once
			receivedPong := func() {
				close(pongEntered)
				<-releasePong
			}
			t.Cleanup(func() { releasePongOnce.Do(func() { close(releasePong) }) })
			done := startKeepAlivePing(cc, receivedPong)
			writerMessage := firstKeepAliveWrite(t, session)
			mid := writerMessage.MessageID()
			elem, ok := cc.midHandlerContainer.Load(mid)
			require.True(t, ok)

			if tc.feedback == 0 {
				// A zero retransmit budget terminalizes this ping on the first
				// sweep while the original writer is still using its message.
				cc.checkMidHandlerContainer(time.Now(), tc.maxRetransmit, time.Second, mid, elem)
				got := snapshotKeepAliveWrite(writerMessage)
				require.Equal(t, codes.Empty, got.code)
				require.Equal(t, message.Confirmable, got.typ)
				require.Equal(t, mid, got.mid)
				require.False(t, got.ctxNil)
			} else {
				resp := cc.AcquireMessage(cc.Context())
				resp.SetType(tc.feedback)
				resp.SetCode(codes.Empty)
				resp.SetMessageID(mid)
				handled := make(chan bool, 1)
				go func() { handled <- cc.handleSpecialMessages(resp) }()
				select {
				case <-pongEntered:
				case <-time.After(time.Second):
					t.Fatal("ping response handler did not reach callback")
				}
				// Feedback has released the retained MID message and paused in the
				// callback, before the initial writer is allowed to continue.
				got := snapshotKeepAliveWrite(writerMessage)
				require.Equal(t, codes.Empty, got.code)
				require.Equal(t, message.Confirmable, got.typ)
				require.Equal(t, mid, got.mid)
				require.False(t, got.ctxNil)
				releasePongOnce.Do(func() { close(releasePong) })
				require.False(t, <-handled)
				cc.ReleaseMessage(resp)
			}

			session.closeFirstWrite()
			select {
			case got := <-session.firstSnapshot:
				require.Equal(t, codes.Empty, got.code)
				require.Equal(t, message.Confirmable, got.typ)
				require.Equal(t, mid, got.mid)
				require.False(t, got.ctxNil)
			case <-time.After(time.Second):
				t.Fatal("blocked writer did not capture its request")
			}
			result := awaitKeepAlivePing(t, done)
			if result.cancel != nil {
				result.cancel()
			}
		})
	}
}

func TestKeepAlivePingInitialWriterSurvivesQBlockFeedback(t *testing.T) {
	for _, typ := range []message.Type{message.Acknowledgement, message.Reset} {
		t.Run(typ.String(), func(t *testing.T) {
			cc, session, _ := newKeepAliveTestConn(t, 4, true)
			pongEntered := make(chan struct{})
			releasePong := make(chan struct{})
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(releasePong) }) })
			done := startKeepAlivePing(cc, func() {
				close(pongEntered)
				<-releasePong
			})
			writerMessage := firstKeepAliveWrite(t, session)
			mid := writerMessage.MessageID()
			require.Eventually(t, func() bool {
				m := cc.qblockClient.endpoint
				m.domain.mu.Lock()
				defer m.domain.mu.Unlock()
				s := m.stateLocked()
				return s != nil && s.attempts == 1 && s.gate.bytes > 0
			}, time.Second, time.Millisecond)

			resp := cc.AcquireMessage(cc.Context())
			resp.SetType(typ)
			resp.SetCode(codes.Empty)
			resp.SetMessageID(mid)
			handled := make(chan bool, 1)
			go func() { handled <- cc.handleSpecialMessages(resp) }()
			select {
			case <-pongEntered:
			case <-time.After(time.Second):
				t.Fatal("Q-block ping feedback did not reach callback")
			}
			got := snapshotKeepAliveWrite(writerMessage)
			require.Equal(t, codes.Empty, got.code)
			require.Equal(t, message.Confirmable, got.typ)
			require.Equal(t, mid, got.mid)
			require.False(t, got.ctxNil)

			releaseOnce.Do(func() { close(releasePong) })
			require.False(t, <-handled)
			cc.ReleaseMessage(resp)
			session.closeFirstWrite()
			result := awaitKeepAlivePing(t, done)
			require.NoError(t, result.err)
			result.cancel()
		})
	}
}

func TestKeepAlivePingStaleCancelPreservesReplacementMID(t *testing.T) {
	cc, session, _ := newKeepAliveTestConn(t, 4)
	done := startKeepAlivePing(cc)
	writerMessage := firstKeepAliveWrite(t, session)
	mid := writerMessage.MessageID()
	session.closeFirstWrite()
	result := awaitKeepAlivePing(t, done)
	require.NoError(t, result.err)
	original, ok := cc.midHandlerContainer.Load(mid)
	require.True(t, ok)
	require.True(t, cc.removeMIDHandlerIfMatch(mid, original))
	original.ReleaseMessage(cc)

	replacement := &midElement{start: time.Now()}
	_, loaded := cc.storeMIDHandler(mid, replacement)
	require.False(t, loaded)
	result.cancel()
	got, ok := cc.midHandlerContainer.Load(mid)
	require.True(t, ok)
	require.Same(t, replacement, got)
	require.False(t, replacement.private.terminal)
	require.True(t, cc.removeMIDHandlerIfMatch(mid, replacement))
	replacement.ReleaseMessage(cc)
	result.cancel()
	_, stalePresent := cc.midHandlerContainer.Load(mid)
	require.False(t, stalePresent, "repeated stale cancellation must not create a nil MID entry")
	require.Zero(t, cc.midHandlerContainer.Length())
}

func TestKeepAlivePingFeedbackRemovalLeavesReplacementAndPermitUntouched(t *testing.T) {
	cc, _, _ := newKeepAliveTestConn(t, 4, true)
	msg := cc.AcquireMessage(cc.Context())
	defer cc.ReleaseMessage(msg)
	msg.SetType(message.Confirmable)
	msg.SetCode(codes.Empty)
	msg.SetMessageID(77)
	permit, err := cc.acquireOrdinary(msg)
	require.NoError(t, err)
	require.True(t, permit.member.owns(1))
	original := &midElement{ordinary: permit}
	_, loaded := cc.storeMIDHandler(77, original)
	require.False(t, loaded)
	replacement := &midElement{}
	cc.midHandlerContainer.Replace(77, replacement)

	// This is the conditional removal used after feedback validates a MID. A
	// replacement that won the race remains published and the old permit remains
	// untouched; the caller must not dispatch the old handler.
	require.False(t, cc.removeMIDHandlerIfMatch(77, original))
	got, ok := cc.midHandlerContainer.Load(77)
	require.True(t, ok)
	require.Same(t, replacement, got)
	require.True(t, permit.member.owns(1))
	permit.finish(false, permit.member.domain.clock.Now())
	require.True(t, cc.removeMIDHandlerIfMatch(77, replacement))
	replacement.ReleaseMessage(cc)
}

func TestKeepAlivePingCloseCancelsInitialWrite(t *testing.T) {
	cc, session, _ := newKeepAliveTestConn(t, 4)
	done := startKeepAlivePing(cc)
	writerMessage := firstKeepAliveWrite(t, session)
	mid := writerMessage.MessageID()
	require.Eventually(t, func() bool {
		_, ok := cc.midHandlerContainer.Load(mid)
		return ok
	}, time.Second, time.Millisecond)

	require.NoError(t, session.Close())
	result := awaitKeepAlivePing(t, done)
	require.Error(t, result.err)
	_, ok := cc.midHandlerContainer.Load(mid)
	require.False(t, ok)
	select {
	case <-session.done:
	case <-time.After(time.Second):
		t.Fatal("session close hooks did not finish")
	}
}

func TestPrepareWriteMessageCleanupPreservesReplacementMID(t *testing.T) {
	cc, _, _ := newKeepAliveTestConn(t, 4)
	request := cc.AcquireMessage(cc.Context())
	request.SetType(message.Confirmable)
	request.SetCode(codes.GET)
	request.SetMessageID(0x1234)
	t.Cleanup(func() { cc.ReleaseMessage(request) })

	closeFn, err := cc.prepareWriteMessage(request, nil, nil)
	require.NoError(t, err)
	var closeOnce sync.Once
	cleanupMessage := func() { closeOnce.Do(closeFn) }
	t.Cleanup(cleanupMessage)
	original, ok := cc.midHandlerContainer.Load(request.MessageID())
	require.True(t, ok)
	t.Cleanup(func() { original.ReleaseMessage(cc) })

	replacement := &midElement{}
	replaced := false
	cc.midHandlerContainer.ReplaceWithFunc(request.MessageID(), func(_ *midElement, loaded bool) (*midElement, bool) {
		replaced = loaded
		return replacement, false
	})
	require.True(t, replaced)
	t.Cleanup(func() {
		cc.removeMIDHandlerIfMatch(request.MessageID(), replacement)
		replacement.ReleaseMessage(cc)
	})

	cleanupMessage()
	actual, ok := cc.midHandlerContainer.Load(request.MessageID())
	require.True(t, ok, "cleanup for the original request must not remove a replacement MID")
	require.Same(t, replacement, actual)
}
