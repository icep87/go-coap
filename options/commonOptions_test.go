package options_test

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dtlsServer "github.com/plgd-dev/go-coap/v3/dtls/server"
	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/pool"
	"github.com/plgd-dev/go-coap/v3/mux"
	coapNet "github.com/plgd-dev/go-coap/v3/net"
	"github.com/plgd-dev/go-coap/v3/net/blockwise"
	"github.com/plgd-dev/go-coap/v3/net/qblock"
	"github.com/plgd-dev/go-coap/v3/net/responsewriter"
	"github.com/plgd-dev/go-coap/v3/options"
	"github.com/plgd-dev/go-coap/v3/options/config"
	"github.com/plgd-dev/go-coap/v3/pkg/runner/periodic"
	"github.com/plgd-dev/go-coap/v3/tcp"
	"github.com/plgd-dev/go-coap/v3/tcp/client"
	"github.com/plgd-dev/go-coap/v3/tcp/server"
	"github.com/plgd-dev/go-coap/v3/udp"
	udpClient "github.com/plgd-dev/go-coap/v3/udp/client"
	udpServer "github.com/plgd-dev/go-coap/v3/udp/server"
	"github.com/stretchr/testify/require"
)

type keepAliveOptionSession struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	writeStarted  chan struct{}
	writeReturned chan error
	writeCalls    atomic.Int32
	writeMID      atomic.Int32
	writeRelease  chan struct{}
	releaseOnce   sync.Once

	hookMu sync.Mutex
	hooks  []udpClient.EventFunc
	once   sync.Once
}

func newKeepAliveOptionSession() *keepAliveOptionSession {
	ctx, cancel := context.WithCancel(context.Background())
	return &keepAliveOptionSession{
		ctx:           ctx,
		cancel:        cancel,
		done:          make(chan struct{}),
		writeStarted:  make(chan struct{}, 1),
		writeReturned: make(chan error, 1),
		writeRelease:  make(chan struct{}),
	}
}

func (s *keepAliveOptionSession) Context() context.Context { return s.ctx }
func (s *keepAliveOptionSession) Close() error {
	s.cancel()
	return nil
}
func (*keepAliveOptionSession) MaxMessageSize() uint32 { return 2048 }
func (*keepAliveOptionSession) RemoteAddr() net.Addr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5683}
}
func (*keepAliveOptionSession) LocalAddr() net.Addr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5684}
}
func (*keepAliveOptionSession) NetConn() net.Conn { return nil }
func (s *keepAliveOptionSession) WriteMessage(msg *pool.Message) error {
	if s.writeCalls.Add(1) == 1 {
		s.writeMID.Store(msg.MessageID())
		s.writeStarted <- struct{}{}
	}
	var err error
	select {
	case <-s.ctx.Done():
		err = s.ctx.Err()
	case <-s.writeRelease:
	}
	s.writeReturned <- err
	return err
}
func (s *keepAliveOptionSession) releaseWrites() { s.releaseOnce.Do(func() { close(s.writeRelease) }) }
func (*keepAliveOptionSession) WriteMulticastMessage(*pool.Message, *net.UDPAddr, ...coapNet.MulticastOption) error {
	return nil
}
func (s *keepAliveOptionSession) Run(*udpClient.Conn) error {
	<-s.ctx.Done()
	s.once.Do(func() {
		s.hookMu.Lock()
		hooks := append([]udpClient.EventFunc(nil), s.hooks...)
		s.hookMu.Unlock()
		for _, hook := range hooks {
			hook()
		}
		close(s.done)
	})
	return nil
}
func (s *keepAliveOptionSession) AddOnClose(hook udpClient.EventFunc) {
	s.hookMu.Lock()
	s.hooks = append(s.hooks, hook)
	s.hookMu.Unlock()
}
func (*keepAliveOptionSession) SetContextValue(interface{}, interface{}) {}
func (s *keepAliveOptionSession) Done() <-chan struct{}                  { return s.done }

func TestUDPKeepAliveOptionReturnsWhilePingSendIsPending(t *testing.T) {
	var evictions atomic.Int32
	keepAlive := options.WithKeepAlive[options.UDPOnInactive](3, 4*time.Second, func(*udpClient.Conn) {
		evictions.Add(1)
	})
	clientCfg := udpClient.DefaultConfig
	keepAlive.UDPClientApply(&clientCfg)
	udpServerCfg := udpServer.DefaultConfig
	keepAlive.UDPServerApply(&udpServerCfg)
	dtlsServerCfg := dtlsServer.DefaultConfig
	keepAlive.DTLSServerApply(&dtlsServerCfg)
	qblockClientOpt := options.WithQBlock(qblock.DefaultClientConfig())

	for _, tc := range []struct {
		name    string
		monitor func() udpClient.InactivityMonitor
		qblock  bool
	}{
		{name: "UDP client", monitor: clientCfg.CreateInactivityMonitor},
		{name: "UDP server", monitor: udpServerCfg.CreateInactivityMonitor},
		{name: "DTLS server", monitor: dtlsServerCfg.CreateInactivityMonitor},
		{name: "Q-enabled UDP client", monitor: func() udpClient.InactivityMonitor {
			cfg := udpClient.DefaultConfig
			keepAlive.UDPClientApply(&cfg)
			qblockClientOpt.UDPClientApply(&cfg)
			return cfg.CreateInactivityMonitor()
		}, qblock: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			session := newKeepAliveOptionSession()
			connCfg := udpClient.DefaultConfig
			connCfg.MessagePool = pool.New(8, 2048)
			if tc.qblock {
				qblockClientOpt.UDPClientApply(&connCfg)
			}
			cc := udpClient.NewConnWithOpts(session, &connCfg)
			runDone := make(chan error, 1)
			go func() { runDone <- session.Run(cc) }()
			monitor := tc.monitor()
			var checkDone chan struct{}
			t.Cleanup(func() {
				_ = session.Close()
				if checkDone != nil {
					select {
					case <-checkDone:
					case <-time.After(time.Second):
						t.Errorf("inactivity check did not exit after close")
					}
				}
				select {
				case <-session.done:
				case <-time.After(time.Second):
					t.Errorf("session close hooks did not finish")
				}
				select {
				case <-runDone:
				case <-time.After(time.Second):
					t.Errorf("session Run goroutine did not exit")
				}
			})

			checkDone = make(chan struct{})
			go func() {
				monitor.CheckInactivity(time.Now().Add(5*time.Second), cc)
				close(checkDone)
			}()
			select {
			case <-session.writeStarted:
			case <-time.After(time.Second):
				t.Fatal("keep-alive ping did not reach the session writer")
			}
			select {
			case <-checkDone:
			case <-time.After(time.Second):
				t.Fatal("keep-alive inactivity check blocked on ping admission or write")
			}
			for range 3 {
				monitor.CheckInactivity(time.Now().Add(5*time.Second), cc)
			}
			require.EqualValues(t, 1, session.writeCalls.Load(), "pending checks coalesce to one ping")
			require.Zero(t, evictions.Load())
		})
	}
}

func TestTCPKeepAliveOptionZeroRetries(t *testing.T) {
	var clientEvictions, serverEvictions atomic.Int32
	clientCfg := client.DefaultConfig
	options.WithKeepAlive[options.TCPOnInactive](0, time.Second, func(*client.Conn) {
		clientEvictions.Add(1)
	}).TCPClientApply(&clientCfg)
	serverCfg := server.DefaultConfig
	options.WithKeepAlive[options.TCPOnInactive](0, time.Second, func(*client.Conn) {
		serverEvictions.Add(1)
	}).TCPServerApply(&serverCfg)

	now := time.Now().Add(2 * time.Second)
	clientCfg.CreateInactivityMonitor().CheckInactivity(now, nil)
	serverCfg.CreateInactivityMonitor().CheckInactivity(now, nil)
	require.EqualValues(t, 1, clientEvictions.Load())
	require.EqualValues(t, 1, serverEvictions.Load())
}

func TestUDPKeepAliveProbeDoesNotResetActivity(t *testing.T) {
	keepAlive := options.WithKeepAlive[options.UDPOnInactive](3, 4*time.Second, func(*udpClient.Conn) {})
	monitorCfg := udpClient.DefaultConfig
	keepAlive.UDPClientApply(&monitorCfg)
	monitor := monitorCfg.CreateInactivityMonitor()
	activity, ok := monitor.(interface{ LastActivity() time.Time })
	require.True(t, ok, "the public keep-alive monitor exposes its activity timestamp")

	session := newKeepAliveOptionSession()
	session.releaseWrites()
	connCfg := udpClient.DefaultConfig
	connCfg.MessagePool = pool.New(8, 2048)
	cc := udpClient.NewConnWithOpts(session, &connCfg, udpClient.WithInactivityMonitor(monitor))
	runDone := make(chan error, 1)
	go func() { runDone <- session.Run(cc) }()
	t.Cleanup(func() {
		session.releaseWrites()
		_ = session.Close()
		select {
		case <-session.done:
		case <-time.After(time.Second):
			t.Errorf("session close hooks did not finish")
		}
		select {
		case <-runDone:
		case <-time.After(time.Second):
			t.Errorf("session Run goroutine did not exit")
		}
	})

	before := activity.LastActivity()
	checkDone := make(chan struct{})
	go func() {
		monitor.CheckInactivity(time.Now().Add(5*time.Second), cc)
		close(checkDone)
	}()
	select {
	case <-checkDone:
	case <-time.After(time.Second):
		t.Fatal("UDP inactivity check blocked on the local probe")
	}
	select {
	case <-session.writeStarted:
	case <-time.After(time.Second):
		t.Fatal("local keep-alive ping was not written")
	}
	select {
	case err := <-session.writeReturned:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("local keep-alive write did not complete")
	}
	require.Equal(t, before, activity.LastActivity(), "a locally generated probe is not peer activity")

	time.Sleep(time.Millisecond)
	mid := session.writeMID.Load()
	ack := []byte{0x60, 0, byte(mid >> 8), byte(mid)}
	require.NoError(t, cc.Process(nil, ack))
	require.Eventually(t, func() bool { return activity.LastActivity().After(before) }, time.Second, time.Millisecond,
		"received peer feedback continues to reset activity")
}

func TestCommonTCPServerApply(t *testing.T) {
	cfg := server.Config{}
	handler := func(*responsewriter.ResponseWriter[*client.Conn], *pool.Message) {
		// no-op
	}
	ctx := context.Background()
	errs := func(error) {
		// no-op
	}
	processRecvMessage := func(*pool.Message, *client.Conn, config.HandlerFunc[*client.Conn]) {
		// no-op
	}
	inactivityMonitor := func(*client.Conn) {
		// no-op
	}
	periodicRunner := periodic.New(ctx.Done(), time.Millisecond*10)
	onNewConn := func(*client.Conn) {
		// no-op
	}
	requestMonitor := func(*client.Conn, *pool.Message) (bool, error) {
		return false, nil
	}
	mp := pool.New(1024, 1600)
	getToken := func() (message.Token, error) {
		return nil, nil
	}
	opts := []server.Option{
		options.WithHandlerFunc(handler),
		options.WithContext(ctx),
		options.WithMaxMessageSize(1024),
		options.WithErrors(errs),
		options.WithProcessReceivedMessageFunc(processRecvMessage),
		options.WithInactivityMonitor(time.Minute, inactivityMonitor),
		options.WithPeriodicRunner(periodicRunner),
		options.WithBlockwise(true, blockwise.SZX16, time.Second),
		options.WithOnNewConn(onNewConn),
		options.WithRequestMonitor(requestMonitor),
		options.WithMessagePool(mp),
		options.WithGetToken(getToken),
		options.WithLimitClientParallelRequest(42),
		options.WithLimitClientEndpointParallelRequest(43),
		options.WithReceivedMessageQueueSize(10),
	}

	for _, o := range opts {
		o.TCPServerApply(&cfg)
	}
	// WithHandlerFunc
	require.NotNil(t, cfg.Handler)
	// WithContext
	require.Equal(t, ctx, cfg.Ctx)
	// WithMaxMessageSize
	require.Equal(t, uint32(1024), cfg.MaxMessageSize)
	// WithErrors
	require.NotNil(t, cfg.Errors)
	// WithProcessReceivedMessageFunc
	require.NotNil(t, cfg.ProcessReceivedMessage)
	// WithInactivityMonitor
	require.NotNil(t, cfg.CreateInactivityMonitor)
	// WithPeriodicRunner
	require.NotNil(t, cfg.PeriodicRunner)
	// WithBlockwise
	require.True(t, cfg.BlockwiseEnable)
	require.Equal(t, blockwise.SZX16, cfg.BlockwiseSZX)
	require.Equal(t, time.Second, cfg.BlockwiseTransferTimeout)
	// WithOnNewConn
	require.NotNil(t, cfg.OnNewConn)
	// WithRequestMonitor
	require.NotNil(t, cfg.RequestMonitor)
	// WithMessagePool
	require.Equal(t, mp, cfg.MessagePool)
	// WithGetToken
	require.NotNil(t, cfg.GetToken)
	// WithLimitClientParallelRequest
	require.Equal(t, int64(42), cfg.LimitClientParallelRequests)
	// WithLimitClientEndpointParallelRequest
	require.Equal(t, int64(43), cfg.LimitClientEndpointParallelRequests)
	// WithReceivedMessageQueueSize
	require.Equal(t, 10, cfg.ReceivedMessageQueueSize)

	m := mux.NewRouter()
	keepAlive := func(*client.Conn) {
		// no-op
	}
	cfg = server.Config{}
	opts = []server.Option{
		options.WithMux(m),
		options.WithKeepAlive(16, time.Second, keepAlive),
	}
	for _, o := range opts {
		o.TCPServerApply(&cfg)
	}
	// WithMux
	require.NotNil(t, cfg.Handler)
	// WithKeepAlive
	require.NotNil(t, cfg.CreateInactivityMonitor)
}

func TestCommonTCPClientApply(t *testing.T) {
	cfg := client.Config{}
	handler := func(*responsewriter.ResponseWriter[*client.Conn], *pool.Message) {
		// no-op
	}
	ctx := context.Background()
	errs := func(error) {
		// no-op
	}
	processRecvMessage := func(*pool.Message, *client.Conn, config.HandlerFunc[*client.Conn]) {
		// no-op
	}
	inactivityMonitor := func(*client.Conn) {
		// no-op
	}
	network := "tcp"
	periodicRunner := periodic.New(ctx.Done(), time.Millisecond*10)
	dialer := &net.Dialer{Timeout: time.Second * 3}
	mp := pool.New(1024, 1600)
	getToken := func() (message.Token, error) {
		return nil, nil
	}
	opts := []tcp.Option{
		options.WithHandlerFunc(handler),
		options.WithContext(ctx),
		options.WithMaxMessageSize(1024),
		options.WithErrors(errs),
		options.WithProcessReceivedMessageFunc(processRecvMessage),
		options.WithInactivityMonitor(time.Minute, inactivityMonitor),
		options.WithNetwork(network),
		options.WithPeriodicRunner(periodicRunner),
		options.WithBlockwise(true, blockwise.SZX16, time.Second),
		options.WithCloseSocket(),
		options.WithDialer(dialer),
		options.WithMessagePool(mp),
		options.WithGetToken(getToken),
		options.WithLimitClientParallelRequest(42),
		options.WithLimitClientEndpointParallelRequest(43),
		options.WithReceivedMessageQueueSize(10),
	}

	for _, o := range opts {
		o.TCPClientApply(&cfg)
	}
	// WithHandlerFunc
	require.NotNil(t, cfg.Handler)
	// WithContext
	require.Equal(t, ctx, cfg.Ctx)
	// WithMaxMessageSize
	require.Equal(t, uint32(1024), cfg.MaxMessageSize)
	// WithErrors
	require.NotNil(t, cfg.Errors)
	// WithProcessReceivedMessageFunc
	require.NotNil(t, cfg.ProcessReceivedMessage)
	// WithInactivityMonitor
	require.NotNil(t, cfg.CreateInactivityMonitor)
	// WithNetwork
	require.Equal(t, network, cfg.Net)
	// WithPeriodicRunner
	require.NotNil(t, cfg.PeriodicRunner)
	// WithBlockwise
	require.True(t, cfg.BlockwiseEnable)
	require.Equal(t, blockwise.SZX16, cfg.BlockwiseSZX)
	require.Equal(t, time.Second, cfg.BlockwiseTransferTimeout)
	// WithCloseSocket
	require.True(t, cfg.CloseSocket)
	// WithDialer
	require.Equal(t, dialer, cfg.Dialer)
	// WithMessagePool
	require.Equal(t, mp, cfg.MessagePool)
	// WithGetToken
	require.NotNil(t, cfg.GetToken)
	// WithLimitClientParallelRequest
	require.Equal(t, int64(42), cfg.LimitClientParallelRequests)
	// WithLimitClientEndpointParallelRequest
	require.Equal(t, int64(43), cfg.LimitClientEndpointParallelRequests)
	// WithReceivedMessageQueueSize
	require.Equal(t, 10, cfg.ReceivedMessageQueueSize)

	m := mux.NewRouter()
	keepAlive := func(*client.Conn) {
		// no-op
	}
	cfg = client.Config{}
	opts = []tcp.Option{
		options.WithMux(m),
		options.WithKeepAlive(16, time.Second, keepAlive),
	}
	for _, o := range opts {
		o.TCPClientApply(&cfg)
	}
	// WithMux
	require.NotNil(t, cfg.Handler)
	// WithKeepAlive
	require.NotNil(t, cfg.CreateInactivityMonitor)
}

func TestCommonUDPServerApply(t *testing.T) {
	cfg := udpServer.Config{}
	handler := func(*responsewriter.ResponseWriter[*udpClient.Conn], *pool.Message) {
		// no-op
	}
	ctx := context.Background()
	errs := func(error) {
		// no-op
	}
	processRecvMessage := func(*pool.Message, *udpClient.Conn, config.HandlerFunc[*udpClient.Conn]) {
		// no-op
	}
	inactivityMonitor := func(*udpClient.Conn) {
		// no-op
	}
	periodicRunner := periodic.New(ctx.Done(), time.Millisecond*10)
	onNewConn := func(*udpClient.Conn) {
		// no-op
	}
	requestMonitor := func(*udpClient.Conn, *pool.Message) (bool, error) {
		return false, nil
	}
	mp := pool.New(1024, 1600)
	getToken := func() (message.Token, error) {
		return nil, nil
	}
	opts := []udpServer.Option{
		options.WithHandlerFunc(handler),
		options.WithContext(ctx),
		options.WithMaxMessageSize(1024),
		options.WithErrors(errs),
		options.WithProcessReceivedMessageFunc(processRecvMessage),
		options.WithInactivityMonitor(time.Minute, inactivityMonitor),
		options.WithPeriodicRunner(periodicRunner),
		options.WithBlockwise(true, blockwise.SZX16, time.Second),
		options.WithOnNewConn(onNewConn),
		options.WithRequestMonitor(requestMonitor),
		options.WithMessagePool(mp),
		options.WithGetToken(getToken),
		options.WithLimitClientParallelRequest(42),
		options.WithLimitClientEndpointParallelRequest(43),
		options.WithReceivedMessageQueueSize(10),
	}

	for _, o := range opts {
		o.UDPServerApply(&cfg)
	}
	// WithHandlerFunc
	require.NotNil(t, cfg.Handler)
	// WithContext
	require.Equal(t, ctx, cfg.Ctx)
	// WithMaxMessageSize
	require.Equal(t, uint32(1024), cfg.MaxMessageSize)
	// WithErrors
	require.NotNil(t, cfg.Errors)
	// WithProcessReceivedMessageFunc
	require.NotNil(t, cfg.ProcessReceivedMessage)
	// WithInactivityMonitor
	require.NotNil(t, cfg.CreateInactivityMonitor)
	// WithPeriodicRunner
	require.NotNil(t, cfg.PeriodicRunner)
	// WithBlockwise
	require.True(t, cfg.BlockwiseEnable)
	require.Equal(t, blockwise.SZX16, cfg.BlockwiseSZX)
	require.Equal(t, time.Second, cfg.BlockwiseTransferTimeout)
	// WithOnNewConn
	require.NotNil(t, cfg.OnNewConn)
	// WithRequestMonitor
	require.NotNil(t, cfg.RequestMonitor)
	// WithMessagePool
	require.Equal(t, mp, cfg.MessagePool)
	// WithGetToken
	require.NotNil(t, cfg.GetToken)
	// WithLimitClientParallelRequest
	require.Equal(t, int64(42), cfg.LimitClientParallelRequests)
	// WithLimitClientEndpointParallelRequest
	require.Equal(t, int64(43), cfg.LimitClientEndpointParallelRequests)
	// WithReceivedMessageQueueSize
	require.Equal(t, 10, cfg.ReceivedMessageQueueSize)

	m := mux.NewRouter()
	keepAlive := func(*udpClient.Conn) {
		// no-op
	}
	cfg = udpServer.Config{}
	opts = []udpServer.Option{
		options.WithMux(m),
		options.WithKeepAlive(16, time.Second, keepAlive),
	}
	for _, o := range opts {
		o.UDPServerApply(&cfg)
	}
	// WithMux
	require.NotNil(t, cfg.Handler)
	// WithKeepAlive
	require.NotNil(t, cfg.CreateInactivityMonitor)
}

func TestCommonDTLSServerApply(t *testing.T) {
	cfg := dtlsServer.Config{}
	handler := func(*responsewriter.ResponseWriter[*udpClient.Conn], *pool.Message) {
		// no-op
	}
	ctx := context.Background()
	errs := func(error) {
		// no-op
	}
	processRecvMessage := func(*pool.Message, *udpClient.Conn, config.HandlerFunc[*udpClient.Conn]) {
		// no-op
	}
	inactivityMonitor := func(*udpClient.Conn) {
		// no-op
	}
	periodicRunner := periodic.New(ctx.Done(), time.Millisecond*10)
	onNewConn := func(*udpClient.Conn) {
		// no-op
	}
	requestMonitor := func(*udpClient.Conn, *pool.Message) (bool, error) {
		return false, nil
	}
	mp := pool.New(1024, 1600)
	getToken := func() (message.Token, error) {
		return nil, nil
	}
	opts := []dtlsServer.Option{
		options.WithHandlerFunc(handler),
		options.WithContext(ctx),
		options.WithMaxMessageSize(1024),
		options.WithErrors(errs),
		options.WithProcessReceivedMessageFunc(processRecvMessage),
		options.WithInactivityMonitor(time.Minute, inactivityMonitor),
		options.WithPeriodicRunner(periodicRunner),
		options.WithBlockwise(true, blockwise.SZX16, time.Second),
		options.WithOnNewConn(onNewConn),
		options.WithRequestMonitor(requestMonitor),
		options.WithMessagePool(mp),
		options.WithGetToken(getToken),
		options.WithLimitClientParallelRequest(42),
		options.WithLimitClientEndpointParallelRequest(43),
		options.WithReceivedMessageQueueSize(10),
	}

	for _, o := range opts {
		o.DTLSServerApply(&cfg)
	}
	// WithHandlerFunc
	require.NotNil(t, cfg.Handler)
	// WithContext
	require.Equal(t, ctx, cfg.Ctx)
	// WithMaxMessageSize
	require.Equal(t, uint32(1024), cfg.MaxMessageSize)
	// WithErrors
	require.NotNil(t, cfg.Errors)
	// WithProcessReceivedMessageFunc
	require.NotNil(t, cfg.ProcessReceivedMessage)
	// WithInactivityMonitor
	require.NotNil(t, cfg.CreateInactivityMonitor)
	// WithPeriodicRunner
	require.NotNil(t, cfg.PeriodicRunner)
	// WithBlockwise
	require.True(t, cfg.BlockwiseEnable)
	require.Equal(t, blockwise.SZX16, cfg.BlockwiseSZX)
	require.Equal(t, time.Second, cfg.BlockwiseTransferTimeout)
	// WithOnNewConn
	require.NotNil(t, cfg.OnNewConn)
	// WithRequestMonitor
	require.NotNil(t, cfg.RequestMonitor)
	// WithMessagePool
	require.Equal(t, mp, cfg.MessagePool)
	// WithGetToken
	require.NotNil(t, cfg.GetToken)
	// WithLimitClientParallelRequest
	require.Equal(t, int64(42), cfg.LimitClientParallelRequests)
	// WithLimitClientEndpointParallelRequest
	require.Equal(t, int64(43), cfg.LimitClientEndpointParallelRequests)
	// WithReceivedMessageQueueSize
	require.Equal(t, 10, cfg.ReceivedMessageQueueSize)

	m := mux.NewRouter()
	keepAlive := func(*udpClient.Conn) {
		// no-op
	}
	cfg = dtlsServer.Config{}
	opts = []dtlsServer.Option{
		options.WithMux(m),
		options.WithKeepAlive(16, time.Second, keepAlive),
	}
	for _, o := range opts {
		o.DTLSServerApply(&cfg)
	}
	// WithMux
	require.NotNil(t, cfg.Handler)
	// WithKeepAlive
	require.NotNil(t, cfg.CreateInactivityMonitor)
}

func TestCommonUDPClientApply(t *testing.T) {
	cfg := udpClient.Config{}
	handler := func(*responsewriter.ResponseWriter[*udpClient.Conn], *pool.Message) {
		// no-op
	}
	ctx := context.Background()
	errs := func(error) {
		// no-op
	}
	processRecvMessage := func(*pool.Message, *udpClient.Conn, config.HandlerFunc[*udpClient.Conn]) {
		// no-op
	}
	inactivityMonitor := func(*udpClient.Conn) {
		// no-op
	}
	network := "udp4"
	periodicRunner := periodic.New(ctx.Done(), time.Millisecond*10)
	dialer := &net.Dialer{Timeout: time.Second * 3}

	mp := pool.New(1024, 1600)
	getToken := func() (message.Token, error) {
		return nil, nil
	}
	opts := []udp.Option{
		options.WithHandlerFunc(handler),
		options.WithContext(ctx),
		options.WithMaxMessageSize(1024),
		options.WithErrors(errs),
		options.WithProcessReceivedMessageFunc(processRecvMessage),
		options.WithInactivityMonitor(time.Minute, inactivityMonitor),
		options.WithNetwork(network),
		options.WithPeriodicRunner(periodicRunner),
		options.WithBlockwise(true, blockwise.SZX16, time.Second),
		options.WithCloseSocket(),
		options.WithDialer(dialer),
		options.WithMessagePool(mp),
		options.WithGetToken(getToken),
		options.WithLimitClientParallelRequest(42),
		options.WithLimitClientEndpointParallelRequest(43),
		options.WithReceivedMessageQueueSize(10),
	}

	for _, o := range opts {
		o.UDPClientApply(&cfg)
	}
	// WithHandlerFunc
	require.NotNil(t, cfg.Handler)
	// WithContext
	require.Equal(t, ctx, cfg.Ctx)
	// WithMaxMessageSize
	require.Equal(t, uint32(1024), cfg.MaxMessageSize)
	// WithErrors
	require.NotNil(t, cfg.Errors)
	// WithProcessReceivedMessageFunc
	require.NotNil(t, cfg.ProcessReceivedMessage)
	// WithInactivityMonitor
	require.NotNil(t, cfg.CreateInactivityMonitor)
	// WithNetwork
	require.Equal(t, network, cfg.Net)
	// WithPeriodicRunner
	require.NotNil(t, cfg.PeriodicRunner)
	// WithBlockwise
	require.True(t, cfg.BlockwiseEnable)
	require.Equal(t, blockwise.SZX16, cfg.BlockwiseSZX)
	require.Equal(t, time.Second, cfg.BlockwiseTransferTimeout)
	// WithCloseSocket
	require.True(t, cfg.CloseSocket)
	// WithDialer
	require.Equal(t, dialer, cfg.Dialer)
	// WithMessagePool
	require.Equal(t, mp, cfg.MessagePool)
	// WithGetToken
	require.NotNil(t, cfg.GetToken)
	// WithLimitClientParallelRequest
	require.Equal(t, int64(42), cfg.LimitClientParallelRequests)
	// WithLimitClientEndpointParallelRequest
	require.Equal(t, int64(43), cfg.LimitClientEndpointParallelRequests)
	// WithReceivedMessageQueueSize
	require.Equal(t, 10, cfg.ReceivedMessageQueueSize)

	m := mux.NewRouter()
	keepAlive := func(*udpClient.Conn) {
		// no-op
	}
	cfg = udpClient.Config{}
	opts = []udp.Option{
		options.WithMux(m),
		options.WithKeepAlive(16, time.Second, keepAlive),
	}
	for _, o := range opts {
		o.UDPClientApply(&cfg)
	}
	// WithMux
	require.NotNil(t, cfg.Handler)
	// WithKeepAlive
	require.NotNil(t, cfg.CreateInactivityMonitor)
}
