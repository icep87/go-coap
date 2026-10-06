package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/message/pool"
	"github.com/plgd-dev/go-coap/v3/net/blockwise"
	"github.com/plgd-dev/go-coap/v3/net/qblock"
	"github.com/plgd-dev/go-coap/v3/net/responsewriter"
	"github.com/stretchr/testify/require"
)

type serverHarness struct {
	cc      *Conn
	session *qblockTestSession
	now     time.Time
	nextMID int32
}

type serverSnapshot struct {
	active        uint32
	managerTokens int
	managerBytes  uint64
	records       int
	serverTokens  int
	mids          int
	reservations  int
	metadataBytes uint64
}

func newServerHarness(t *testing.T, mc qblock.ManagerConfig, sc qblockServerConfig, handler HandlerFunc) *serverHarness {
	t.Helper()
	h := &serverHarness{now: time.Unix(100, 0), nextMID: 1}
	h.session = &qblockTestSession{ctx: context.Background()}
	cfg := DefaultConfig
	cfg.BlockwiseEnable = false
	cfg.BlockwiseSZX = blockwise.SZX16
	cfg.Handler = handler
	cfg.GetMID = func() int32 {
		mid := h.nextMID
		h.nextMID++
		return mid
	}
	h.cc = NewConnWithOpts(h.session, &cfg,
		withQBlockClient(qblockClientConfig{Manager: mc, Now: func() time.Time { return h.now }, ScheduleMode: qblockScheduleManual}),
		withQBlockServer(sc),
	)
	t.Cleanup(h.session.closeForTest)
	return h
}

func (h *serverHarness) q1(t *testing.T, token byte, number uint32, more bool, size uint32, body string) *pool.Message {
	t.Helper()
	msg := h.cc.AcquireMessage(context.Background())
	msg.SetType(message.NonConfirmable)
	msg.SetCode(codes.POST)
	msg.SetToken(message.Token{token})
	msg.SetMessageID(h.nextMID)
	h.nextMID++
	require.NoError(t, msg.SetPath("/upload"))
	msg.SetContentFormat(message.TextPlain)
	msg.AddOptionBytes(message.RequestTag, []byte("tag-a"))
	msg.SetOptionUint32(message.Size1, size)
	value, err := qblock.EncodeBlock(qblock.Block{Number: number, More: more, SZX: blockwise.SZX16})
	require.NoError(t, err)
	msg.SetOptionUint32(message.QBlock1, value)
	msg.SetBody(bytes.NewReader([]byte(body)))
	return msg
}

func (h *serverHarness) control(t *testing.T, token byte, number uint32, more bool, tag string) *pool.Message {
	t.Helper()
	msg := h.cc.AcquireMessage(context.Background())
	msg.SetType(message.NonConfirmable)
	msg.SetCode(codes.POST)
	msg.SetToken(message.Token{token})
	msg.SetMessageID(h.nextMID)
	h.nextMID++
	require.NoError(t, msg.SetPath("/upload"))
	msg.SetContentFormat(message.TextPlain)
	msg.AddOptionBytes(message.RequestTag, []byte(tag))
	value, err := qblock.EncodeBlock(qblock.Block{Number: number, More: more, SZX: blockwise.SZX16})
	require.NoError(t, err)
	msg.SetOptionUint32(message.QBlock2, value)
	return msg
}

func (h *serverHarness) ingest(msg *pool.Message) {
	h.cc.ProcessReceivedMessageWithHandler(msg, h.cc.handleReq)
}

func (h *serverHarness) snapshot() serverSnapshot {
	client := h.cc.qblockClient
	client.mu.Lock()
	defer client.mu.Unlock()
	snapshot := serverSnapshot{active: client.manager.Active()}
	manager := reflect.ValueOf(client.manager).Elem()
	snapshot.managerTokens = manager.FieldByName("byToken").Len()
	snapshot.managerBytes = manager.FieldByName("retained").Uint()
	if client.server != nil {
		snapshot.records = len(client.server.records)
		snapshot.metadataBytes = client.server.metadata
		snapshot.mids = len(client.server.byMID)
		for _, record := range client.server.records {
			snapshot.serverTokens += len(record.tokens)
		}
	}
	h.cc.tokenReservations.Range(func(_ uint64, _ tokenReservation) bool {
		snapshot.reservations++
		return true
	})
	return snapshot
}

func (h *serverHarness) serverTokenBound(token byte) bool {
	client := h.cc.qblockClient
	client.mu.Lock()
	defer client.mu.Unlock()
	for _, record := range client.server.records {
		if _, ok := record.tokens[string(message.Token{token})]; ok {
			return true
		}
	}
	return false
}

func (h *serverHarness) advance(duration time.Duration) {
	h.now = h.now.Add(duration)
	h.cc.CheckExpirations(h.now)
}

func TestQBlockServerAdmission(t *testing.T) {
	h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, nil)
	h.ingest(h.q1(t, 1, 0, true, 32, "abcdefghijklmnop"))

	snapshot := h.snapshot()
	require.Equal(t, uint32(1), snapshot.active)
	require.Equal(t, 1, snapshot.records)
	require.Empty(t, h.session.writesSnapshot())
}

func TestQBlockServerRejectsMismatchedOutputOperation(t *testing.T) {
	h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, nil)
	h.ingest(h.q1(t, 1, 0, true, 32, "abcdefghijklmnop"))
	h.cc.qblockClient.mu.Lock()
	var id qblock.TransferID
	for _, record := range h.cc.qblockClient.server.records {
		id = record.id
	}
	h.cc.qblockClient.mu.Unlock()
	require.NotZero(t, id)

	h.cc.qblockClient.lockAction()
	callbacks := h.cc.qblockClient.executeServerOutput(qblock.Output{
		TransferID: id,
		Operation:  qblock.OperationKey("different-operation"),
		Action:     qblock.Action{Kind: qblock.SendContinue, Through: 0},
	})
	h.cc.qblockClient.actionMu.Unlock()
	require.Empty(t, callbacks)
	require.Empty(t, h.session.writesSnapshot(), "an output for another operation must not use this record")
}

func TestQBlockServerQ1AdmissionWaitsForOutputGate(t *testing.T) {
	h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, nil)
	contended := make(chan struct{}, 1)
	h.cc.qblockClient.actionMuContention = func() { contended <- struct{}{} }
	h.cc.qblockClient.actionMu.Lock()
	locked := true
	defer func() {
		if locked {
			h.cc.qblockClient.actionMu.Unlock()
		}
	}()
	done := make(chan struct{})
	first := h.q1(t, 1, 0, true, 32, "abcdefghijklmnop")
	go func() {
		h.ingest(first)
		close(done)
	}()
	requireQBlockContention(t, contended, "server Q1 admission")
	snapshot := h.snapshot()
	require.Zero(t, snapshot.active)
	require.Zero(t, snapshot.records)
	require.Zero(t, snapshot.reservations)
	h.cc.qblockClient.actionMu.Unlock()
	locked = false
	requireQBlockCompletion(t, done, "server Q1 admission")
	require.Equal(t, uint32(1), h.snapshot().active)
}

func TestQBlockServerDispatchAndHandoff(t *testing.T) {
	calls := 0
	mc := qblock.DefaultManagerConfig()
	mc.MaxTransfers, mc.MaxTokens = 1, 1
	h := newServerHarness(t, mc, qblockServerConfig{}, func(w *responsewriter.ResponseWriter[*Conn], r *pool.Message) {
		calls++
		body, err := io.ReadAll(r.Body())
		require.NoError(t, err)
		require.Equal(t, "body", string(body))
		require.NoError(t, w.SetResponse(codes.Changed, message.TextPlain, bytes.NewReader([]byte("response"))))
	})
	h.ingest(h.q1(t, 1, 0, false, 4, "body"))

	require.Equal(t, 1, calls)
	writes := h.session.writesSnapshot()
	require.Len(t, writes, 1)
	require.Equal(t, message.Token{1}, writes[0].token)
	require.True(t, writes[0].options.HasOption(message.QBlock2))
	require.Equal(t, uint32(1), h.snapshot().active)
	h.ingest(h.q1(t, 2, 0, false, 4, "body"))
	require.Equal(t, 1, calls)
}

func TestQBlockServerQ1DuplicateReplaysResponse(t *testing.T) {
	mc := qblock.DefaultManagerConfig()
	mc.Transfer.MaxPayloads = 1
	var calls atomic.Int32
	h := newServerHarness(t, mc, qblockServerConfig{}, func(w *responsewriter.ResponseWriter[*Conn], _ *pool.Message) {
		calls.Add(1)
		require.NoError(t, w.SetResponse(codes.Changed, message.TextPlain, bytes.NewReader(bytes.Repeat([]byte{'r'}, 16))))
	})

	h.ingest(h.q1(t, 1, 0, true, 32, "abcdefghijklmnop"))
	require.Len(t, h.session.writesSnapshot(), 1)
	firstContinue := h.session.writesSnapshot()[0]
	require.Equal(t, codes.Continue, firstContinue.code)
	require.Equal(t, message.Token{1}, firstContinue.token)

	h.ingest(h.q1(t, 1, 0, true, 32, "XXXXXXXXXXXXXXXX"))
	h.ingest(h.q1(t, 2, 0, true, 32, "YYYYYYYYYYYYYYYY"))
	writes := h.session.writesSnapshot()
	require.Len(t, writes, 3, "each duplicate of the stored set boundary must replay Continue")
	for i, token := range []message.Token{{1}, {2}} {
		write := writes[i+1]
		require.Equal(t, codes.Continue, write.code)
		require.Equal(t, token, write.token)
		require.True(t, write.options.HasOption(message.QBlock1))
		require.NotEqual(t, firstContinue.mid, write.mid)
	}
	require.Zero(t, calls.Load(), "a partial-body duplicate must not invoke the handler")
	require.LessOrEqual(t, h.snapshot().metadataBytes, uint64(64<<10))

	h.ingest(h.q1(t, 1, 1, false, 32, "qrstuvwxyzABCDEF"))
	require.EqualValues(t, 1, calls.Load())
	writes = h.session.writesSnapshot()
	require.Len(t, writes, 4)
	require.True(t, writes[3].options.HasOption(message.QBlock2))
	require.Equal(t, message.Token{1}, writes[3].token)

	h.ingest(h.q1(t, 1, 1, false, 32, "1111111111111111"))
	h.ingest(h.q1(t, 3, 1, false, 32, "2222222222222222"))
	h.advance(mc.Transfer.NonTimeout) // a second repair joins the sender's paced repair set
	writes = h.session.writesSnapshot()
	require.Len(t, writes, 6, "a completed upload duplicate must replay the retained Q2 response")
	for i, token := range []message.Token{{1}, {3}} {
		write := writes[i+4]
		require.True(t, write.options.HasOption(message.QBlock2))
		require.Equal(t, uint32(0), write.block)
		require.Equal(t, token, write.token)
		require.Equal(t, bytes.Repeat([]byte{'r'}, 16), write.payload)
	}
	require.EqualValues(t, 1, calls.Load())
	require.LessOrEqual(t, h.snapshot().serverTokens, int(mc.MaxTokens))

	h.advance(mc.Transfer.Lifetime)
	snapshot := h.snapshot()
	require.Zero(t, snapshot.active)
	require.Zero(t, snapshot.managerTokens)
	require.Zero(t, snapshot.managerBytes)
	require.Zero(t, snapshot.serverTokens)
	require.Zero(t, snapshot.reservations)
	require.LessOrEqual(t, snapshot.metadataBytes, uint64(64<<10))
}

func TestQBlockServerQ1DuplicateDuringHandlerReplaysOnce(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	var calls atomic.Int32
	h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, func(w *responsewriter.ResponseWriter[*Conn], _ *pool.Message) {
		calls.Add(1)
		close(started)
		<-release
		require.NoError(t, w.SetResponse(codes.Changed, message.TextPlain, bytes.NewReader([]byte("response"))))
	})
	first := h.q1(t, 4, 0, false, 4, "body")
	firstDone := make(chan struct{})
	go func() {
		h.ingest(first)
		close(firstDone)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("application handler did not start")
	}

	h.ingest(h.q1(t, 5, 0, false, 4, "diff"))
	require.EqualValues(t, 1, calls.Load())
	require.True(t, h.serverTokenBound(5), "the duplicate token must stay reserved until the response is ready")
	require.Empty(t, h.session.writesSnapshot(), "the Q2 response waits for the running handler")
	close(release)
	released = true
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("application handler did not settle")
	}

	writes := h.session.writesSnapshot()
	require.Len(t, writes, 2, "the eventual response must be delivered for both original and duplicate tokens")
	require.True(t, writes[0].options.HasOption(message.QBlock2))
	require.Equal(t, message.Token{4}, writes[0].token)
	require.True(t, writes[1].options.HasOption(message.QBlock2))
	require.Equal(t, message.Token{5}, writes[1].token)
	require.EqualValues(t, 1, calls.Load())
}

func TestQBlockServerQ1DuplicateReplyExpiryReleasesToken(t *testing.T) {
	mc := qblock.DefaultManagerConfig()
	mc.Transfer.MaxPayloads = 1
	h := newServerHarness(t, mc, qblockServerConfig{}, nil)
	h.ingest(h.q1(t, 6, 0, true, 32, "abcdefghijklmnop"))
	h.ingest(h.q1(t, 7, 0, true, 32, "XXXXXXXXXXXXXXXX"))
	require.True(t, h.serverTokenBound(7))

	h.advance(mc.Transfer.Lifetime)
	snapshot := h.snapshot()
	require.Zero(t, snapshot.active)
	require.Zero(t, snapshot.managerTokens)
	require.Zero(t, snapshot.managerBytes)
	require.Zero(t, snapshot.serverTokens)
	require.Zero(t, snapshot.reservations)
}

func TestQBlockServerQ1DuplicateWriteFailureReleasesState(t *testing.T) {
	var calls atomic.Int32
	h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, func(w *responsewriter.ResponseWriter[*Conn], _ *pool.Message) {
		calls.Add(1)
		require.NoError(t, w.SetResponse(codes.Changed, message.TextPlain, bytes.NewReader([]byte("response"))))
	})
	h.ingest(h.q1(t, 8, 0, false, 4, "body"))
	require.Len(t, h.session.writesSnapshot(), 1)
	h.session.writeErr = errors.New("duplicate response write failed")

	h.ingest(h.q1(t, 9, 0, false, 4, "diff"))
	snapshot := h.snapshot()
	require.EqualValues(t, 1, calls.Load())
	require.Len(t, h.session.writesSnapshot(), 2)
	require.Zero(t, snapshot.active)
	require.Zero(t, snapshot.managerTokens)
	require.Zero(t, snapshot.managerBytes)
	require.Zero(t, snapshot.serverTokens)
	require.Zero(t, snapshot.reservations)
}

func TestQBlockServerFailedQ2WriteStillSuppressesDuplicateUpload(t *testing.T) {
	calls := 0
	h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, func(w *responsewriter.ResponseWriter[*Conn], r *pool.Message) {
		calls++
		require.NoError(t, w.SetResponse(codes.Changed, message.TextPlain, bytes.NewReader([]byte("response"))))
	})
	h.session.writeErr = errors.New("write failed")
	h.ingest(h.q1(t, 1, 0, false, 4, "body"))
	require.Equal(t, 1, calls)

	h.ingest(h.q1(t, 2, 0, false, 4, "body"))
	h.ingest(h.q1(t, 3, 0, false, 4, "body"))

	require.Equal(t, 1, calls)
	require.Equal(t, uint32(0), h.snapshot().active)
	require.Equal(t, 1, h.snapshot().records)
}

func TestQBlockServerQ2BlocksUseStableRepresentationSize(t *testing.T) {
	body := bytes.Repeat([]byte{'r'}, 32)
	h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, func(w *responsewriter.ResponseWriter[*Conn], r *pool.Message) {
		require.NoError(t, w.SetResponse(codes.Changed, message.TextPlain, bytes.NewReader(body)))
	})
	h.ingest(h.q1(t, 1, 0, false, 4, "body"))

	writes := h.session.writesSnapshot()
	require.Len(t, writes, 2)
	for _, write := range writes {
		size, err := write.options.GetUint32(message.Size2)
		require.NoError(t, err)
		require.Equal(t, uint32(len(body)), size)
		etag, err := write.options.GetBytes(message.ETag)
		require.NoError(t, err)
		require.Len(t, etag, 8)
	}
}

func q2ServerResponseWrites(t *testing.T, code codes.Code, body []byte, options ...message.Option) []qblockTestWrite {
	t.Helper()
	calls := 0
	h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, func(w *responsewriter.ResponseWriter[*Conn], _ *pool.Message) {
		calls++
		require.NoError(t, w.SetResponse(code, message.TextPlain, bytes.NewReader(body), options...))
	})
	h.ingest(h.q1(t, 1, 0, false, 4, "body"))
	require.Equal(t, 1, calls)
	return h.session.writesSnapshot()
}

func responseOptionCount(options message.Options, id message.OptionID) int {
	count := 0
	for _, option := range options {
		if option.ID == id {
			count++
		}
	}
	return count
}

func requireCleanQ2ResponseOptions(t *testing.T, options message.Options) {
	t.Helper()
	require.Equal(t, 1, responseOptionCount(options, message.QBlock2))
	require.Equal(t, 1, responseOptionCount(options, message.Size2))
	require.Equal(t, 1, responseOptionCount(options, message.ETag))
	for _, id := range []message.OptionID{message.QBlock1, message.Block1, message.Block2, message.Size1, message.RequestTag} {
		require.Zero(t, responseOptionCount(options, id), "option %v must be removed from Q2 responses", id)
	}
}

func TestQBlockServerQ2ETagSelection(t *testing.T) {
	body := bytes.Repeat([]byte{'r'}, 48)
	for _, tc := range []struct {
		name    string
		options []message.Option
		valid   []byte
	}{
		{name: "valid application ETag", options: []message.Option{{ID: message.ETag, Value: []byte("version")}}, valid: []byte("version")},
		{name: "absent ETag"},
		{name: "empty ETag", options: []message.Option{{ID: message.ETag}}},
		{name: "oversized ETag", options: []message.Option{{ID: message.ETag, Value: bytes.Repeat([]byte{'x'}, 9)}}},
		{name: "duplicate ETag", options: []message.Option{{ID: message.ETag, Value: []byte("first")}, {ID: message.ETag, Value: []byte("second")}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			writes := q2ServerResponseWrites(t, codes.Changed, body, tc.options...)
			require.Len(t, writes, 3)
			var first []byte
			for _, write := range writes {
				etag, err := write.options.GetBytes(message.ETag)
				require.NoError(t, err)
				if tc.valid != nil {
					require.Equal(t, tc.valid, etag)
				} else {
					require.Len(t, etag, 8)
				}
				if first == nil {
					first = etag
				} else {
					require.Equal(t, first, etag)
				}
			}
		})
	}

	generated := func(code codes.Code, options ...message.Option) []byte {
		writes := q2ServerResponseWrites(t, code, body, options...)
		etag, err := writes[0].options.GetBytes(message.ETag)
		require.NoError(t, err)
		return etag
	}
	base := generated(codes.Changed, message.Option{ID: message.LocationPath, Value: []byte("west")})
	require.NotEqual(t, base, generated(codes.Content, message.Option{ID: message.LocationPath, Value: []byte("west")}))
	require.NotEqual(t, base, generated(codes.Changed, message.Option{ID: message.LocationPath, Value: []byte("east")}))

	ordered := []message.Option{{ID: message.MaxAge, Value: []byte{1}}, {ID: message.LocationPath, Value: []byte("west")}}
	reversed := []message.Option{ordered[1], ordered[0]}
	require.Equal(t, generated(codes.Changed, ordered...), generated(codes.Changed, reversed...))
	require.NotEqual(t,
		generated(codes.Changed, message.Option{ID: message.LocationPath, Value: []byte("first")}, message.Option{ID: message.LocationPath, Value: []byte("second")}),
		generated(codes.Changed, message.Option{ID: message.LocationPath, Value: []byte("second")}, message.Option{ID: message.LocationPath, Value: []byte("first")}),
	)
	require.NotEqual(t,
		generated(codes.Changed, message.Option{ID: message.LocationPath, Value: []byte("ab")}),
		generated(codes.Changed, message.Option{ID: message.LocationPath, Value: []byte("a")}, message.Option{ID: message.LocationPath, Value: []byte("b")}),
	)
}

func TestQBlockServerQ2ResponsesStripApplicationBlockOptions(t *testing.T) {
	applicationOptions := []message.Option{
		{ID: message.QBlock1, Value: []byte{0}},
		{ID: message.QBlock2, Value: []byte{0}},
		{ID: message.Block1, Value: []byte{0}},
		{ID: message.Block2, Value: []byte{0}},
		{ID: message.Size1, Value: []byte{32}},
		{ID: message.Size2, Value: []byte{32}},
		{ID: message.RequestTag, Value: []byte("stale")},
		{ID: message.ETag, Value: []byte("old-one")},
		{ID: message.ETag, Value: []byte("old-two")},
	}
	for _, write := range q2ServerResponseWrites(t, codes.Changed, bytes.Repeat([]byte{'r'}, 32), applicationOptions...) {
		requireCleanQ2ResponseOptions(t, write.options)
	}

	h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, func(w *responsewriter.ResponseWriter[*Conn], _ *pool.Message) {
		require.NoError(t, w.SetResponse(codes.Content, message.TextPlain, bytes.NewReader(bytes.Repeat([]byte{'r'}, 32)), applicationOptions...))
	})
	h.ingest(conGET(t, h, 70, 1, 0))
	writes := h.session.writesSnapshot()
	require.Len(t, writes, 1)
	require.Equal(t, codes.Content, writes[0].code)
	requireCleanQ2ResponseOptions(t, writes[0].options)
}

func TestQBlockServerTickSendsNextQ2SetWithRetainedToken(t *testing.T) {
	mc := qblock.DefaultManagerConfig()
	mc.Transfer.MaxPayloads = 1
	body := bytes.Repeat([]byte{'r'}, 32)
	h := newServerHarness(t, mc, qblockServerConfig{}, func(w *responsewriter.ResponseWriter[*Conn], r *pool.Message) {
		require.NoError(t, w.SetResponse(codes.Changed, message.TextPlain, bytes.NewReader(body)))
	})
	h.ingest(h.q1(t, 1, 0, false, 4, "body"))
	require.Len(t, h.session.writesSnapshot(), 1)

	h.advance(2 * time.Second)

	writes := h.session.writesSnapshot()
	require.Len(t, writes, 2)
	require.Equal(t, message.Token{1}, writes[1].token)
	value, err := writes[1].options.GetUint32(message.QBlock2)
	require.NoError(t, err)
	block, err := qblock.DecodeBlock(value)
	require.NoError(t, err)
	require.Equal(t, uint32(1), block.Number)
}

func TestQBlockServerUnmodifiedHandlerResponseSuppressesDuplicate(t *testing.T) {
	calls := 0
	h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, func(*responsewriter.ResponseWriter[*Conn], *pool.Message) {
		calls++
	})
	h.ingest(h.q1(t, 1, 0, false, 4, "body"))
	h.ingest(h.q1(t, 2, 0, false, 4, "body"))

	require.Equal(t, 1, calls)
	require.Empty(t, h.session.writesSnapshot())
	require.Zero(t, h.snapshot().active)
	require.Equal(t, 1, h.snapshot().records)
}

func TestQBlockServerControlRollback(t *testing.T) {
	mc := qblock.DefaultManagerConfig()
	mc.Transfer.MaxPayloads = 2
	h := newServerHarness(t, mc, qblockServerConfig{}, func(w *responsewriter.ResponseWriter[*Conn], r *pool.Message) {
		require.NoError(t, w.SetResponse(codes.Changed, message.TextPlain, bytes.NewReader(bytes.Repeat([]byte{'r'}, 48))))
	})
	h.ingest(h.q1(t, 1, 0, false, 4, "body"))
	before := h.snapshot()

	h.ingest(h.control(t, 9, 2, false, "tag-a"))
	require.Equal(t, before, h.snapshot())
	h.ingest(h.control(t, 10, 0, false, "wrong-tag"))
	require.Equal(t, before, h.snapshot())
	h.ingest(h.control(t, 11, 0, false, "tag-a"))

	writes := h.session.writesSnapshot()
	require.Equal(t, message.Token{11}, writes[len(writes)-1].token)
}

func TestQBlockServerRepeatedControlTokenQueuesRepair(t *testing.T) {
	mc := qblock.DefaultManagerConfig()
	mc.Transfer.MaxPayloads = 2
	h := newServerHarness(t, mc, qblockServerConfig{}, func(w *responsewriter.ResponseWriter[*Conn], r *pool.Message) {
		require.NoError(t, w.SetResponse(codes.Changed, message.TextPlain, bytes.NewReader(bytes.Repeat([]byte{'r'}, 48))))
	})
	h.ingest(h.q1(t, 1, 0, false, 4, "body"))
	h.advance(2 * time.Second) // send the final initial block before repairs
	h.ingest(h.control(t, 11, 0, false, "tag-a"))
	before := len(h.session.writesSnapshot())
	h.ingest(h.control(t, 11, 0, false, "tag-a"))

	h.advance(2 * time.Second)
	writes := h.session.writesSnapshot()
	require.Greater(t, len(writes), before)
	require.Equal(t, message.Token{11}, writes[len(writes)-1].token)
}

func TestQBlockServerRepairWaitsForOutputGateAndUsesAcceptedToken(t *testing.T) {
	mc := qblock.DefaultManagerConfig()
	mc.Transfer.MaxPayloads = 2
	h := newServerHarness(t, mc, qblockServerConfig{}, func(w *responsewriter.ResponseWriter[*Conn], r *pool.Message) {
		require.NoError(t, w.SetResponse(codes.Changed, message.TextPlain, bytes.NewReader(bytes.Repeat([]byte{'r'}, 48))))
	})
	h.ingest(h.q1(t, 1, 0, false, 4, "body"))
	h.advance(2 * time.Second) // finish the initial Q2 set; repairs are now valid

	first := h.control(t, 11, 0, false, "tag-a")
	second := h.control(t, 12, 1, false, "tag-a")
	contended := make(chan struct{}, 1)
	h.cc.qblockClient.actionMuContention = func() { contended <- struct{}{} }
	h.cc.qblockClient.actionMu.Lock()
	locked := true
	defer func() {
		if locked {
			h.cc.qblockClient.actionMu.Unlock()
		}
	}()
	firstDone := make(chan struct{})
	go func() {
		h.ingest(first)
		close(firstDone)
	}()
	requireQBlockContention(t, contended, "server Q2 repair")
	require.False(t, h.serverTokenBound(11))
	h.cc.qblockClient.actionMu.Unlock()
	locked = false
	requireQBlockCompletion(t, firstDone, "server Q2 repair")

	writes := h.session.writesSnapshot()
	require.Equal(t, message.Token{11}, writes[len(writes)-1].token)
	h.ingest(second)
	require.True(t, h.serverTokenBound(12))
	h.advance(2 * time.Second)
	writes = h.session.writesSnapshot()
	require.Equal(t, message.Token{12}, writes[len(writes)-1].token)
}

func TestQBlockServerControlWrongSZXRollsBack(t *testing.T) {
	h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, func(w *responsewriter.ResponseWriter[*Conn], r *pool.Message) {
		require.NoError(t, w.SetResponse(codes.Changed, message.TextPlain, bytes.NewReader(bytes.Repeat([]byte{'r'}, 32))))
	})
	h.ingest(h.q1(t, 1, 0, false, 4, "body"))
	before := h.snapshot()
	control := h.control(t, 9, 0, false, "tag-a")
	value, err := qblock.EncodeBlock(qblock.Block{Number: 0, SZX: blockwise.SZX32})
	require.NoError(t, err)
	control.SetOptionUint32(message.QBlock2, value)

	h.ingest(control)

	require.Equal(t, before, h.snapshot())
}

func TestQBlockServerControlRequiresFullRequestIdentity(t *testing.T) {
	// Each mutation must fail before a fresh control token becomes observable.
	// Removing any of these checks would route a repair by a partial identity.
	for name, mutate := range map[string]func(*pool.Message){
		"changed URI": func(msg *pool.Message) {
			require.NoError(t, msg.SetPath("/other"))
		},
		"changed query": func(msg *pool.Message) {
			msg.AddOptionString(message.URIQuery, "other=true")
		},
		"changed method": func(msg *pool.Message) {
			msg.SetCode(codes.PUT)
		},
		"multiple request tags": func(msg *pool.Message) {
			msg.AddOptionBytes(message.RequestTag, []byte("tag-b"))
		},
		"partial request tag": func(msg *pool.Message) {
			msg.Remove(message.RequestTag)
			msg.AddOptionBytes(message.RequestTag, []byte("tag"))
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, func(w *responsewriter.ResponseWriter[*Conn], r *pool.Message) {
				require.NoError(t, w.SetResponse(codes.Changed, message.TextPlain, bytes.NewReader(bytes.Repeat([]byte{'r'}, 32))))
			})
			h.ingest(h.q1(t, 1, 0, false, 4, "body"))
			before := h.snapshot()
			control := h.control(t, 9, 0, false, "tag-a")
			mutate(control)

			h.ingest(control)

			require.Equal(t, before, h.snapshot())
		})
	}
}

func TestQBlockServerControlDoesNotUseResponseETagAsIdentity(t *testing.T) {
	h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, func(w *responsewriter.ResponseWriter[*Conn], r *pool.Message) {
		require.NoError(t, w.SetResponse(codes.Changed, message.TextPlain, bytes.NewReader(bytes.Repeat([]byte{'r'}, 32))))
	})
	h.ingest(h.q1(t, 1, 0, false, 4, "body"))
	other := h.q1(t, 2, 0, false, 4, "body")
	require.NoError(t, other.SetPath("/other"))
	h.ingest(other)
	h.cc.qblockClient.mu.Lock()
	deadline, ready := h.cc.qblockClient.probeGate.nextDeadline()
	h.cc.qblockClient.mu.Unlock()
	require.True(t, ready)
	h.advance(deadline.Sub(h.now))

	writes := h.session.writesSnapshot()
	require.Len(t, writes, 4)
	firstETag, err := writes[0].options.GetBytes(message.ETag)
	require.NoError(t, err)
	secondETag, err := writes[2].options.GetBytes(message.ETag)
	require.NoError(t, err)
	require.Equal(t, firstETag, secondETag)

	control := h.control(t, 11, 0, false, "tag-a")
	require.NoError(t, control.SetPath("/other"))
	h.ingest(control)
	writes = h.session.writesSnapshot()
	require.Equal(t, message.Token{11}, writes[len(writes)-1].token)
}

func TestQBlockServerRejectedControlsDoNotConsumeTokens(t *testing.T) {
	t.Run("stale continue", func(t *testing.T) {
		h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, func(w *responsewriter.ResponseWriter[*Conn], r *pool.Message) {
			require.NoError(t, w.SetResponse(codes.Changed, message.TextPlain, bytes.NewReader(bytes.Repeat([]byte{'r'}, 48))))
		})
		h.ingest(h.q1(t, 1, 0, false, 4, "body"))
		before := h.snapshot()
		h.ingest(h.control(t, 9, 10, true, "tag-a"))
		require.Equal(t, before, h.snapshot())
	})
	t.Run("zero continue", func(t *testing.T) {
		h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, func(w *responsewriter.ResponseWriter[*Conn], r *pool.Message) {
			require.NoError(t, w.SetResponse(codes.Changed, message.TextPlain, bytes.NewReader(bytes.Repeat([]byte{'r'}, 32))))
		})
		h.ingest(h.q1(t, 1, 0, false, 4, "body"))
		before := h.snapshot()
		h.ingest(h.control(t, 9, 0, true, "tag-a"))
		require.Equal(t, before, h.snapshot())
	})
	t.Run("manager token limit", func(t *testing.T) {
		mc := qblock.DefaultManagerConfig()
		mc.MaxTransfers, mc.MaxTokens = 1, 1
		h := newServerHarness(t, mc, qblockServerConfig{}, func(w *responsewriter.ResponseWriter[*Conn], r *pool.Message) {
			require.NoError(t, w.SetResponse(codes.Changed, message.TextPlain, bytes.NewReader(bytes.Repeat([]byte{'r'}, 32))))
		})
		h.ingest(h.q1(t, 1, 0, false, 4, "body"))
		before := h.snapshot()
		h.ingest(h.control(t, 9, 0, false, "tag-a"))
		require.Equal(t, before, h.snapshot())
	})
	t.Run("other qblock record owns token", func(t *testing.T) {
		h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, func(w *responsewriter.ResponseWriter[*Conn], r *pool.Message) {
			require.NoError(t, w.SetResponse(codes.Changed, message.TextPlain, bytes.NewReader(bytes.Repeat([]byte{'r'}, 32))))
		})
		h.ingest(h.q1(t, 1, 0, false, 4, "body"))
		other := h.q1(t, 2, 0, false, 4, "body")
		require.NoError(t, other.SetPath("/other"))
		h.ingest(other)
		before := h.snapshot()
		h.ingest(h.control(t, 2, 0, false, "tag-a"))
		require.Equal(t, before, h.snapshot())
	})
}

func TestQBlockServerDelayedRepairUsesMostRecentAcceptedControlToken(t *testing.T) {
	mc := qblock.DefaultManagerConfig()
	mc.Transfer.MaxPayloads = 2
	h := newServerHarness(t, mc, qblockServerConfig{}, func(w *responsewriter.ResponseWriter[*Conn], r *pool.Message) {
		require.NoError(t, w.SetResponse(codes.Changed, message.TextPlain, bytes.NewReader(bytes.Repeat([]byte{'r'}, 48))))
	})
	h.ingest(h.q1(t, 1, 0, false, 4, "body"))
	h.advance(2 * time.Second)
	h.ingest(h.control(t, 11, 0, false, "tag-a"))
	h.ingest(h.control(t, 12, 1, false, "tag-a"))

	h.advance(2 * time.Second)
	writes := h.session.writesSnapshot()
	require.Equal(t, message.Token{12}, writes[len(writes)-1].token)
}

func TestQBlockServerRejectsMalformedFirstFragmentWithoutState(t *testing.T) {
	for name, mutate := range map[string]func(*pool.Message){
		"missing request tag": func(msg *pool.Message) { msg.Remove(message.RequestTag) },
		"missing size":        func(msg *pool.Message) { msg.Remove(message.Size1) },
		"mixed classic block": func(msg *pool.Message) { msg.SetOptionUint32(message.Block1, 0) },
		"short payload": func(msg *pool.Message) {
			msg.SetBody(bytes.NewReader([]byte("short")))
		},
		"empty token": func(msg *pool.Message) { msg.SetToken(nil) },
		"oversized identity": func(msg *pool.Message) {
			msg.AddOptionBytes(message.URIQuery, bytes.Repeat([]byte{'x'}, 512))
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, nil)
			before := h.snapshot()
			msg := h.q1(t, 1, 0, true, 32, "abcdefghijklmnop")
			mutate(msg)

			h.ingest(msg)

			require.Equal(t, before, h.snapshot())
			switch name {
			case "missing request tag", "missing size":
				writes := h.session.writesSnapshot()
				require.Len(t, writes, 1)
				require.Equal(t, codes.BadRequest, writes[0].code)
			case "mixed classic block":
				writes := h.session.writesSnapshot()
				require.Len(t, writes, 1)
				require.Equal(t, codes.BadOption, writes[0].code)
			default:
				require.Empty(t, h.session.writesSnapshot())
			}
		})
	}
}

func TestQBlockEnabledServerRejectsMixedOptions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		token   byte
		request func(*serverHarness, *testing.T) *pool.Message
		classic message.OptionID
	}{
		{
			name:  "QBlock1 and Block1",
			token: 1,
			request: func(h *serverHarness, t *testing.T) *pool.Message {
				return h.q1(t, 1, 0, false, 4, "body")
			},
			classic: message.Block1,
		},
		{
			name:  "QBlock2 and Block2",
			token: 2,
			request: func(h *serverHarness, t *testing.T) *pool.Message {
				return h.control(t, 2, 0, false, "tag-a")
			},
			classic: message.Block2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, func(*responsewriter.ResponseWriter[*Conn], *pool.Message) {
				calls++
			})
			before := h.snapshot()
			request := tc.request(h, t)
			request.SetOptionUint32(tc.classic, 0)
			requestMID := request.MessageID()

			h.ingest(request)

			writes := h.session.writesSnapshot()
			require.Len(t, writes, 1)
			first := writes[0]
			require.Equal(t, codes.BadOption, first.code)
			require.Equal(t, message.NonConfirmable, first.typ)
			require.NotEqual(t, requestMID, first.mid)
			require.Equal(t, message.Token{tc.token}, first.token)
			require.Zero(t, calls)
			require.Equal(t, before, h.snapshot())

			duplicate := tc.request(h, t)
			duplicate.SetOptionUint32(tc.classic, 0)
			duplicate.SetMessageID(requestMID)
			h.ingest(duplicate)

			require.Equal(t, []qblockTestWrite{first, first}, h.session.writesSnapshot())
			require.Zero(t, calls)
			require.Equal(t, before, h.snapshot())
		})
	}
}

func TestQBlockServerRetainsDeliveredQ1ForLaterDispatch(t *testing.T) {
	h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, nil)
	h.ingest(h.q1(t, 1, 0, false, 4, "body"))

	h.cc.qblockClient.mu.Lock()
	require.Len(t, h.cc.qblockClient.server.records, 1)
	for _, record := range h.cc.qblockClient.server.records {
		require.True(t, record.ready)
		require.Equal(t, []byte("body"), record.payload)
	}
	h.cc.qblockClient.mu.Unlock()
	require.Equal(t, uint32(1), h.snapshot().active)
	require.Empty(t, h.session.writesSnapshot())
}

func TestQBlockServerSendsContinueAtQ1SetBoundary(t *testing.T) {
	mc := qblock.DefaultManagerConfig()
	mc.Transfer.MaxPayloads = 2
	h := newServerHarness(t, mc, qblockServerConfig{}, nil)
	h.ingest(h.q1(t, 1, 0, true, 48, "abcdefghijklmnop"))
	h.ingest(h.q1(t, 2, 1, true, 48, "qrstuvwxyzabcdef"))

	writes := h.session.writesSnapshot()
	require.Len(t, writes, 1)
	require.Equal(t, codes.Continue, writes[0].code)
	require.Equal(t, message.Token{2}, writes[0].token)
	value, err := writes[0].options.GetUint32(message.QBlock1)
	require.NoError(t, err)
	block, err := qblock.DecodeBlock(value)
	require.NoError(t, err)
	require.Equal(t, uint32(1), block.Number)
	require.True(t, block.More)
}

func TestQBlockServerRequestsMissingBlocksOnTick(t *testing.T) {
	h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, nil)
	h.ingest(h.q1(t, 1, 1, false, 32, "qrstuvwxyzabcdef"))
	h.advance(qblock.DefaultTransferConfig().NonReceiveTimeout)

	writes := h.session.writesSnapshot()
	require.Len(t, writes, 1)
	require.Equal(t, codes.RequestEntityIncomplete, writes[0].code)
	require.Equal(t, message.Token{1}, writes[0].token)
	require.Equal(t, message.AppMissingBlocksCBORSeq, mustContentFormat(t, writes[0].options))
	missing, err := qblock.DecodeMissing(writes[0].payload, 2, 2)
	require.NoError(t, err)
	require.Equal(t, []uint32{0}, missing)
}

func TestQBlockServerAdmissionDoesNotStealOrdinaryToken(t *testing.T) {
	h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, nil)
	require.NoError(t, h.cc.claimToken(message.Token{1}, tokenOwnerRequest))
	defer h.cc.releaseToken(message.Token{1}, tokenOwnerRequest)
	before := h.snapshot()

	h.ingest(h.q1(t, 1, 0, true, 32, "abcdefghijklmnop"))

	require.Equal(t, before, h.snapshot())
}

func TestQBlockServerRecordLimitRefusesNewIdentity(t *testing.T) {
	h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{MaxRecords: 1}, nil)
	h.ingest(h.q1(t, 1, 0, true, 32, "abcdefghijklmnop"))
	before := h.snapshot()
	second := h.q1(t, 2, 0, true, 32, "abcdefghijklmnop")
	require.NoError(t, second.SetPath("/other"))

	h.ingest(second)

	require.Equal(t, before, h.snapshot())
}

func TestQBlockServerRecoveryWriteFailureReleasesAdmissionState(t *testing.T) {
	h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, nil)
	before := h.snapshot()
	h.session.writeErr = errors.New("write failed")
	h.ingest(h.q1(t, 1, 1, false, 32, "qrstuvwxyzabcdef"))

	h.advance(qblock.DefaultTransferConfig().NonReceiveTimeout)

	require.Equal(t, before, h.snapshot())
}

func TestQBlockServerCloseReleasesReadyReceiver(t *testing.T) {
	h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, nil)
	h.ingest(h.q1(t, 1, 0, false, 4, "body"))
	require.Equal(t, uint32(1), h.snapshot().active)

	h.session.closeForTest()

	require.Equal(t, serverSnapshot{}, h.snapshot())
}

func TestQBlockServerMetadataConflictCancelsOnlyResolvedReceiver(t *testing.T) {
	h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, nil)
	h.ingest(h.q1(t, 1, 0, true, 32, "abcdefghijklmnop"))
	conflict := h.q1(t, 2, 1, true, 48, "qrstuvwxyzabcdef")
	_, options, err := serverQ1Fragment(conflict)
	require.NoError(t, err)
	operation, err := serverRequestKey(codes.POST, options)
	require.NoError(t, err)
	_, resolved := h.cc.qblockClient.server.records[operation]
	require.True(t, resolved)
	unrelated := h.q1(t, 3, 0, true, 32, "ponmlkjihgfedcba")
	require.NoError(t, unrelated.SetPath("/other"))
	unrelatedOptions, err := canonicalServerRequestOptions(unrelated.Options())
	require.NoError(t, err)
	unrelatedOperation, err := serverRequestKey(codes.POST, unrelatedOptions)
	require.NoError(t, err)
	require.NotEqual(t, operation, unrelatedOperation)
	h.ingest(unrelated)
	require.Equal(t, uint32(2), h.snapshot().active)
	conflict.SetBody(bytes.NewReader([]byte("qrstuvwxyzabcdef")))

	h.ingest(conflict)

	require.Equal(t, uint32(1), h.snapshot().active)
	require.Equal(t, 1, h.snapshot().records)
	_, resolved = h.cc.qblockClient.server.records[operation]
	require.False(t, resolved)
	_, remains := h.cc.qblockClient.server.records[unrelatedOperation]
	require.True(t, remains)
}

func TestQBlockServerSameTagDifferentRequestIdentityAdmitsBoth(t *testing.T) {
	h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, nil)
	h.ingest(h.q1(t, 1, 0, true, 32, "abcdefghijklmnop"))
	other := h.q1(t, 2, 0, true, 32, "abcdefghijklmnop")
	other.AddOptionString(message.URIQuery, "other=true")

	h.ingest(other)

	require.Equal(t, uint32(2), h.snapshot().active)
	require.Equal(t, 2, h.snapshot().records)
}

func TestQBlockServerOutOfOrderFirstBlockAdmitsReceiver(t *testing.T) {
	h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{}, nil)
	h.ingest(h.q1(t, 1, 1, false, 32, "qrstuvwxyzabcdef"))

	require.Equal(t, uint32(1), h.snapshot().active)
	require.Empty(t, h.session.writesSnapshot())
}

func TestQBlockServerAdmissionLimitsRollback(t *testing.T) {
	t.Run("metadata", func(t *testing.T) {
		h := newServerHarness(t, qblock.DefaultManagerConfig(), qblockServerConfig{MaxMetadataBytes: 1}, nil)
		before := h.snapshot()
		h.ingest(h.q1(t, 1, 0, true, 32, "abcdefghijklmnop"))
		require.Equal(t, before, h.snapshot())
	})
	t.Run("manager bytes", func(t *testing.T) {
		mc := qblock.DefaultManagerConfig()
		mc.Transfer.MaxBodySize = 32
		mc.MaxRetainedBytes = 32
		h := newServerHarness(t, mc, qblockServerConfig{}, nil)
		before := h.snapshot()
		h.ingest(h.q1(t, 1, 0, true, 48, "abcdefghijklmnop"))
		require.Equal(t, before, h.snapshot())
	})
	t.Run("manager transfers", func(t *testing.T) {
		mc := qblock.DefaultManagerConfig()
		mc.MaxTransfers, mc.MaxTokens = 1, 2
		h := newServerHarness(t, mc, qblockServerConfig{MaxRecords: 2}, nil)
		h.ingest(h.q1(t, 1, 0, true, 32, "abcdefghijklmnop"))
		before := h.snapshot()
		second := h.q1(t, 2, 0, true, 32, "abcdefghijklmnop")
		require.NoError(t, second.SetPath("/other"))
		h.ingest(second)
		require.Equal(t, before, h.snapshot())
	})
}

func TestQBlockServerRequiresValidPrivateConstruction(t *testing.T) {
	session := &qblockTestSession{ctx: context.Background()}
	cfg := DefaultConfig
	cfg.BlockwiseEnable = false
	clientOnly := NewConnWithOpts(session, &cfg, withQBlockClient(qblockClientConfig{Manager: qblock.DefaultManagerConfig()}))
	require.Nil(t, clientOnly.qblockClient.server)

	invalid := NewConnWithOpts(&qblockTestSession{ctx: context.Background()}, &cfg,
		withQBlockClient(qblockClientConfig{Manager: qblock.DefaultManagerConfig()}),
		withQBlockServer(qblockServerConfig{Retention: -time.Second}),
	)
	require.Nil(t, invalid.qblockClient.server)
}

func TestQBlockNextDeadlineIncludesTerminalServerRetention(t *testing.T) {
	h := newServerHarness(t, serverLifecycleConfig(), qblockServerConfig{Retention: 10 * time.Second}, func(w *responsewriter.ResponseWriter[*Conn], _ *pool.Message) {
		require.NoError(t, w.SetResponse(codes.Changed, message.TextPlain, bytes.NewReader([]byte("response"))))
	})
	h.session.writeErr = io.ErrClosedPipe
	h.ingest(h.q1(t, 1, 0, false, 4, "body"))

	var deadline time.Time
	var ok bool
	var records int
	func() {
		h.cc.qblockClient.mu.Lock()
		defer h.cc.qblockClient.mu.Unlock()
		deadline, ok = h.cc.qblockClient.nextDeadlineLocked()
		records = len(h.cc.qblockClient.server.records)
	}()
	require.Equal(t, 1, records)
	require.True(t, ok)
	require.Equal(t, h.now.Add(10*time.Second), deadline)
}

func mustContentFormat(t *testing.T, options message.Options) message.MediaType {
	t.Helper()
	format, err := options.ContentFormat()
	require.NoError(t, err)
	return format
}
