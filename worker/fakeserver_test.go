package worker

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// A fake Gearman job server, enough of the protocol to drive a real Worker with
// no gearmand running. The client package has its own (client/fakeserver_test.go)
// -- the two cannot be shared, because the wire constants are duplicated per
// package (see CLAUDE.md) and the two sides speak opposite halves of the
// protocol.
//
// It answers:
//
//	CAN_DO, CAN_DO_TIMEOUT   -> nothing; the ability is recorded
//	CANT_DO, RESET_ABILITIES -> the ability is forgotten
//	SET_CLIENT_ID            -> nothing; the id is recorded
//	GRAB_JOB, GRAB_JOB_UNIQ  -> JOB_ASSIGN_UNIQ while jobs remain, else NO_JOB
//	PRE_SLEEP                -> nothing, until Serve wakes the worker with NOOP
//	ECHO_REQ                 -> ECHO_RES (body verbatim)
//	WORK_COMPLETE/FAIL/…     -> counted; the Nth one closes the done channel
//
// The job supply is driven by GRAB, not pushed: Work() grabs once per agent at
// startup and handleInPack grabs again for every JOB_ASSIGN it dispatches
// (worker.go:157), so answering each grab with one assignment keeps the
// pipeline saturated with at most one server->worker packet outstanding, which
// keeps the benchmarks measuring the pipeline rather than the queueing.
// Coalescing and splitting are agent.read's problem and it handles both;
// SetChunkWrites forces a split for the tests that want one.
//
// PRE_SLEEP is answered with silence, as gearmand does: a worker that is told
// NO_JOB sleeps until a NOOP wakes it. Replying to PRE_SLEEP immediately would
// turn an idle worker into a busy loop.
//
// All server state is guarded by mu.

type fakeWorkerServer struct {
	ln net.Listener

	wmu       sync.Mutex // see send: chunked packets must not interleave
	mu        sync.Mutex
	conns     []net.Conn
	abilities []string         // funcnames from CAN_DO / CAN_DO_TIMEOUT
	clientId  string           // from SET_CLIENT_ID
	echoes    [][]byte         // ECHO_REQ bodies seen
	results   []fakeWorkResult // WORK_COMPLETE / WORK_FAIL / WORK_EXCEPTION
	record    bool             // record the above; off for benchmarks
	remaining int              // assignments still to hand out
	assigned  int              // assignments handed out
	completed int              // work results seen
	target    int              // completed count that closes done
	done      chan struct{}    // closed when completed == target
	payload   []byte           // body of each JOB_ASSIGN_UNIQ
	fn        string           // funcname of each JOB_ASSIGN_UNIQ
	chunks    []int            // leading write sizes per response; see SetChunkWrites
}

// send exists so a test can force a packet to arrive in pieces. Over loopback
// one Write usually lands as one read, so a split has to be made rather than
// hoped for -- and it has to be paced, because back-to-back writes are
// recoalesced by TCP and by the worker's bufio.Reader. A test that only looked
// split would pass against framing that cannot handle a short read.
//
// wmu is required once packets take several writes: serve() answering a GRAB
// and Serve() sending its wake-up NOOP write to the same connection, and
// interleaved chunks desync the worker for reasons unrelated to the code under
// test.
func (s *fakeWorkerServer) send(conn net.Conn, pkt []byte) {
	s.wmu.Lock()
	defer s.wmu.Unlock()

	s.mu.Lock()
	sizes := append([]int(nil), s.chunks...)
	s.mu.Unlock()

	off := 0
	for _, n := range sizes {
		if n <= 0 || off+n >= len(pkt) {
			break
		}
		if _, err := conn.Write(pkt[off : off+n]); err != nil {
			return
		}
		off += n
		time.Sleep(time.Millisecond)
	}
	conn.Write(pkt[off:])
}

// fakeWorkResult is one WORK_COMPLETE-family packet received from the worker.
type fakeWorkResult struct {
	DataType uint32
	Handle   string
	Data     []byte
}

const (
	fakeJobHandle = "H:fake:1"
	fakeJobUniq   = "uniq-1"
)

// newFakeWorkerServer starts a server on a free port and stops it when the test
// ends. testing.TB rather than *testing.T so benchmarks can use it.
func newFakeWorkerServer(t testing.TB) *fakeWorkerServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeWorkerServer{
		ln:      ln,
		record:  true,
		done:    make(chan struct{}),
		payload: []byte("payload"),
		fn:      "bench",
	}
	t.Cleanup(s.stop)
	go s.accept()
	return s
}

// Addr is the host:port to hand to Worker.AddServer.
func (s *fakeWorkerServer) Addr() string { return s.ln.Addr().String() }

func (s *fakeWorkerServer) stop() {
	s.ln.Close()
	s.mu.Lock()
	conns := s.conns
	s.conns = nil
	s.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}

func (s *fakeWorkerServer) accept() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return // listener closed
		}
		s.mu.Lock()
		s.conns = append(s.conns, conn)
		s.mu.Unlock()
		go s.serve(conn)
	}
}

func (s *fakeWorkerServer) serve(conn net.Conn) {
	defer conn.Close()
	hdr := make([]byte, minPacketLength)
	for {
		if _, err := io.ReadFull(conn, hdr); err != nil {
			return // worker hung up, or the test ended
		}
		dataType := binary.BigEndian.Uint32(hdr[4:8])
		var body []byte
		if n := binary.BigEndian.Uint32(hdr[8:12]); n > 0 {
			body = make([]byte, n)
			if _, err := io.ReadFull(conn, body); err != nil {
				return
			}
		}
		s.handle(conn, dataType, body)
	}
}

func (s *fakeWorkerServer) handle(conn net.Conn, dataType uint32, body []byte) {
	switch dataType {
	case dtCanDo:
		s.addAbility(string(body))
	case dtCanDoTimeout:
		s.addAbility(string(bytes.SplitN(body, []byte{'\x00'}, 2)[0]))
	case dtCantDo:
		s.removeAbility(string(body))
	case dtResetAbilities:
		s.mu.Lock()
		s.abilities = nil
		s.mu.Unlock()
	case dtSetClientId:
		s.mu.Lock()
		s.clientId = string(body)
		s.mu.Unlock()

	case dtGrabJob, dtGrabJobUniq:
		if assign, ok := s.takeJob(); ok {
			s.send(conn, assign)
		} else {
			s.send(conn, resPacket(dtNoJob, nil))
		}

	case dtPreSleep:
		// Silence on purpose: the worker is now asleep until Serve sends NOOP.

	case dtEchoReq:
		s.mu.Lock()
		if s.record {
			s.echoes = append(s.echoes, body)
		}
		s.mu.Unlock()
		s.send(conn, resPacket(dtEchoRes, body))

	case dtWorkComplete, dtWorkFail, dtWorkException:
		s.completeJob(dataType, body)

	case dtWorkStatus, dtWorkData, dtWorkWarning:
		// Progress reports; nothing to answer.
	}
}

func (s *fakeWorkerServer) addAbility(fn string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.record {
		s.abilities = append(s.abilities, fn)
	}
}

func (s *fakeWorkerServer) removeAbility(fn string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, a := range s.abilities {
		if a == fn {
			s.abilities = append(s.abilities[:i], s.abilities[i+1:]...)
			return
		}
	}
}

// takeJob hands out one assignment if any are left. The packet is built inside
// the lock but from fixed parts, so it costs one allocation per job and no
// per-job bookkeeping -- Serve(b.N) must not make the harness the thing being
// measured.
func (s *fakeWorkerServer) takeJob() ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.remaining == 0 {
		return nil, false
	}
	if s.remaining > 0 {
		s.remaining--
	}
	s.assigned++
	return resPacket(dtJobAssignUniq, jobAssignBody(fakeJobHandle, s.fn, fakeJobUniq, s.payload)), true
}

func (s *fakeWorkerServer) completeJob(dataType uint32, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.record {
		parts := bytes.SplitN(body, []byte{'\x00'}, 2)
		r := fakeWorkResult{DataType: dataType, Handle: string(parts[0])}
		if len(parts) == 2 {
			r.Data = parts[1]
		}
		s.results = append(s.results, r)
	}
	s.completed++
	if s.target > 0 && s.completed == s.target {
		close(s.done)
	}
}

// jobAssignBody builds a JOB_ASSIGN_UNIQ body. The four NUL-separated fields
// are not optional: decodeInPack (inpack.go:113-120) drops the packet's
// contents entirely unless SplitN finds exactly four, which leaves fn empty,
// makes exec fail with "The function does not exist" and sends no
// WORK_COMPLETE -- a benchmark that then just times out with nothing to show.
func jobAssignBody(handle, fn, uniq string, data []byte) []byte {
	buf := make([]byte, 0, len(handle)+len(fn)+len(uniq)+len(data)+3)
	buf = append(buf, handle...)
	buf = append(buf, '\x00')
	buf = append(buf, fn...)
	buf = append(buf, '\x00')
	buf = append(buf, uniq...)
	buf = append(buf, '\x00')
	return append(buf, data...)
}

// resPacket frames one server-side (\x00RES) packet.
func resPacket(dataType uint32, data []byte) []byte {
	buf := make([]byte, minPacketLength+len(data))
	copy(buf[:4], resStr)
	binary.BigEndian.PutUint32(buf[4:8], dataType)
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(data)))
	copy(buf[minPacketLength:], data)
	return buf
}

// --- knobs -----------------------------------------------------------------

// Serve makes n job assignments available and returns a channel closed once n
// results have come back. It also NOOPs every live connection, because a worker
// that has already been told NO_JOB is asleep and will not grab again on its
// own.
//
// Call it after the worker is running. n < 0 means an unlimited supply, in
// which case the returned channel is never closed.
func (s *fakeWorkerServer) Serve(n int) <-chan struct{} {
	s.mu.Lock()
	s.remaining = n
	s.target = n
	s.completed = 0
	s.assigned = 0
	s.done = make(chan struct{})
	done := s.done
	conns := append([]net.Conn(nil), s.conns...)
	s.mu.Unlock()

	noop := resPacket(dtNoop, nil)
	for _, c := range conns {
		s.send(c, noop)
	}
	return done
}

// SetJob fixes the funcname and payload of every assignment.
func (s *fakeWorkerServer) SetJob(fn string, payload []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fn = fn
	s.payload = payload
}

// SetChunkWrites takes leading write sizes rather than one chunk size because
// the sizes are the whole point. (8, 10) is the pattern that desynced the old
// framing: a first write shorter than a header, then one crossing 12 bytes, so
// read returns a partial packet and the next starts mid-body, taking its length
// from payload bytes. A uniform sub-header split does *not* reproduce it --
// every read then comes back short and the old leftdata buffer reassembled
// those correctly, which cost one wrong test before it was noticed.
func (s *fakeWorkerServer) SetChunkWrites(sizes ...int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chunks = append([]int(nil), sizes...)
}

// SetRecord turns recording of abilities, echoes and work results on or off. It
// is on by default. Benchmarks turn it off: recording appends one result plus a
// copy of its body per job, which would grow with b.N inside the measured
// region. The counters that Serve depends on are kept either way.
func (s *fakeWorkerServer) SetRecord(record bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.record = record
}

// Abilities returns the funcnames registered so far.
func (s *fakeWorkerServer) Abilities() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.abilities...)
}

// Results returns the WORK_COMPLETE-family packets received so far.
func (s *fakeWorkerServer) Results() []fakeWorkResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]fakeWorkResult(nil), s.results...)
}

// Counts reports assignments handed out and results received.
func (s *fakeWorkerServer) Counts() (assigned, completed int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.assigned, s.completed
}

// --- harness self-tests ----------------------------------------------------
//
// These prove the fake server actually speaks the protocol. Without them the
// benchmarks built on it would be measuring a harness nobody had checked --
// and the failure mode is silent: a malformed JOB_ASSIGN body produces a worker
// that grabs, dispatches nothing and reports nothing.

// newTestWorker wires a worker to the fake server, registers fn, and starts
// Work() in its own goroutine (Work blocks, and it is the only place jobs are
// dispatched).
//
// It deliberately does not call Close() at the end: Close closes worker.in
// while the agent's work() goroutine may still be sending into it
// (worker.go:231 against agent.go:101), which panics. The fake server's own
// cleanup drops the connection, which is what ends the agent loop.
func newTestWorker(t testing.TB, s *fakeWorkerServer, fn string, f JobFunc) *Worker {
	t.Helper()
	w := New(Unlimited)
	w.ErrorHandler = func(error) {} // the teardown disconnect is expected
	if err := w.AddServer(Network, s.Addr()); err != nil {
		t.Fatal(err)
	}
	if err := w.AddFunc(fn, f, 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Ready(); err != nil {
		t.Fatal(err)
	}
	go w.Work()
	return w
}

func TestFakeWorkerServerRunsAJobCycle(t *testing.T) {
	s := newFakeWorkerServer(t)
	s.SetJob("bench", []byte("payload"))

	ran := make(chan Job, 1)
	newTestWorker(t, s, "bench", func(job Job) ([]byte, error) {
		ran <- job
		return job.Data(), nil
	})

	// The worker announces what it can do as part of Ready().
	deadline := time.Now().Add(2 * time.Second)
	for len(s.Abilities()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("server saw no CAN_DO")
		}
		time.Sleep(time.Millisecond)
	}
	if got := s.Abilities(); got[0] != "bench" {
		t.Errorf("abilities = %v, want [bench]", got)
	}

	done := s.Serve(1)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		assigned, completed := s.Counts()
		t.Fatalf("no WORK_COMPLETE: assigned %d, completed %d", assigned, completed)
	}

	select {
	case job := <-ran:
		if job.Fn() != "bench" {
			t.Errorf("job.Fn() = %q, want bench", job.Fn())
		}
		if job.Handle() != fakeJobHandle {
			t.Errorf("job.Handle() = %q, want %q", job.Handle(), fakeJobHandle)
		}
		if job.UniqueId() != fakeJobUniq {
			t.Errorf("job.UniqueId() = %q, want %q", job.UniqueId(), fakeJobUniq)
		}
		if string(job.Data()) != "payload" {
			t.Errorf("job.Data() = %q, want payload", job.Data())
		}
	default:
		t.Fatal("the job function never ran, but a result arrived")
	}

	results := s.Results()
	if len(results) != 1 {
		t.Fatalf("results = %d, want 1", len(results))
	}
	if results[0].DataType != dtWorkComplete {
		t.Errorf("result opcode = %d, want %d (WORK_COMPLETE)", results[0].DataType, dtWorkComplete)
	}
	if results[0].Handle != fakeJobHandle || string(results[0].Data) != "payload" {
		t.Errorf("result = (%q, %q)", results[0].Handle, results[0].Data)
	}
}

// A failing job function must come back as WORK_FAIL when it returns no data,
// and the pipeline must keep going.
func TestFakeWorkerServerRecordsFailure(t *testing.T) {
	s := newFakeWorkerServer(t)
	newTestWorker(t, s, "bench", func(job Job) ([]byte, error) {
		return nil, ErrUnknown
	})

	done := s.Serve(1)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("no result for a failing job")
	}
	if results := s.Results(); results[0].DataType != dtWorkFail {
		t.Errorf("result opcode = %d, want %d (WORK_FAIL)", results[0].DataType, dtWorkFail)
	}
}

// Several jobs in sequence: each JOB_ASSIGN triggers the next GRAB, so the
// supply drains without anything pushing it.
func TestFakeWorkerServerDrainsAQueue(t *testing.T) {
	s := newFakeWorkerServer(t)
	var mu sync.Mutex
	ran := 0
	newTestWorker(t, s, "bench", func(job Job) ([]byte, error) {
		mu.Lock()
		ran++
		mu.Unlock()
		return nil, nil
	})

	const n = 25
	done := s.Serve(n)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		assigned, completed := s.Counts()
		t.Fatalf("drained %d/%d: assigned %d", completed, n, assigned)
	}
	mu.Lock()
	defer mu.Unlock()
	if ran != n {
		t.Errorf("the job function ran %d times, want %d", ran, n)
	}
}
