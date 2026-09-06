package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// A fake Gearman job server, enough of the protocol to drive the real client
// with no gearmand running. The wire format is trivial: a 12-byte header of
// magic, big-endian data type and big-endian body length, then the body.
//
// It answers:
//
//	SUBMIT_JOB{,_HIGH,_LOW}       -> JOB_CREATED, then WORK_COMPLETE
//	SUBMIT_JOB{,_HIGH,_LOW}_BG    -> JOB_CREATED only
//	ECHO_REQ                      -> ECHO_RES (body verbatim)
//	GET_STATUS                    -> STATUS_RES
//
// Handles are deterministic: H:fake:1, H:fake:2, ... in submit order. Repeat
// submissions of the same unique id coalesce onto the same handle, as gearmand
// does.
//
// All server state is guarded by mu. That matters more than usual here: this
// package is tested under -race specifically to find races in the client, so a
// race in the harness would be indistinguishable from the bug under test.

// fakeRequest is one packet received from the client.
type fakeRequest struct {
	DataType uint32
	Data     []byte
}

// Job splits a submit request body into its funcname, id and payload.
func (r fakeRequest) Job() (funcname, id string, payload []byte) {
	parts := bytes.SplitN(r.Data, []byte{'\x00'}, 3)
	if len(parts) != 3 {
		return "", "", nil
	}
	return string(parts[0]), string(parts[1]), parts[2]
}

type fakeStatus struct {
	Known, Running         bool
	Numerator, Denominator uint64
}

type fakeServer struct {
	ln net.Listener

	mu       sync.Mutex
	requests []fakeRequest
	conns    []net.Conn
	silent   bool
	handles  int
	byId     map[string]string // unique id -> handle, for coalescing
	status   map[string]fakeStatus
}

// newFakeServer starts a server on a free port and stops it when the test ends.
func newFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeServer{
		ln:     ln,
		byId:   map[string]string{},
		status: map[string]fakeStatus{},
	}
	t.Cleanup(s.stop)
	go s.accept()
	return s
}

// Addr is the host:port to hand to New.
func (s *fakeServer) Addr() string { return s.ln.Addr().String() }

func (s *fakeServer) stop() {
	s.ln.Close()
	s.mu.Lock()
	conns := s.conns
	s.conns = nil
	s.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}

func (s *fakeServer) accept() {
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

func (s *fakeServer) serve(conn net.Conn) {
	defer conn.Close()
	hdr := make([]byte, minPacketLength)
	for {
		if _, err := io.ReadFull(conn, hdr); err != nil {
			return // client hung up, or the test ended
		}
		req := fakeRequest{DataType: binary.BigEndian.Uint32(hdr[4:8])}
		if n := binary.BigEndian.Uint32(hdr[8:12]); n > 0 {
			req.Data = make([]byte, n)
			if _, err := io.ReadFull(conn, req.Data); err != nil {
				return
			}
		}

		s.mu.Lock()
		s.requests = append(s.requests, req)
		silent := s.silent
		s.mu.Unlock()

		if silent {
			continue // record it, answer nothing: drives the timeout paths
		}
		s.respond(conn, req)
	}
}

func (s *fakeServer) respond(conn net.Conn, req fakeRequest) {
	switch req.DataType {
	case dtSubmitJob, dtSubmitJobHigh, dtSubmitJobLow:
		_, id, payload := req.Job()
		handle := s.handleFor(id)
		writePacket(conn, dtJobCreated, []byte(handle))
		writePacket(conn, dtWorkComplete, append([]byte(handle+"\x00"), payload...))

	case dtSubmitJobBg, dtSubmitJobHighBg, dtSubmitJobLowBg:
		_, id, _ := req.Job()
		writePacket(conn, dtJobCreated, []byte(s.handleFor(id)))

	case dtEchoReq:
		writePacket(conn, dtEchoRes, req.Data)

	case dtGetStatus:
		handle := string(req.Data)
		s.mu.Lock()
		st, ok := s.status[handle]
		s.mu.Unlock()
		if !ok {
			st = fakeStatus{Known: true, Running: true, Numerator: 1, Denominator: 2}
		}
		// decodeResponse takes the handle off the front; _status wants the
		// remaining four NUL-separated fields.
		body := fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%d",
			handle, boolByte(st.Known), boolByte(st.Running),
			st.Numerator, st.Denominator)
		writePacket(conn, dtStatusRes, []byte(body))
	}
}

// handleFor allocates a handle for a unique id, coalescing repeat submissions
// of the same id onto the same handle as gearmand does.
func (s *fakeServer) handleFor(id string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if h, ok := s.byId[id]; ok {
		return h
	}
	s.handles++
	h := fmt.Sprintf("H:fake:%d", s.handles)
	s.byId[id] = h
	return h
}

func boolByte(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// writePacket frames and sends one response packet.
func writePacket(conn net.Conn, dataType uint32, data []byte) {
	buf := make([]byte, minPacketLength+len(data))
	copy(buf[:4], resStr)
	binary.BigEndian.PutUint32(buf[4:8], dataType)
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(data)))
	copy(buf[minPacketLength:], data)
	conn.Write(buf)
}

// --- knobs -----------------------------------------------------------------

// Requests returns a snapshot of everything received so far.
func (s *fakeServer) Requests() []fakeRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]fakeRequest, len(s.requests))
	copy(out, s.requests)
	return out
}

// WaitRequests blocks until at least n requests have arrived, and returns a
// snapshot. It fails the test on timeout rather than blocking forever.
func (s *fakeServer) WaitRequests(t *testing.T, n int, d time.Duration) []fakeRequest {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		if got := s.Requests(); len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d requests, got %d", n, len(s.Requests()))
			return nil
		}
		time.Sleep(time.Millisecond)
	}
}

// SetSilent stops the server answering. Requests are still recorded, so the
// client is left waiting for a response that never comes.
func (s *fakeServer) SetSilent(silent bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.silent = silent
}

// SetStatus fixes what GET_STATUS reports for a handle.
func (s *fakeServer) SetStatus(handle string, st fakeStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status[handle] = st
}

// DropConnections hangs up on every live client, as a restarting gearmand
// would. The listener stays open, so the client can reconnect.
func (s *fakeServer) DropConnections() {
	s.mu.Lock()
	conns := s.conns
	s.conns = nil
	s.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}

// --- harness self-tests ----------------------------------------------------
//
// These prove the fake server actually speaks the protocol. Without them the
// characterisation tests built on it would be asserting against a harness that
// nobody had checked.

func TestFakeServerDo(t *testing.T) {
	s := newFakeServer(t)
	c, err := New(Network, s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	done := make(chan []byte, 1)
	handle, err := c.Do("ToUpper", []byte("payload"), JobNormal, func(r *Response) {
		done <- r.Data
	})
	if err != nil {
		t.Fatal(err)
	}
	if handle != "H:fake:1" {
		t.Errorf("handle = %q, want H:fake:1", handle)
	}
	select {
	case got := <-done:
		if string(got) != "payload" {
			t.Errorf("WORK_COMPLETE payload = %q, want %q", got, "payload")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no WORK_COMPLETE delivered")
	}
}

func TestFakeServerDoBgRecordsRequest(t *testing.T) {
	s := newFakeServer(t)
	c, err := New(Network, s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.DoBgWithId("ToUpper", []byte("data"), JobHigh, "my-id"); err != nil {
		t.Fatal(err)
	}
	reqs := s.WaitRequests(t, 1, 2*time.Second)
	if reqs[0].DataType != dtSubmitJobHighBg {
		t.Errorf("opcode = %d, want %d (SUBMIT_JOB_HIGH_BG)", reqs[0].DataType, dtSubmitJobHighBg)
	}
	fn, id, payload := reqs[0].Job()
	if fn != "ToUpper" || id != "my-id" || string(payload) != "data" {
		t.Errorf("job = (%q, %q, %q)", fn, id, payload)
	}
}

func TestFakeServerEcho(t *testing.T) {
	s := newFakeServer(t)
	c, err := New(Network, s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	echo, err := c.Echo([]byte("Hello\x00 world"))
	if err != nil {
		t.Fatal(err)
	}
	if string(echo) != "Hello\x00 world" {
		t.Errorf("echo = %q", echo)
	}
}

func TestFakeServerStatus(t *testing.T) {
	s := newFakeServer(t)
	s.SetStatus("H:fake:7", fakeStatus{Known: true, Running: false, Numerator: 3, Denominator: 9})
	c, err := New(Network, s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	st, err := c.Status("H:fake:7")
	if err != nil {
		t.Fatal(err)
	}
	if st.Handle != "H:fake:7" || !st.Known || st.Running || st.Numerator != 3 || st.Denominator != 9 {
		t.Errorf("status = %+v", st)
	}
}

func TestFakeServerSilentDrivesTimeout(t *testing.T) {
	s := newFakeServer(t)
	s.SetSilent(true)
	c, err := New(Network, s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.ResponseTimeout = 50 * time.Millisecond

	if _, err := c.Do("ToUpper", []byte("x"), JobNormal, nil); err != ErrLostConn {
		t.Errorf("err = %v, want ErrLostConn", err)
	}
	if reqs := s.Requests(); len(reqs) != 1 {
		t.Errorf("server recorded %d requests, want 1", len(reqs))
	}
}

// Driven over a raw socket rather than through Client on purpose. Dropping the
// connection sends the real client into readLoop's re-dial, which is itself a
// known data race (todo.md section 2) — running this through Client would make
// the harness self-tests fail under -race for a reason that has nothing to do
// with the harness. client/race_test.go covers that race deliberately.
func TestFakeServerDropConnections(t *testing.T) {
	s := newFakeServer(t)

	conn, err := net.Dial(Network, s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// A live connection answers.
	sendRequest(t, conn, dtEchoReq, []byte("ping"))
	if dt, body := readPacket(t, conn); dt != dtEchoRes || string(body) != "ping" {
		t.Fatalf("echo response = (%d, %q)", dt, body)
	}

	s.DropConnections()

	// After the drop the peer is gone: the next read ends the stream.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(conn, make([]byte, minPacketLength)); err == nil {
		t.Fatal("expected the dropped connection to fail, got a successful read")
	}

	// The listener stays open, so a fresh connection still works.
	conn2, err := net.Dial(Network, s.Addr())
	if err != nil {
		t.Fatalf("listener closed after DropConnections: %v", err)
	}
	defer conn2.Close()
	sendRequest(t, conn2, dtEchoReq, []byte("again"))
	if dt, body := readPacket(t, conn2); dt != dtEchoRes || string(body) != "again" {
		t.Fatalf("echo response after reconnect = (%d, %q)", dt, body)
	}
}

// sendRequest writes one client-side (\x00REQ) packet.
func sendRequest(t *testing.T, conn net.Conn, dataType uint32, data []byte) {
	t.Helper()
	buf := make([]byte, minPacketLength+len(data))
	copy(buf[:4], reqStr)
	binary.BigEndian.PutUint32(buf[4:8], dataType)
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(data)))
	copy(buf[minPacketLength:], data)
	if _, err := conn.Write(buf); err != nil {
		t.Fatal(err)
	}
}

// readPacket reads one server-side packet.
func readPacket(t *testing.T, conn net.Conn) (uint32, []byte) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	hdr := make([]byte, minPacketLength)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		t.Fatal(err)
	}
	body := make([]byte, binary.BigEndian.Uint32(hdr[8:12]))
	if _, err := io.ReadFull(conn, body); err != nil {
		t.Fatal(err)
	}
	return binary.BigEndian.Uint32(hdr[4:8]), body
}
