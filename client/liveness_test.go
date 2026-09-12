package client

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// Regression tests for three defects fixed together: Status and Echo now hold
// client.Mutex across their round trip and give up after ResponseTimeout, and
// readLoop closes through connMu only so an in-flight request cannot stall the
// re-dial.
//
// They lived in knownbugs_test.go while the defects were open; a fixed defect's
// test belongs in the default suite, so the gate came off. They still use
// mustReturnWithin (defined there, same package) because a regression hangs
// rather than fails.
//
// Do not reuse the package-level `client` or `pool` from client_test.go and
// pool_test.go: shared, order-dependent state.

// Status and Echo used to call client.write without holding client.Mutex,
// scribbling into the same bufio.Writer as a concurrent do(). Under -race that
// reports inside bufio; without it the framing just breaks.
func TestConcurrentSubmitAndEchoDoNotCorruptTheStream(t *testing.T) {
	s := newFakeJobServer(t)
	c := newTestClient(t, s)
	c.ResponseTimeout = 2 * time.Second

	const n = 20
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { // submits
			defer wg.Done()
			for i := 0; i < n; i++ {
				if _, err := c.DoBgWithId("f", []byte("payload"), JobNormal, fmt.Sprintf("id-%d", i)); err != nil {
					t.Errorf("DoBgWithId: %v", err)
					return
				}
			}
		}()
		go func() { // echoes, concurrently
			defer wg.Done()
			for i := 0; i < n; i++ {
				if _, err := c.Echo([]byte("ping")); err != nil {
					t.Errorf("Echo: %v", err)
					return
				}
			}
		}()
		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("interleaved writes wedged the connection")
	}

	// Interleaved writes corrupt the framing, so a garbled body is the defect
	// showing up without -race.
	reqs := s.Requests()
	if len(reqs) != 2*n {
		t.Errorf("server received %d requests, want %d", len(reqs), 2*n)
	}
	for i, req := range reqs {
		switch req.DataType {
		case dtSubmitJobBg:
			if fn, _, _ := req.Job(); fn != "f" {
				t.Errorf("request %d: corrupted submit body %q", i, req.Data)
			}
		case dtEchoReq:
			if string(req.Data) != "ping" {
				t.Errorf("request %d: corrupted echo body %q", i, req.Data)
			}
		default:
			t.Errorf("request %d: unexpected opcode %d (framing lost)", i, req.DataType)
		}
	}
}

// Echo used a locked-twice sync.Mutex as a latch with no timeout, so a caller
// blocked forever when no response arrived.
func TestEchoTimesOutWhenServerNeverAnswers(t *testing.T) {
	s := newFakeJobServer(t)
	s.SetSilent(true)
	c := newTestClient(t, s)
	c.ResponseTimeout = 100 * time.Millisecond

	err := mustReturnWithin(t, 2*time.Second, "Echo", func() error {
		_, err := c.Echo([]byte("ping"))
		return err
	})
	if err != ErrLostConn {
		t.Errorf("Echo err = %v, want ErrLostConn", err)
	}
}

// The same defect in Status.
func TestStatusTimesOutWhenServerNeverAnswers(t *testing.T) {
	s := newFakeJobServer(t)
	s.SetSilent(true)
	c := newTestClient(t, s)
	c.ResponseTimeout = 100 * time.Millisecond

	err := mustReturnWithin(t, 2*time.Second, "Status", func() error {
		_, err := c.Status("H:fake:1")
		return err
	})
	if err != ErrLostConn {
		t.Errorf("Status err = %v, want ErrLostConn", err)
	}
}

// readLoop used to close through Close(), which took client.Mutex. do() holds
// that mutex for its whole round trip, so a connection error during an
// in-flight submit stalled the re-dial for a full ResponseTimeout. readLoop now
// uses closeConn(), on connMu only.
//
// The discriminator is 2s of ResponseTimeout against a 500ms assertion: with
// the old code the second OPTION_REQ cannot appear until the parked Do gives
// up, and re-dialling a live loopback listener takes well under a millisecond.
//
// No ErrorHandler on purpose: readLoop calls client.err() from its own
// goroutine, and installing one after New() races that read. Still open.
func TestRedialNotBlockedByInFlightDo(t *testing.T) {
	s := newFakeJobServer(t)
	s.SetSilent(true)
	c := newTestClient(t, s)
	c.ResponseTimeout = 2 * time.Second

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Do("f", []byte("payload"), JobNormal, nil)
	}()

	// Wait until the submit is on the wire, so the goroutine above is parked
	// holding client.Mutex rather than still starting.
	s.WaitRequests(t, 1, 2*time.Second)
	s.DropConnections()

	deadline := time.Now().Add(500 * time.Millisecond)
	for s.OptionReqs() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("no re-dial within 500ms: OPTION_REQ count is %d, want 2 -- "+
				"readLoop is waiting on client.Mutex", s.OptionReqs())
		}
		time.Sleep(time.Millisecond)
	}

	// The submit still has to time out on its own; it cannot be woken.
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("Do did not return")
	}
}

// Close must not wait for an in-flight request: Status and Echo hold
// client.Mutex across their round trip, so a Close on the same mutex would
// block for a whole ResponseTimeout. Hence connMu.
//
// TestCloseIsIdempotentAndSubsequentCallsFail covers Close with nothing in
// flight; this is the concurrent case.
func TestCloseDuringInFlightEcho(t *testing.T) {
	s := newFakeJobServer(t)
	s.SetSilent(true)
	c := newTestClient(t, s)
	c.ResponseTimeout = 2 * time.Second

	echoDone := make(chan struct{})
	go func() {
		defer close(echoDone)
		c.Echo([]byte("ping"))
	}()
	s.WaitRequests(t, 1, 2*time.Second) // the echo is on the wire

	if err := mustReturnWithin(t, 500*time.Millisecond, "Close during an in-flight Echo", c.Close); err != nil {
		t.Errorf("Close: %v", err)
	}

	// Close does not wake the parked Echo; it still runs out its own timeout.
	select {
	case <-echoDone:
	case <-time.After(4 * time.Second):
		t.Fatal("Echo did not return")
	}
}

// do's response handler runs on processLoop's goroutine. It used to assign the
// named returns `handle` and `err`, which a response arriving as the timeout
// fires writes while do is already returning them.
//
// The window is between the slot's take returning a handler and deliver
// calling it: cancelling on timeout empties the slot afterwards, not during. So
// the server answers at exactly ResponseTimeout and the test runs enough rounds
// to land inside it. No assertions -- the detector is the oracle, as in
// race_test.go.
func TestDoHandlerDoesNotWriteReturnsAfterTimeout(t *testing.T) {
	const (
		rounds  = 300
		timeout = 2 * time.Millisecond
	)

	addr := lateJobCreatedServer(t, timeout)
	c, err := New(Network, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.ErrorHandler = func(error) {}
	c.ResponseTimeout = timeout

	for i := 0; i < rounds; i++ {
		// Either outcome is fine: the point is the handler running while do
		// returns, not which side wins.
		c.DoBgWithId("f", []byte("x"), JobNormal, fmt.Sprintf("id-%d", i))
	}
}

// do, Status and Echo share one time.Timer on the Client, reset per call rather
// than allocated per call. This is what that costs if the Go version is wrong.
//
// The dangerous interleaving is: a response wins the select, the timer fires in
// the gap before the deferred Stop, and Stop therefore cannot take it back. Under
// pre-1.23 timer semantics the fired value stays on the buffered channel and the
// *next* caller's Reset does not clear it -- so that caller reads an immediate
// timeout and gets ErrLostConn from a perfectly healthy server.
//
// `go 1.23` in go.mod is the fix, and this test is its guard: it fails under
// GODEBUG=asynctimerchan=1, which restores the old semantics. Driving the same
// thing through a socket does not work -- the window between the select and the
// Stop is microseconds, and a fake server answering on a timer boundary misses
// it in hundreds of rounds -- so the sequence is constructed directly instead.
//
// Building the Client literally is also the test for the lazy init: startTimer
// must work on a zero Client, which is what New's absence here provides.
func TestSharedResponseTimerResetClearsAPendingFire(t *testing.T) {
	c := &Client{ResponseTimeout: time.Millisecond}
	c.Lock()
	defer c.Unlock()

	// Caller A arms it and never receives: its response won the race.
	c.startTimer()
	time.Sleep(20 * time.Millisecond) // long enough that it has certainly fired
	c.stopTimer()

	// Caller B arms the same timer with a timeout it cannot legitimately reach.
	c.ResponseTimeout = time.Hour
	timeout := c.startTimer()
	defer c.stopTimer()
	select {
	case <-timeout:
		t.Fatal("Reset did not clear the fire left by the previous caller: " +
			"the next do/Status/Echo would return ErrLostConn immediately. " +
			"go.mod needs `go 1.23` or later")
	case <-time.After(50 * time.Millisecond):
	}
}

// lateJobCreatedServer answers OPTION_REQ at once -- connect() sends it first
// on every connection and blocks the handshake otherwise -- and every
// SUBMIT_JOB after delay.
func lateJobCreatedServer(t *testing.T, delay time.Duration) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				hdr := make([]byte, minPacketLength)
				for {
					if _, err := io.ReadFull(conn, hdr); err != nil {
						return
					}
					dt := binary.BigEndian.Uint32(hdr[4:8])
					body := make([]byte, binary.BigEndian.Uint32(hdr[8:12]))
					if _, err := io.ReadFull(conn, body); err != nil {
						return
					}
					if dt == dtOptionReq {
						writePacket(conn, dtOptionRes, body)
						continue
					}
					time.Sleep(delay)
					writePacket(conn, dtJobCreated, []byte("H:late:1"))
				}
			}(conn)
		}
	}()
	return ln.Addr().String()
}
