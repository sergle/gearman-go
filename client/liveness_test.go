package client

import (
	"fmt"
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
