package client

import (
	"net"
	"sync"
	"testing"
	"time"
)

// TestCloseDuringReadLoopIsRaceFree guards the synchronisation around
// Client.conn and Client.rw.
//
// New() starts readLoop() in its own goroutine. That loop reads client.conn in
// its loop condition and reassigns conn/rw when it self-redials after an
// unexpected read error. Close() writes client.conn = nil. Without
// synchronisation on the reading side these are concurrent accesses to the
// same pointer words, which the race detector reports.
//
// Run with -race; without it this test always passes and proves nothing.
func TestCloseDuringReadLoopIsRaceFree(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	// The server accepts and holds connections open without replying, so
	// readLoop sits in its blocking read when Close() lands — the situation a
	// caller creates by closing a client while it is still connected.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var conns []net.Conn
		defer func() {
			for _, c := range conns {
				c.Close()
			}
		}()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conns = append(conns, conn)
		}
	}()

	for i := 0; i < 20; i++ {
		c, err := New(Network, ln.Addr().String())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		c.SetErrorHandler(func(error) {})
		if err := c.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	ln.Close()
	wg.Wait()
}

// TestCloseDuringRedialDoesNotReconnect covers the second route into the
// resurrection: readLoop can take a genuine transport error, call closeConn,
// and be about to redial while Close() lands concurrently -- no nil rw is
// ever observed on this path, unlike the route TestCloseIsIdempotentAndSubsequentCallsFail
// exercises, so a fix that only patched readPacket's nil-rw check would still
// resurrect the connection here.
//
// There is no hook into readLoop's internal state to land exactly between its
// closeConn and connect, so this drives the race blind: kill the connection
// from the server side, which sends readLoop into that window on its own
// goroutine, and call Close() immediately after on this one. Across enough
// rounds some land inside the window. A dial already in flight when Close
// lands is allowed to put one OPTION_REQ on the wire -- setConn's refusal
// happens after the write, see connect() -- so OptionReqs is not the
// assertion; whether the client goes on to use that connection is, which
// getConn and a subsequent call both show directly.
func TestCloseDuringRedialDoesNotReconnect(t *testing.T) {
	const rounds = 50
	s := newFakeJobServer(t)

	for i := 0; i < rounds; i++ {
		c, err := New(Network, s.Addr())
		if err != nil {
			t.Fatalf("round %d: New: %v", i, err)
		}
		c.SetErrorHandler(func(error) {})
		waitFor(t, "handshake before drop", c.ExceptionsEnabled)

		s.DropConnections() // readLoop notices on its own goroutine, asynchronously
		if err := mustReturnWithin(t, time.Second, "Close racing a redial", c.Close); err != nil {
			t.Fatalf("round %d: Close: %v", i, err)
		}

		// Give a dial that raced Close time to come back and try to publish
		// itself; setConn must keep refusing it for as long as we keep
		// looking.
		deadline := time.Now().Add(50 * time.Millisecond)
		for time.Now().Before(deadline) {
			if c.getConn() != nil {
				t.Fatalf("round %d: client reconnected after Close", i)
			}
			time.Sleep(time.Millisecond)
		}

		if _, err := c.DoBg("f", []byte("x"), JobNormal); err != ErrLostConn {
			t.Fatalf("round %d: DoBg after Close = %v, want ErrLostConn", i, err)
		}
	}
}
