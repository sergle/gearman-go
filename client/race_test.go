package client

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// These tests reproduce the data races documented in docs/todo.md. They carry
// no assertions of their own: the race detector is the oracle, and a reported
// race fails the test binary. Without -race they pass trivially, so they must
// be run as:
//
//	go test -race -run TestRace ./client
//
// They deliberately need no gearmand, so they are not gated behind the
// -integration flag the rest of the suite uses.

// hostileServer accepts a connection and immediately shuts down its own side,
// which is what a gearmand restart looks like from the client side. That drives
// readLoop into its error path, where the unsynchronised conn/rw accesses live.
//
// It half-closes (FIN) and then keeps draining rather than closing outright,
// so the client always observes io.EOF. This matters: a full close leaves the
// client's unread in-flight writes to come back as an RST, and readLoop treats
// a non-temporary *net.OpError as break-without-re-dial (client.go:133).
// Only io.EOF reaches the re-dial at client.go:140, which is the code the
// conn and rw races live in.
func hostileServer(t *testing.T) string {
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
				if tcp, ok := conn.(*net.TCPConn); ok {
					tcp.CloseWrite() // FIN now: the client's next read is EOF
				}
				io.Copy(io.Discard, conn) // absorb writes so they never RST
			}(conn)
		}
	}()
	return ln.Addr().String()
}

// TestRaceErrorHandler covers todo.md section 1: readLoop reads
// client.ErrorHandler at client.go:200, but New starts readLoop at
// client.go:85 before it returns, so the documented way to install a handler
// always races the goroutine that reads it.
func TestRaceErrorHandler(t *testing.T) {
	addr := hostileServer(t)

	c, err := New(Network, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.ErrorHandler = func(error) {} // the documented usage

	// Give readLoop time to fail its read and call err().
	time.Sleep(200 * time.Millisecond)
}

// TestRaceCloseDuringRedial covers todo.md section 2: Close writes conn at
// client.go:320 under client.Mutex while readLoop's internal re-dial reads it
// at client.go:124 and writes it at client.go:140 holding nothing.
func TestRaceCloseDuringRedial(t *testing.T) {
	addr := hostileServer(t)

	for i := 0; i < 20; i++ {
		c, err := New(Network, addr)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond) // let readLoop begin its re-dial
		c.Close()
	}
}

// TestRaceWriteDuringRedial covers todo.md section 3: do -> write uses rw
// under client.Mutex while readLoop's re-dial replaces it at
// client.go:145-146 under no lock at all.
func TestRaceWriteDuringRedial(t *testing.T) {
	addr := hostileServer(t)

	c, err := New(Network, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// The default 1s timeout would make this take minutes; nothing else reads
	// this field, so setting it here is not itself a race.
	c.ResponseTimeout = 10 * time.Millisecond

	// readLoop only swaps rw on its way through the error path, so a single
	// short burst of writes can miss the window. Several writers over a
	// longer run make the overlap reliable rather than lucky.
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				c.DoBg("test", []byte("{}"), JobNormal)
			}
		}()
	}
	wg.Wait()
}
