package client

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

// The created/echo slots held one entry, so a timed-out caller's late reply
// satisfied whoever issued the next request. handlerSlot is a bounded FIFO
// now, and a timed-out entry stays queued to absorb its own reply.

// --- unit tests against handlerSlot directly --------------------------------

// An entry left behind by abandon must absorb its own reply, not hand it on.
func TestHandlerSlotAbandonedEntryIsDiscardedNotDelivered(t *testing.T) {
	var s handlerSlot
	var got1, got2 bool
	tok1 := s.set(func(*Response) { got1 = true }, nil)
	s.set(func(*Response) { got2 = true }, nil)
	s.abandon(tok1, time.Hour)

	// The reply correlated with the first (now-abandoned) request pops first.
	if h := s.take(); h.internal != nil {
		t.Fatal("abandoned entry still delivered a handler")
	}
	// The reply correlated with the second, still-live request pops next.
	h := s.take()
	if h.internal == nil {
		t.Fatal("the live entry was lost along with the abandoned one")
	}
	h.internal(nil)
	if got1 || !got2 {
		t.Errorf("got1=%v got2=%v, want false true", got1, got2)
	}
}

// The write-failure path removes exactly its own entry, which is not always
// the front: an abandoned entry may still be queued ahead of it.
func TestHandlerSlotCancelRemovesOnlyItsOwnEntry(t *testing.T) {
	var s handlerSlot
	tok1 := s.set(nil, nil)
	s.abandon(tok1, time.Hour) // an earlier caller timed out; its entry stays queued

	var fired bool
	tok2 := s.set(func(*Response) { fired = true }, nil)
	s.cancel(tok2) // this caller's write failed; no reply is coming

	// Only the abandoned entry is left, and it must still be a no-op.
	if h := s.take(); h.internal != nil {
		t.Fatal("cancel removed the wrong entry")
	}
	if h := s.take(); h.internal != nil {
		t.Fatal("cancel left a stale entry behind")
	}
	if fired {
		t.Error("cancelled handler ran")
	}
}

// Eviction takes the oldest entry, never the live newest. Asserting the
// length alone would also pass if set refused to append, or evicted the wrong
// end.
func TestHandlerSlotOverflowEvictsOldestAbandonedEntry(t *testing.T) {
	var s handlerSlot
	const extra = 5
	// Past the cap, all abandoned but the last.
	for i := 0; i < handlerSlotCap+extra-1; i++ {
		s.abandon(s.set(nil, nil), time.Hour)
	}
	var lastFired bool
	s.set(func(*Response) { lastFired = true }, nil)

	if got := len(s.queue); got != handlerSlotCap {
		t.Fatalf("queue length = %d, want %d (handlerSlotCap)", got, handlerSlotCap)
	}
	for i := 0; i < handlerSlotCap-1; i++ {
		if h := s.take(); h.internal != nil {
			t.Fatalf("entry %d ahead of the live one was not the abandoned no-op eviction should have kept", i)
		}
	}
	h := s.take()
	if h.internal == nil {
		t.Fatal("overflow evicted the live entry instead of an abandoned one")
	}
	h.internal(nil)
	if !lastFired {
		t.Error("the surviving live handler never ran")
	}
}

// With the one-live-entry invariant broken, eviction must grow the queue
// rather than drop a handler. Contrived, and what set's check is for.
func TestHandlerSlotNeverEvictsALiveFrontEntry(t *testing.T) {
	var s handlerSlot
	for i := 0; i < handlerSlotCap+5; i++ {
		s.set(func(*Response) {}, nil) // never abandoned or taken: all live
	}
	if got := len(s.queue); got <= handlerSlotCap {
		t.Fatalf("queue length = %d, want more than %d: eviction trimmed a live front entry", got, handlerSlotCap)
	}
}

// --- end-to-end regression, driven over a real socket -----------------------

// lateEchoServer holds the first ECHO_REQ unanswered until a second arrives,
// then answers the held one and the second right behind it: a reply landing
// after its caller gave up, ahead of the next caller's own.
func lateEchoServer(t *testing.T) string {
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
				var held []byte
				for {
					if _, err := io.ReadFull(conn, hdr); err != nil {
						return
					}
					dt := binary.BigEndian.Uint32(hdr[4:8])
					body := make([]byte, binary.BigEndian.Uint32(hdr[8:12]))
					if _, err := io.ReadFull(conn, body); err != nil {
						return
					}
					switch {
					case dt == dtOptionReq:
						writePacket(conn, dtOptionRes, body)
					case dt == dtEchoReq && held == nil:
						held = append([]byte(nil), body...) // hold: no reply yet
					case dt == dtEchoReq:
						writePacket(conn, dtEchoRes, held) // the late reply, first
						writePacket(conn, dtEchoRes, body) // this caller's own reply
					}
				}
			}(conn)
		}
	}()
	return ln.Addr().String()
}

// TestEchoLateReplyDoesNotSatisfyNextCaller: a first Echo times out against a
// server that never answers it, a second Echo is then issued, and the first
// caller's reply finally arrives just ahead of the second's own. The second
// caller must get its own data back, not the first caller's.
func TestEchoLateReplyDoesNotSatisfyNextCaller(t *testing.T) {
	addr := lateEchoServer(t)
	c, err := New(Network, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.ResponseTimeout = 100 * time.Millisecond

	if _, err := c.Echo([]byte("first")); err != ErrLostConn {
		t.Fatalf("first Echo err = %v, want ErrLostConn (server never answers it)", err)
	}

	c.ResponseTimeout = 2 * time.Second
	got, err := c.Echo([]byte("second"))
	if err != nil {
		t.Fatalf("second Echo: %v", err)
	}
	if string(got) != "second" {
		t.Errorf("second Echo = %q, want %q -- the first caller's late reply satisfied it instead",
			got, "second")
	}
}
