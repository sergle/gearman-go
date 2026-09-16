package worker

import (
	"bufio"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

// badMagicServer answers every accepted connection with one malformed packet
// (wrong \x00RES magic) and then goes silent. Each accept therefore drives
// agent.read into the generic-error branch of work(), which redials -- the one
// spot that used to touch a.conn/a.rw with no lock at all, unlike every other
// writer of that pair (reconnect, the exported Write path), which already held
// a.Mutex. A concurrent Write (locked, same as every documented writer) still
// dereferences a.rw underneath the unlocked redial.
func badMagicServer(t *testing.T) string {
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
			go func(c net.Conn) {
				defer c.Close()
				hdr := make([]byte, minPacketLength)
				copy(hdr[:4], reqStr) // \x00REQ -- a job server has no
				// business sending this; decodeInPack ignores hdr[0:4], so
				// the magic check is read()'s only cheap desync detector and
				// the cheapest way to make every read() call fail without
				// needing real protocol state.
				binary.BigEndian.PutUint32(hdr[8:12], 0)
				c.Write(hdr)
			}(conn)
		}
	}()
	return ln.Addr().String()
}

// TestWorkRedialRacesConcurrentWrite: work()'s inline redial branch
// assigned a.conn/a.rw with no lock at all, so a concurrent Write -- which
// takes a.Mutex, exactly like every documented writer -- read a.rw out from
// under it. -race must catch this before the fix (a torn/unsynchronized
// pointer read, occasionally a use of the stale connection) and run clean
// after it, for the whole two-second window below.
func TestWorkRedialRacesConcurrentWrite(t *testing.T) {
	addr := badMagicServer(t)
	w := New(Unlimited)
	w.SetErrorHandler(func(error) {}) // the bad-magic errors are expected noise

	a, err := newAgent(Network, addr, w)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Connect(); err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	stop := make(chan struct{})
	time.AfterFunc(2*time.Second, func() { close(stop) })

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			a.Write(&outPack{dataType: dtPreSleep})
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("writer goroutine did not stop within 5s of the deadline")
	}
}

// TestReadOperatesOnlyOnItsArgument pins the refinement on top of the conn
// lock and the connect guard:
// read() used to snapshot a.rw itself (via getRW()) on every call, so a stale
// work() loop calling read() again after a newer Connect/reconnect had
// already published could pick up the *new* connection instead of its own --
// two readers on one bufio.Reader, exactly the desync class the epoch check
// exists to prevent, and that check (which only runs after read() returns) too
// late to stop it.
//
// read() now takes rw as an explicit parameter and never consults a.rw or
// getRW() itself, so this is provable without racing real goroutines against
// each other: publish one connection (rw2) as "current" via setConn, put a
// packet only on the *other* one's wire (rw1, which nothing published), and
// call read(rw1). If read ever looked at the published field instead of its
// argument, it would return rw2's packet immediately; it must instead block
// on rw1 having nothing to give it, even though rw2 has a whole valid packet
// sitting there unread the entire time.
func TestReadOperatesOnlyOnItsArgument(t *testing.T) {
	local1, remote1 := net.Pipe()
	defer local1.Close()
	defer remote1.Close()
	local2, remote2 := net.Pipe()
	defer local2.Close()
	defer remote2.Close()

	a := &agent{}
	rw1 := bufio.NewReadWriter(bufio.NewReader(local1), bufio.NewWriter(local1))
	rw2 := bufio.NewReadWriter(bufio.NewReader(local2), bufio.NewWriter(local2))
	a.setConn(local2, rw2) // rw2 is "current"; rw1 is deliberately not published

	pkt := resPacket(dtNoop, nil)
	go remote2.Write(pkt) // waiting on the *published* connection the whole time

	done := make(chan readResult, 1)
	go func() {
		data, err := a.read(rw1)
		done <- readResult{data, err}
	}()

	select {
	case got := <-done:
		t.Fatalf("read(rw1) returned (%q, %v) with nothing ever written to rw1's "+
			"pipe -- it must have consulted the published connection instead of "+
			"the rw passed to it", got.data, got.err)
	case <-time.After(200 * time.Millisecond):
		// Correct: still blocked on rw1, proving read() never touched rw2
		// despite a whole packet waiting there.
	}
}
