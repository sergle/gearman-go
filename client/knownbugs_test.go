package client

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// Tests for defects that are known and not yet fixed. They describe the
// behaviour the client *should* have, so they fail against the code as it
// stands — that is the point. They are gated behind -knownbugs so the default
// run stays a usable signal:
//
//	go test ./client              # green; these are skipped
//	go test ./client -knownbugs   # the outstanding defects, red
//
// The flag must come *after* the package list. `go test -knownbugs ./client`
// silently tests the current directory instead and exits 0 — go passes the
// unrecognised flag and everything after it to the test binary. The same trap
// applies to this package's existing -integration flag.
//
// When a fix phase lands, its test starts passing and its gate comes off, so
// it joins the default suite as a regression test.
//
// Every one of these calls something that can block forever today, so nothing
// here is allowed to call a client method directly on the test goroutine.
// mustReturnWithin runs it in a goroutine behind a deadline: a hanging defect
// then fails one test instead of wedging the whole package until the binary's
// 10-minute timeout kills it and takes every other result with it.

func requireKnownBugs(t *testing.T) {
	t.Helper()
	if !runKnownBugTests {
		t.Skip("known unfixed defect; run with: go test ./client -knownbugs")
	}
}

// mustReturnWithin calls fn on its own goroutine and fails if it has not
// returned within d. The goroutine is left blocked on purpose — it belongs to a
// defect that has no way to unblock it.
func mustReturnWithin(t *testing.T, d time.Duration, what string, fn func() error) error {
	t.Helper()
	errc := make(chan error, 1)
	go func() { errc <- fn() }()
	select {
	case err := <-errc:
		return err
	case <-time.After(d):
		t.Fatalf("%s did not return within %s", what, d)
		return nil
	}
}

// todo.md section 4: Status and Echo call client.write without holding
// client.Mutex, so they scribble into the same bufio.Writer as a concurrent
// do(). Fails under -race with writes colliding at client.go:94 (do -> write)
// and client.go:309 (Echo).
//
// Only one Echo is in flight at a time here, to keep this about the write race
// rather than the handler clobbering covered below.
//
// Fixed by: Phase 5 (hold client.Mutex around the Status and Echo writes).
func TestConcurrentSubmitAndEchoDoNotCorruptTheStream(t *testing.T) {
	requireKnownBugs(t)

	s := newFakeServer(t)
	c := newTestClient(t, s)
	c.ResponseTimeout = 200 * time.Millisecond

	const n = 20
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { // submits, serialised among themselves by do()'s lock
			defer wg.Done()
			for i := 0; i < n; i++ {
				c.DoBgWithId("f", []byte("payload"), JobNormal, fmt.Sprintf("id-%d", i))
			}
		}()
		go func() { // echoes, one at a time, taking no lock at all
			defer wg.Done()
			for i := 0; i < n; i++ {
				c.Echo([]byte("ping"))
			}
		}()
		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("interleaved writes wedged the connection")
	}

	// Whatever arrived must be well-formed: interleaved writes corrupt the
	// framing, so a garbled body is the defect showing up without -race.
	for i, req := range s.Requests() {
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

// todo.md section 5: Echo blocks forever when no response arrives. It uses a
// locked-twice sync.Mutex as a latch with no timeout, unlike do(), which has
// ResponseTimeout.
//
// Fixed by: Phase 5 (chan + ResponseTimeout select, removing the inner handler
// on timeout).
func TestEchoTimesOutWhenServerNeverAnswers(t *testing.T) {
	requireKnownBugs(t)

	s := newFakeServer(t)
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

// todo.md section 5, the same defect in Status.
//
// Fixed by: Phase 5.
func TestStatusTimesOutWhenServerNeverAnswers(t *testing.T) {
	requireKnownBugs(t)

	s := newFakeServer(t)
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

// todo.md section 6: Pool.Do takes the embedded Client.Mutex (pool.go:96) and
// then calls Client.Do, which reaches do() and takes the same non-reentrant
// mutex again. Every Pool.Do call deadlocks on the first attempt.
//
// Fixed by: Phase 6 (drop the outer Lock; Client serialises itself).
func TestPoolDoDoesNotDeadlock(t *testing.T) {
	requireKnownBugs(t)

	s := newFakeServer(t)
	p := NewPool()
	if err := p.Add(Network, s.Addr(), 1); err != nil {
		t.Fatal(err)
	}
	// No t.Cleanup(p.Close) on purpose: Pool.Close calls Client.Close, which
	// takes the same mutex the deadlocked Pool.Do goroutine is still holding,
	// so cleaning up would hang the test binary instead of the one test. The
	// fake server's own cleanup closes the sockets.

	err := mustReturnWithin(t, 2*time.Second, "Pool.Do", func() error {
		_, _, err := p.Do("f", []byte("payload"), JobNormal, nil)
		return err
	})
	if err != nil {
		t.Errorf("Pool.Do err = %v, want nil", err)
	}
}

// todo.md section 6, the same deadlock in Pool.DoBg (pool.go:105).
//
// Fixed by: Phase 6.
func TestPoolDoBgDoesNotDeadlock(t *testing.T) {
	requireKnownBugs(t)

	s := newFakeServer(t)
	p := NewPool()
	if err := p.Add(Network, s.Addr(), 1); err != nil {
		t.Fatal(err)
	}
	// See TestPoolDoDoesNotDeadlock: no p.Close() cleanup, for the same reason.

	err := mustReturnWithin(t, 2*time.Second, "Pool.DoBg", func() error {
		_, _, err := p.DoBg("f", []byte("payload"), JobNormal)
		return err
	})
	if err != nil {
		t.Errorf("Pool.DoBg err = %v, want nil", err)
	}
}

// todo.md section 6: Pool.selectServer (pool.go:157-166) spins forever when the
// pool is empty. SelectWithRate returns pool.last ("") with nothing to choose
// from, the map lookup misses, and `for client == nil` goes round again. Pool
// already defines ErrNotFound for exactly this case.
//
// Reached in practice by `make integration` with no gearmand: every Pool.Add
// fails, so the pool is empty by the time a Pool.Echo runs.
//
// NOTE: unlike the other tests here, the goroutine this leaks is *runnable*,
// not blocked — it burns a core for the rest of the test binary's life. That is
// the defect, and the reason this test is gated rather than run by default.
//
// Fixed by: Phase 6 (return ErrNotFound instead of looping).
func TestPoolOnEmptyPoolReturnsNotFound(t *testing.T) {
	requireKnownBugs(t)

	p := NewPool()
	err := mustReturnWithin(t, 2*time.Second, "Pool.Echo on an empty pool", func() error {
		_, err := p.Echo("", []byte("ping"))
		return err
	})
	if err != ErrNotFound {
		t.Errorf("Pool.Echo on an empty pool = %v, want ErrNotFound", err)
	}
}
