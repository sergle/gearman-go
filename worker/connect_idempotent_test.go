package worker

import (
	"testing"
	"time"
)

// TestReadyTwiceDoesNotDuplicateConnection: agent.Connect had no
// "already connected" guard, so a retried Ready() -- a partial failure on one
// agent among several, or Work()'s own auto-Ready() after an explicit one that
// errored -- dialed a second connection per already-connected agent and left
// the first work() goroutine blocked reading it. worker_test.go's
// TestLargeDataWork calls Ready() twice for exactly this reason.
//
// The AcceptedConns() check below happens before Work()/Serve() run anything:
// on a regression there are two live connections and two work() goroutines,
// each re-reading the *field* a.rw on every loop iteration (see
// connlock_test.go's TestWorkRedialRacesConcurrentWrite for the same field's
// other defect), so once one of them observes the other's connection the
// stream desyncs instead of failing cleanly. Stopping here avoids depending on
// that corruption resolving into a clean test failure.
func TestReadyTwiceDoesNotDuplicateConnection(t *testing.T) {
	s := newFakeWorkerServer(t)
	s.SetJob("bench", []byte("payload"))

	w := New(Unlimited)
	w.ErrorHandler = func(error) {}
	if err := w.AddServer(Network, s.Addr()); err != nil {
		t.Fatal(err)
	}
	if err := w.AddFunc("bench", func(job Job) ([]byte, error) {
		return job.Data(), nil
	}, 0); err != nil {
		t.Fatal(err)
	}

	if err := w.Ready(); err != nil {
		t.Fatal(err)
	}
	// The retry a partial Ready() failure produces.
	if err := w.Ready(); err != nil {
		t.Fatal(err)
	}

	// A dial returning does not mean the server's Accept has registered it
	// yet.
	deadline := time.Now().Add(2 * time.Second)
	for s.AcceptedConns() < 1 {
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if got := s.AcceptedConns(); got != 1 {
		t.Fatalf("server accepted %d connections for one agent across two Ready() calls, want 1", got)
	}

	go w.Work()
	defer w.Close()

	const n = 3
	done := s.Serve(n)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		assigned, completed := s.Counts()
		t.Fatalf("stalled after Ready() called twice: assigned %d, completed %d", assigned, completed)
	}
	if errs := s.FramingErrors(); len(errs) != 0 {
		t.Fatalf("server could not frame %d packet(s) from the worker; first: %s", len(errs), errs[0])
	}
	if assigned, completed := s.Counts(); completed != n {
		t.Fatalf("completed %d of %d jobs (assigned %d)", completed, n, assigned)
	}
}
