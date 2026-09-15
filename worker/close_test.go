package worker

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// Close used to close(worker.in) with an agent's work() goroutine possibly
// mid-send on it. The panic that follows does not kill the process -- work()
// recovers it, and "send on closed channel" satisfies the err.(error) there
// -- so the symptom is quieter: the packet is dropped and a useless error
// reaches ErrorHandler.

// closeErrors collects what an ErrorHandler received; every blocked sender
// can report at once.
type closeErrors struct {
	mu   sync.Mutex
	errs []error
}

func (c *closeErrors) add(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs = append(c.errs, err)
}

func (c *closeErrors) snapshot() []error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]error(nil), c.errs...)
}

// Nothing drains worker.in here -- Work() is never started -- so senders
// past the eighth block on the send itself. No race to win: a send is
// genuinely in flight when Close runs.
func TestCloseDoesNotPanicWithASendInFlight(t *testing.T) {
	const agents = queueSize + 5
	s := newFakeWorkerServer(t)
	s.SetRecord(false)

	var collected closeErrors
	w := New(Unlimited)
	w.ErrorHandler = collected.add

	for i := 0; i < agents; i++ {
		if err := w.AddServer(Network, s.Addr()); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.AddFunc("bench", func(job Job) ([]byte, error) {
		return nil, nil
	}, 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Ready(); err != nil {
		t.Fatal(err)
	}

	// Repeated because a client Dial returns before the server's accept loop
	// has registered that connection.
	for i := 0; i < 5; i++ {
		s.Serve(0)
		time.Sleep(50 * time.Millisecond)
	}
	// Let every decode-and-send actually land or block.
	time.Sleep(200 * time.Millisecond)

	w.Lock()
	w.running = true
	w.Unlock()

	// Bounded: nothing here may hang the binary.
	done := make(chan struct{})
	go func() {
		w.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return within 5s")
	}
	// The goroutines Close unblocked still need a moment to report.
	time.Sleep(200 * time.Millisecond)

	for _, err := range collected.snapshot() {
		if strings.Contains(err.Error(), "closed channel") {
			t.Errorf("Close reported %q: a send raced the close of worker.in", err)
		}
	}
}

// Close used to run only under "if worker.running", which only Work() sets,
// so the startup-failure unwind -- Ready() dialed every agent, a later step
// failed, deferred Close ran before go w.Work() -- closed nothing.
func TestCloseBeforeWorkClosesAgents(t *testing.T) {
	s := newFakeWorkerServer(t)

	w := New(Unlimited)
	w.ErrorHandler = func(error) {}
	if err := w.AddServer(Network, s.Addr()); err != nil {
		t.Fatal(err)
	}
	if err := w.AddFunc("bench", func(job Job) ([]byte, error) {
		return nil, nil
	}, 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Ready(); err != nil {
		t.Fatal(err)
	}
	// Work() deliberately never called.

	done := make(chan struct{})
	go func() {
		w.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return within 2s")
	}

	w.Lock()
	agents := append([]*agent(nil), w.agents...)
	w.Unlock()
	for i, a := range agents {
		a.Lock()
		conn := a.conn
		a.Unlock()
		if conn != nil {
			t.Errorf("agent %d: conn still open after Close before Work", i)
		}
	}
}

// Pins a deliberate choice: Work's quit case drains what is already
// buffered, as the old range did, rather than leaving each item to the
// select's coin flip between two ready cases.
func TestCloseDrainsAlreadyQueuedPackets(t *testing.T) {
	w := New(Unlimited)
	w.ErrorHandler = func(error) {}

	var mu sync.Mutex
	handled := 0
	w.JobHandler = func(Job) error {
		mu.Lock()
		handled++
		mu.Unlock()
		return nil
	}

	// Without ready set, Work() calls Ready() and panics on ErrNoneAgents.
	w.Lock()
	w.ready = true
	w.running = true
	w.Unlock()

	workReturned := make(chan struct{})
	go func() {
		w.Work()
		close(workReturned)
	}()

	const n = queueSize
	for i := 0; i < n; i++ {
		w.in <- &inPack{dataType: dtEchoRes}
	}
	// Best effort: park Work in its select rather than mid-dispatch. The
	// drain does not depend on it.
	time.Sleep(50 * time.Millisecond)

	w.Close()

	select {
	case <-workReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("Work did not return within 2s of Close")
	}

	mu.Lock()
	defer mu.Unlock()
	if handled != n {
		t.Errorf("handled %d packets, want all %d queued before Close", handled, n)
	}
}
