package worker

import (
	"errors"
	"testing"
	"time"
)

// disconnect_error used to call worker.err while holding a.Mutex, which both
// documented handler moves -- .Reconnect() and w.Close() -- re-enter, wedging
// the agent's lock forever. That is a deadlock rather than a race, so -race
// will not catch it and each case needs a deadline: calling disconnect_error
// on the test goroutine would just hang.

// callWithin runs fn on its own goroutine and fails the test if it has not
// returned within d.
func callWithin(t *testing.T, d time.Duration, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s did not return within %s", what, d)
	}
}

func TestDisconnectErrorHandlerCanReconnect(t *testing.T) {
	w := New(Unlimited)
	a := discardAgent(t, w)
	w.SetErrorHandler(func(err error) {
		de, ok := err.(*WorkerDisconnectError)
		if !ok {
			t.Errorf("handler got %T, want *WorkerDisconnectError", err)
			return
		}
		// The dial fails immediately (no net/addr set); only whether this
		// call returns at all is under test.
		de.Reconnect()
	})

	callWithin(t, 2*time.Second, "disconnect_error with a Reconnect handler", func() {
		a.disconnect_error(errors.New("connection lost"))
	})
}

func TestDisconnectErrorHandlerCanClose(t *testing.T) {
	w := New(Unlimited)
	a := discardAgent(t, w)
	// Close's shutdown branch is gated on running and only closes what's in
	// agents; set both directly rather than via Ready/Work, which need a live
	// agent loop.
	w.Lock()
	w.agents = []*agent{a}
	w.running = true
	w.Unlock()
	w.SetErrorHandler(func(err error) {
		w.Close()
	})

	callWithin(t, 2*time.Second, "disconnect_error with a Close handler", func() {
		a.disconnect_error(errors.New("connection lost"))
	})
}
