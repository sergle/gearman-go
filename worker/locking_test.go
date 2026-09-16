package worker

import (
	"bufio"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

// reconnect used to call into the worker while holding a.Mutex, taking
// a.Mutex then worker.Mutex, while AddFunc/RemoveFunc hold worker.Mutex and
// then write through the agent -- ABBA. Two goroutines wedge holding one lock
// each, so this fails on a deadline rather than hanging the binary.
//
// A fresh agent per round, dialed here rather than through Connect: Connect
// starts a work() goroutine that would consume the connection out from under
// this test's own direct a.reconnect() calls, reporting a grab/read race
// unrelated to the lock order under test here.

// lockOrderAgent returns an agent connected to srv, with no read loop running.
// It reports errors rather than failing the test: its callers run off the test
// goroutine, where t.Fatal is not allowed.
func lockOrderAgent(w *Worker, srv *fakeWorkerServer) (a *agent, err error) {
	if a, err = newAgent("tcp", srv.Addr(), w); err != nil {
		return nil, err
	}
	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		return nil, err
	}
	a.conn = conn
	a.rw = bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	return a, nil
}

func TestReconnectDoesNotDeadlockAgainstAddFunc(t *testing.T) {
	const lockOrderRounds = 300

	srv := newFakeWorkerServer(t)
	w := New(Unlimited)
	if err := w.AddFunc("lockorder-base", func(job Job) ([]byte, error) {
		return nil, nil
	}, 0); err != nil {
		t.Fatal(err)
	}
	// AddFunc/RemoveFunc only reach the agent when the worker is running; set
	// it directly rather than going through Ready/Work, which would add an
	// agent loop this test does not need.
	w.Lock()
	w.running = true
	w.Unlock()

	// No deferred w.Close: on a regression AddFunc is wedged holding
	// worker.Mutex, and Close would block after the deadline fired.
	callWithin(t, 30*time.Second, "reconnect racing AddFunc/RemoveFunc", func() {
		for i := 0; i < lockOrderRounds; i++ {
			a, err := lockOrderAgent(w, srv)
			if err != nil {
				t.Errorf("connecting agent: %v", err)
				return
			}
			// The conn reconnect is about to replace; both are closed at the
			// end of the round so the loop does not run the process out of
			// descriptors.
			old := a.conn
			w.Lock()
			w.agents = []*agent{a}
			w.Unlock()

			name := "lockorder-" + strconv.Itoa(i)
			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				if err := a.reconnect(); err != nil {
					t.Errorf("reconnect: %v", err)
				}
			}()
			go func() {
				defer wg.Done()
				<-start
				if err := w.AddFunc(name, func(job Job) ([]byte, error) {
					return nil, nil
				}, 0); err != nil {
					t.Errorf("AddFunc: %v", err)
					return
				}
				if err := w.RemoveFunc(name); err != nil {
					t.Errorf("RemoveFunc: %v", err)
				}
			}()
			close(start)
			wg.Wait()
			a.Close()
			old.Close()
		}
	})

	w.Close()
}
