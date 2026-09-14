package worker

import (
	"bufio"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// worker.funcs used to race between exec and AddFunc/RemoveFunc/Reset, a
// concurrent map read/write -- a runtime throw, so a failure here can kill the
// binary rather than fail the test. exec is called directly: the map access
// does not need the network, and going through a fake server would not overlap
// the churn goroutine as tightly.

// discardAgent wires an *agent to a drained net.Pipe so exec's inpack.a.Write
// has somewhere to go; a zero *bufio.ReadWriter panics on the first Write.
func discardAgent(t *testing.T, w *Worker) *agent {
	t.Helper()
	local, remote := net.Pipe()
	t.Cleanup(func() {
		local.Close()
		remote.Close()
	})
	go io.Copy(io.Discard, remote)
	return &agent{
		conn:   local,
		rw:     bufio.NewReadWriter(bufio.NewReader(local), bufio.NewWriter(local)),
		worker: w,
	}
}

func TestExecDoesNotRaceAddFuncRemoveFuncReset(t *testing.T) {
	w := New(Unlimited)
	w.ErrorHandler = func(error) {} // "function does not exist", expected while Reset/RemoveFunc churn

	bench := func(job Job) ([]byte, error) { return nil, nil }
	if err := w.AddFunc("bench", bench, 0); err != nil {
		t.Fatal(err)
	}

	a := discardAgent(t, w)

	// exec's write branch is gated on running; set it directly rather than via
	// Work(), which needs a live agent loop feeding worker.in.
	w.Lock()
	w.running = true
	w.Unlock()

	stop := make(chan struct{})
	var churn sync.WaitGroup
	churn.Add(1)
	go func() {
		defer churn.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			w.RemoveFunc("bench")
			w.AddFunc("bench", bench, 0)
			w.Reset()
			w.AddFunc("bench", bench, 0)
		}
	}()

	const jobs = 3000
	var wg sync.WaitGroup
	wg.Add(jobs)
	for i := 0; i < jobs; i++ {
		go func() {
			defer wg.Done()
			inpack := &inPack{a: a, fn: "bench", handle: "H:test:1"}
			_ = w.exec(inpack)
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("exec goroutines did not finish within 20s")
	}

	close(stop)
	churn.Wait()
}
