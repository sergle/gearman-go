package worker

import (
	"sync"
	"testing"
)

// TestReadyFlagDoesNotRaceWork is the default-suite regression for the
// unsynchronised ready flag: the
// integration-gated TestWorkWithoutReady (worker_test.go) already covers the
// unsynchronised worker.ready field against a live gearmand under
// -race -integration; this exercises the same write (Ready) against the same
// read (isReady, what Work's own check goes through) without needing a job
// server, so the regression is caught by `go test -race ./worker` alone.
func TestReadyFlagDoesNotRaceWork(t *testing.T) {
	s := newFakeWorkerServer(t)
	w := New(Unlimited)
	w.SetErrorHandler(func(error) {})
	if err := w.AddServer(Network, s.Addr()); err != nil {
		t.Fatal(err)
	}
	if err := w.AddFunc("bench", func(job Job) ([]byte, error) {
		return nil, nil
	}, 0); err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	stop := make(chan struct{})
	var poll sync.WaitGroup
	poll.Add(1)
	go func() {
		defer poll.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = w.isReady()
		}
	}()

	if err := w.Ready(); err != nil {
		t.Fatal(err)
	}
	close(stop)
	poll.Wait()

	if !w.isReady() {
		t.Error("isReady() = false after Ready() returned")
	}
}
