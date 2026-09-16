package worker

import (
	"testing"
	"time"
)

// SetErrorHandler/SetJobHandler replaced exported fields that err() and
// customeHandler read from every agent's work() goroutine.

// The case the plain field did handle, and the setter must keep handling:
// installed before Ready, still firing afterwards.
func TestErrorHandlerSetBeforeReadyStillFires(t *testing.T) {
	s := newFakeWorkerServer(t)

	w := New(Unlimited)
	errs := make(chan error, 1)
	w.SetErrorHandler(func(e error) {
		select {
		case errs <- e:
		default:
		}
	})
	if err := w.AddServer(Network, s.Addr()); err != nil {
		t.Fatal(err)
	}
	if err := w.AddFunc("bench", func(job Job) ([]byte, error) {
		return nil, ErrUnknown
	}, 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Ready(); err != nil {
		t.Fatal(err)
	}
	go w.Work()
	defer w.Close()

	s.Serve(1)

	select {
	case err := <-errs:
		if err != ErrUnknown {
			t.Errorf("ErrorHandler got %v, want %v", err, ErrUnknown)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ErrorHandler installed before Ready() never fired")
	}
}

// TestJobHandlerSetBeforeReadyStillFires is JobHandler's half of the same
// pin, using ECHO_RES -- customeHandler's route for anything that is not a
// job dispatch -- since JobHandler is never consulted for an ordinary
// JOB_ASSIGN.
func TestJobHandlerSetBeforeReadyStillFires(t *testing.T) {
	s := newFakeWorkerServer(t)

	w := New(Unlimited)
	echoes := make(chan []byte, 1)
	w.SetJobHandler(func(job Job) error {
		select {
		case echoes <- job.Data():
		default:
		}
		return nil
	})
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
	go w.Work()
	defer w.Close()

	w.Echo([]byte("ping"))

	select {
	case data := <-echoes:
		if string(data) != "ping" {
			t.Errorf("JobHandler saw %q, want %q", data, "ping")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("JobHandler installed before Ready() never fired")
	}
}

// TestHandlerSettersAreRaceFreeWhileRunning is the -race regression: with
// the plain fields, this raced customeHandler/err reading them from every
// agent's work() goroutine against a caller reassigning them. Both setters
// go through atomic.Pointer now, so reassigning while jobs and echoes are
// flowing must stay clean under -race.
func TestHandlerSettersAreRaceFreeWhileRunning(t *testing.T) {
	s := newFakeWorkerServer(t)
	s.SetRecord(false)

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
	if err := w.Ready(); err != nil {
		t.Fatal(err)
	}
	go w.Work()
	defer w.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			w.SetErrorHandler(func(error) {})
			w.SetJobHandler(func(Job) error { return nil })
		}
	}()

	deadline := s.Serve(200)
	select {
	case <-deadline:
	case <-time.After(5 * time.Second):
		t.Fatal("jobs did not complete within 5s")
	}
	<-done
}
