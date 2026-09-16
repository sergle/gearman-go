package worker

import (
	"testing"
	"time"
)

// SendData, SendWarning and UpdateStatus used to call the unlocked a.write, so
// a job function reporting progress pushed bytes into the agent's one
// bufio.Writer while the dispatcher was pushing its own -- Grab after every
// dispatch, PreSleep, exec's WORK_COMPLETE -- all of which take a.Mutex. The
// two interleave inside a single packet.
//
// The damage is on the wire rather than in the race detector, so the assertion
// is the fake server's framing check and this fails without -race. Framing is
// checked before completion: a desynced stream makes serve drop the
// connection, after which no further results arrive, and "no WORK_COMPLETE"
// would report the symptom instead of the cause.
func TestInPackWritersDoNotInterleaveWithTheDispatcher(t *testing.T) {
	const (
		progressJobs     = 60
		progressPerJob   = 120
		progressDeadline = 30 * time.Second
	)
	// Long enough that a packet sometimes exceeds what is left in the 4 KiB
	// bufio buffer, which takes write down its other path -- straight to the
	// connection rather than through the buffer.
	progressPayload := make([]byte, 200)

	s := newFakeWorkerServer(t)
	s.SetJob("progress", nil)

	w := newTestWorker(t, s, "progress", func(job Job) ([]byte, error) {
		for i := 0; i < progressPerJob; i++ {
			job.UpdateStatus(i, progressPerJob)
			job.SendData(progressPayload)
			job.SendWarning(progressPayload)
		}
		return nil, nil
	})
	// Unlike the other tests on this harness, stop the worker before the
	// server: a job goroutine still writing while stop closes the socket is
	// noise this test would otherwise have to tell apart from the defect.
	defer w.Close()

	// Nothing is asserted on the timeout -- the framing check below is the
	// test. A corrupt stream costs serve the connection, so the jobs never
	// finish; polling for the framing error rather than waiting the deadline
	// out is what keeps a failure sub-second instead of thirty of them.
	done := s.Serve(progressJobs)
	deadline := time.Now().Add(progressDeadline)
wait:
	for time.Now().Before(deadline) {
		select {
		case <-done:
			break wait
		case <-time.After(5 * time.Millisecond):
			if len(s.FramingErrors()) != 0 {
				break wait
			}
		}
	}

	if errs := s.FramingErrors(); len(errs) != 0 {
		t.Fatalf("server could not frame %d packet(s) from the worker; first: %s",
			len(errs), errs[0])
	}
	if assigned, completed := s.Counts(); completed != progressJobs {
		t.Fatalf("completed %d of %d jobs (assigned %d)", completed, progressJobs, assigned)
	}
}
