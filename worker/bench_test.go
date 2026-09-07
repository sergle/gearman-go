package worker

import (
	"testing"
	"time"
)

// Benchmarks for the worker's job pipeline, driven by the in-process fake job
// server (fakeserver_test.go) over a real loopback socket. No gearmand.
//
// One iteration is one complete job: GRAB_JOB_UNIQ out, JOB_ASSIGN_UNIQ back,
// agent.read reframes it, worker.in carries it, Work dispatches it, the job
// function runs on its own goroutine and WORK_COMPLETE goes back. The
// benchmark waits for the server to count b.N results, so nothing is measured
// half-finished.
//
// ns/op is therefore pipeline latency per job at a queue depth of one -- the
// worker grabs again as soon as it dispatches (worker.go:157), so there is
// exactly one assignment in flight. It is not a saturation number, and it is
// not meant to be: the Status/Echo locking and timeout work is all
// client-side, so this side exists as a *control*. If these move, the change
// leaked out of the client package.
//
// The allocation figures are per iteration of the benchmark loop, but the work
// happens on the worker's goroutines, so ReportAllocs here measures the
// benchmark goroutine only and is not useful. Use -benchmem output from the
// client benchmarks for allocation questions and these for throughput.
//
// Run with: make bench

// benchTimeout scales with the job count: a slow machine under -benchtime=10s
// still has to finish.
func benchTimeout(n int) time.Duration {
	d := time.Duration(n) * time.Millisecond
	if d < 30*time.Second {
		return 30 * time.Second
	}
	return d
}

// runJobs serves n jobs and waits for all n results.
func runJobs(b *testing.B, s *fakeWorkerServer, n int) {
	b.Helper()
	done := s.Serve(n)
	select {
	case <-done:
	case <-time.After(benchTimeout(n)):
		assigned, completed := s.Counts()
		b.Fatalf("pipeline stalled: %d/%d jobs done, %d assigned", completed, n, assigned)
	}
}

// BenchmarkWorkerJobPipeline is the baseline: an unlimited worker, a job
// function that does nothing, a small payload.
func BenchmarkWorkerJobPipeline(b *testing.B) {
	s := newFakeWorkerServer(b)
	s.SetRecord(false)
	newTestWorker(b, s, "bench", func(job Job) ([]byte, error) {
		return nil, nil
	})

	b.ResetTimer()
	runJobs(b, s, b.N)
	b.StopTimer()
}

// There is deliberately no large-payload variant here. Anything that makes a
// JOB_ASSIGN arrive in more than one read trips the framing defect in
// agent.read and the pipeline stalls, so such a benchmark
// would report a stall rather than a number. The defect has a deterministic
// reproducer instead: TestAgentReadReturnsWholePackets in knownbugs_test.go.

// BenchmarkWorkerJobPipelineEcho sends the payload back, so WORK_COMPLETE
// carries a body and the write path is exercised in both directions.
func BenchmarkWorkerJobPipelineEcho(b *testing.B) {
	s := newFakeWorkerServer(b)
	s.SetRecord(false)
	newTestWorker(b, s, "bench", func(job Job) ([]byte, error) {
		return job.Data(), nil
	})

	b.ResetTimer()
	runJobs(b, s, b.N)
	b.StopTimer()
}

// BenchmarkWorkerJobPipelineOneByOne measures the concurrency cap, not the
// pipeline: New(OneByOne) gives limit capacity 0, so Work blocks on
// `worker.limit <- true` until the previous exec's defer pops the token
// (worker.go:154-156 against worker.go:264-266). The gap against
// BenchmarkWorkerJobPipeline is the cost of that serialisation.
func BenchmarkWorkerJobPipelineOneByOne(b *testing.B) {
	s := newFakeWorkerServer(b)
	s.SetRecord(false)

	w := New(OneByOne)
	w.ErrorHandler = func(error) {}
	if err := w.AddServer(Network, s.Addr()); err != nil {
		b.Fatal(err)
	}
	if err := w.AddFunc("bench", func(job Job) ([]byte, error) { return nil, nil }, 0); err != nil {
		b.Fatal(err)
	}
	if err := w.Ready(); err != nil {
		b.Fatal(err)
	}
	go w.Work()

	b.ResetTimer()
	runJobs(b, s, b.N)
	b.StopTimer()
}
