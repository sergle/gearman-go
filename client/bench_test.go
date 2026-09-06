package client

import (
	"sync"
	"testing"
	"time"
)

// Benchmarks for the client's request paths, driven by the in-process fake job
// server (fakeserver_test.go) over a real loopback socket. No gearmand.
//
// They exist to give the fixes in docs/ai/liveness_fix.md a *before* number.
// Two things that plan changes are measurable here:
//
//   - Status and Echo grow a per-call timeout. Today they park on a
//     two-operation sync.Mutex latch and allocate nothing for it; a
//     time.After per call would show up in ReportAllocs, and time.After keeps
//     its timer alive for the whole ResponseTimeout even when the response
//     arrives immediately. Compare BenchmarkClientEcho / BenchmarkClientStatus
//     across the fix; if the allocation shows, use time.NewTimer with a Stop
//     instead of copying do()'s time.After.
//
//   - Do and Echo stop overlapping, because both will hold client.Mutex for
//     the whole round trip. BenchmarkClientDoBgParallel measures contention on
//     the submit path alone; BenchmarkClientMixedDoAndEcho is the mixed case,
//     and today it does not finish at all (see its comment).
//
// Every benchmark runs the loopback socket and one client goroutine pair, so
// the numbers are dominated by syscalls and scheduling, not by the client's
// arithmetic. That is fine: the questions above are about serialisation and
// allocation per call, both of which survive that noise.
//
// Run with: make bench

// benchPayload is small on purpose: a large body would measure the kernel
// rather than the client. BenchmarkClientDoBgLargePayload covers the other end.
var benchPayload = []byte("payload")

// newBenchServer starts a fake job server with recording off -- see SetRecord.
func newBenchServer(b *testing.B) *fakeJobServer {
	b.Helper()
	s := newFakeJobServer(b)
	s.SetRecord(false)
	return s
}

// BenchmarkClientDoBg is the cheapest complete round trip the client has:
// SUBMIT_JOB_BG out, JOB_CREATED back, no completion to wait for. It measures
// do()'s whole path -- the "c" handler slot, the mutex held across the round
// trip, and the ResponseTimeout timer.
func BenchmarkClientDoBg(b *testing.B) {
	s := newBenchServer(b)
	c := newTestClient(b, s)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.DoBg("f", benchPayload, JobNormal); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkClientDo adds the part DoBg skips: the caller's ResponseHandler,
// dispatched from processLoop when WORK_COMPLETE arrives. Do returns at
// JOB_CREATED, so waiting for the callback is what makes this the full path.
func BenchmarkClientDo(b *testing.B) {
	s := newBenchServer(b)
	c := newTestClient(b, s)
	done := make(chan struct{}, 1)
	h := func(*Response) { done <- struct{}{} }

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Do("f", benchPayload, JobNormal, h); err != nil {
			b.Fatal(err)
		}
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			b.Fatal("no WORK_COMPLETE delivered")
		}
	}
	b.StopTimer()
}

// BenchmarkClientDoBgLargePayload keeps the request path but makes the body
// bigger than the client's 1KB read buffer, so readLoop's leftdata re-framing
// runs on every iteration.
func BenchmarkClientDoBgLargePayload(b *testing.B) {
	s := newBenchServer(b)
	c := newTestClient(b, s)
	payload := make([]byte, 32*1024)

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.DoBg("f", payload, JobNormal); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkClientEcho is one of the two calls docs/ai/liveness_fix.md rewrites.
// Baseline: the sync.Mutex latch, no timer, no lock around the write.
func BenchmarkClientEcho(b *testing.B) {
	s := newBenchServer(b)
	c := newTestClient(b, s)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Echo(benchPayload); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkClientStatus is the other one. Same baseline, plus the STATUS_RES
// parse.
func BenchmarkClientStatus(b *testing.B) {
	s := newBenchServer(b)
	c := newTestClient(b, s)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Status(benchHandle); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkClientDoBgParallel submits from GOMAXPROCS goroutines at once. do()
// already holds client.Mutex for the whole round trip, so this is a
// serialisation number, not a throughput one -- and it is the number to
// re-measure after Status and Echo join that same lock.
func BenchmarkClientDoBgParallel(b *testing.B) {
	s := newBenchServer(b)
	c := newTestClient(b, s)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := c.DoBg("f", benchPayload, JobNormal); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
}

// BenchmarkClientMixedDoAndEcho submits and echoes concurrently.
//
// It does not produce a number today, and that is the measurement: Echo writes
// to the shared bufio.Writer without holding client.Mutex (todo.md section 4),
// so its bytes interleave with a concurrent submit and destroy the framing;
// then Echo waits on a latch with no timeout (section 5) for a response that
// can never arrive. The watchdog below turns that into a failed benchmark
// rather than a wedged `make bench`.
//
// After the fix it becomes the throughput number for the serialisation that
// fix introduces -- Do and Echo can no longer overlap at all.
func BenchmarkClientMixedDoAndEcho(b *testing.B) {
	s := newBenchServer(b)
	c := newTestClient(b, s)

	// Each half runs b.N/2 operations so the reported per-op cost stays
	// comparable with the single-path benchmarks above.
	half := b.N / 2
	if half == 0 {
		half = 1
	}

	errc := make(chan error, 2)
	done := make(chan struct{})

	b.ReportAllocs()
	b.ResetTimer()
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < half; i++ {
				if _, err := c.DoBg("f", benchPayload, JobNormal); err != nil {
					errc <- err
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < half; i++ {
				if _, err := c.Echo(benchPayload); err != nil {
					errc <- err
					return
				}
			}
		}()
		wg.Wait()
	}()

	// The goroutines are left running on timeout: they are blocked on a defect
	// that has no way to unblock them. Same bargain as knownbugs_test.go's
	// mustReturnWithin.
	//
	// The deadline scales with b.N so -benchtime can be raised, with a floor
	// that keeps the wedged case cheap: the framing breaks within the first
	// few operations, so waiting longer than this only slows `make bench` down
	// once per -count.
	//
	// The ceiling matters as much as the floor. Under -benchtime=10s, b.N runs
	// to six figures, and an uncapped deadline would outlast the go test
	// -timeout: the binary would panic instead of failing here with an
	// explanation.
	deadline := time.Duration(b.N) * time.Millisecond
	if deadline < 5*time.Second {
		deadline = 5 * time.Second
	}
	if deadline > 30*time.Second {
		deadline = 30 * time.Second
	}
	select {
	case <-done:
		b.StopTimer()
		select {
		case err := <-errc:
			b.Fatal(err)
		default:
		}
	case <-time.After(deadline):
		b.StopTimer()
		b.Fatal("wedged: interleaved writes lost the framing and Echo has no timeout" +
			" (todo.md sections 4 and 5) -- no baseline number is available before the fix")
	}
}
