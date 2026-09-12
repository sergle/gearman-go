package client

import (
	"sync"
	"testing"
	"time"
)

// Benchmarks for the client's request paths, driven by the in-process fake job
// server (fakeserver_test.go) over a real loopback socket. No gearmand.
//
// They were written for a *before* number on the Status/Echo locking-and-timeout
// change, and still watch two of its effects:
//
//   - Status and Echo carry a per-call timeout. They used to park on a
//     sync.Mutex latch, allocating nothing; each now allocates a result
//     channel, a closure and a timer. Both timer constructors allocate one
//     Timer, so ReportAllocs cannot tell them apart -- they use NewTimer with
//     a Stop because an unstopped time.After timer stays live for the whole
//     ResponseTimeout after a round trip of microseconds. do() still uses
//     time.After.
//
//   - Do and Echo no longer overlap: both hold client.Mutex for the whole
//     round trip. DoBgParallel measures contention on the submit path alone;
//     MixedDoAndEcho is the mixed case, and used not to finish (see it).
//
// Every benchmark runs a loopback socket and one client goroutine pair, so the
// numbers are dominated by syscalls and scheduling, not the client's
// arithmetic. Fine: the questions above are serialisation and allocation per
// call, both of which survive that noise.
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
// large enough that encoding and writing it dominate the round trip.
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

// BenchmarkClientEcho is one of the two calls the locking-and-timeout change
// rewrote. Its recorded baseline used the old shape: a sync.Mutex latch, no
// timer, no lock around the write.
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

// BenchmarkClientStatus is the other one. Same baseline shape, plus the
// STATUS_RES parse.
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
// holds client.Mutex for the whole round trip, so this is a serialisation
// number, not a throughput one -- and since Status and Echo joined that lock,
// the one every path shares.
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
// It used not to produce a number at all, and that was the measurement: Echo
// wrote to the shared bufio.Writer unlocked, interleaving its bytes with a
// concurrent submit and destroying the framing, then waited on a latch with no
// timeout for a response that could never arrive. The watchdog below fired.
//
// Both fixed, so it now measures the serialisation that fix introduced -- Do
// and Echo no longer overlap. The watchdog stays: it turns a regression into a
// failed benchmark rather than a wedged `make bench`.
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
		b.Fatal("wedged: interleaved writes lost the framing, or Echo is waiting" +
			" on a response with no timeout -- both were fixed, so this is a regression")
	}
}
