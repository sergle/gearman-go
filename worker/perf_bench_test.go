package worker

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"strconv"
	"strings"
	"testing"
)

// Socket-free microbenchmarks for the worker's hot path, and the fixture test
// that keeps them honest. The pipeline benchmarks in bench_test.go drive a real
// loopback socket, so every one of them is dominated by the round trip and
// blind to per-packet allocation changes. This file is ./worker's equivalent of
// client/microbench_test.go.
//
// allocs/op and B/op here come from exact Mallocs/TotalAlloc deltas divided by
// N, so they are right at any -benchtime. Only ns/op needs iterations to
// settle.
//
// Everything in this file is prefixed perf/Worker: the package shares mutable
// state between tests and depends on declaration order (CLAUDE.md), so nothing
// here reuses an existing identifier or touches one.

// --- sinks ------------------------------------------------------------------
//
// Results are parked in package-level sinks so escape analysis cannot delete
// the work being measured.

var (
	perfSinkBytes []byte
	perfSinkIn    *inPack
	perfSinkOut   *outPack
	perfSinkErr   error
)

// --- fixtures ---------------------------------------------------------------

const (
	perfHandle = "H:fake:1"
	perfFn     = "bench"
	perfUniq   = "uniq-1"
)

var (
	perfSmallPayload = []byte("payload")
	// 32 KiB, matching client/microbench_test.go's large cases so the two
	// packages' figures are comparable. The whole ./worker benchmark set runs
	// in ~10s at make bench's defaults (2000x x count 10), large cases
	// included -- 20 000 iterations of a ~12us case is 0.24s, not a reason to
	// shrink it.
	perfLargePayload = bytes.Repeat([]byte("x"), 32<<10)
)

// perfResPacket frames one server->worker (\x00RES) packet.
func perfResPacket(dataType uint32, body []byte) []byte {
	buf := make([]byte, minPacketLength+len(body))
	copy(buf[:4], resStr)
	binary.BigEndian.PutUint32(buf[4:8], dataType)
	binary.BigEndian.PutUint32(buf[8:minPacketLength], uint32(len(body)))
	copy(buf[minPacketLength:], body)
	return buf
}

// perfAssignBody builds a JOB_ASSIGN(_UNIQ) body. The field count is not
// cosmetic: decodeInPack has no else branch on its len(s) == 3 / len(s) == 4
// guards (inpack.go:108,115), so a body with the wrong number of NUL-separated
// fields decodes with no error and an empty fn -- a strictly cheaper path than
// the one being measured. TestWorkerPerfFixtures asserts the decoded values.
func perfAssignBody(uniq bool, data []byte) []byte {
	parts := []string{perfHandle, perfFn}
	if uniq {
		parts = append(parts, perfUniq)
	}
	return append([]byte(strings.Join(parts, "\x00")+"\x00"), data...)
}

// --- agents over memory, not sockets ----------------------------------------

// perfCycleReader replays one packet forever. A net.Pipe or a loopback socket
// would put a writer goroutine's allocations and its scheduling inside the
// measurement; this reader allocates nothing after construction.
type perfCycleReader struct {
	buf []byte
	off int
}

func (r *perfCycleReader) Read(p []byte) (int, error) {
	if r.off >= len(r.buf) {
		r.off = 0
	}
	n := copy(p, r.buf[r.off:])
	r.off += n
	return n, nil
}

// perfReadAgent is an agent whose reader replays pkt and whose writer discards.
func perfReadAgent(pkt []byte) *agent {
	r := &perfCycleReader{buf: pkt}
	return &agent{
		rw: bufio.NewReadWriter(bufio.NewReader(r), bufio.NewWriter(io.Discard)),
	}
}

// perfWriteAgent is an agent whose writer discards and whose reader is empty.
func perfWriteAgent() *agent {
	return &agent{
		rw: bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(nil)), bufio.NewWriter(io.Discard)),
	}
}

// --- decode -----------------------------------------------------------------

// BenchmarkWorkerDecodeInPack prices one packet per iteration. BenchmarkDecode
// (inpack_test.go:64) decodes four cases per iteration and converts each from a
// string inside the loop, so its figures are a sum over four packets plus four
// conversions and cannot be attributed to a packet type.
func BenchmarkWorkerDecodeInPack(b *testing.B) {
	cases := []struct {
		name string
		pkt  []byte
	}{
		{"JobAssignUniq", perfResPacket(dtJobAssignUniq, perfAssignBody(true, perfSmallPayload))},
		{"JobAssign", perfResPacket(dtJobAssign, perfAssignBody(false, perfSmallPayload))},
		{"JobAssignUniqLarge", perfResPacket(dtJobAssignUniq, perfAssignBody(true, perfLargePayload))},
		{"NoJob", perfResPacket(dtNoJob, nil)},
		{"Noop", perfResPacket(dtNoop, nil)},
		{"EchoRes", perfResPacket(dtEchoRes, perfSmallPayload)},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				perfSinkIn, _, perfSinkErr = decodeInPack(c.pkt)
			}
		})
	}
}

// --- encode -----------------------------------------------------------------

// BenchmarkWorkerEncodeOutPack prices one Encode per iteration, excluding
// getOutPack (measured separately) so the buffer is the only variable.
func BenchmarkWorkerEncodeOutPack(b *testing.B) {
	cases := []struct {
		name string
		pack *outPack
	}{
		{"GrabJobUniq", &outPack{dataType: dtGrabJobUniq}},
		{"WorkComplete", &outPack{dataType: dtWorkComplete, handle: perfHandle, data: perfSmallPayload}},
		{"WorkCompleteLarge", &outPack{dataType: dtWorkComplete, handle: perfHandle, data: perfLargePayload}},
		{"WorkFail", &outPack{dataType: dtWorkFail, handle: perfHandle}},
		{"CanDo", &outPack{dataType: dtCanDo, data: []byte(perfFn)}},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				perfSinkBytes = c.pack.Encode()
			}
		})
	}
}

// --- framing ----------------------------------------------------------------

// BenchmarkWorkerAgentRead prices agent.read's framing: two io.ReadFulls and
// the one getBuffer that holds header and body together (agent.go:192).
func BenchmarkWorkerAgentRead(b *testing.B) {
	cases := []struct {
		name string
		pkt  []byte
	}{
		{"JobAssignUniq", perfResPacket(dtJobAssignUniq, perfAssignBody(true, perfSmallPayload))},
		{"Large32K", perfResPacket(dtJobAssignUniq, perfAssignBody(true, perfLargePayload))},
		{"NoJob", perfResPacket(dtNoJob, nil)},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			a := perfReadAgent(c.pkt)
			b.ReportAllocs()
			b.ResetTimer()
			// a.rw directly, not getRW(): work() now passes read() the rw it
			// was started with rather than re-fetching it under connMu every
			// call, so a field read is what the real call site costs.
			for i := 0; i < b.N; i++ {
				perfSinkBytes, perfSinkErr = a.read(a.rw)
			}
		})
	}
}

// BenchmarkWorkerAgentWrite prices Encode plus the buffered write and Flush.
// io.Discard behind the bufio.Writer, so no syscall is in the number.
func BenchmarkWorkerAgentWrite(b *testing.B) {
	cases := []struct {
		name string
		pack *outPack
	}{
		{"GrabJobUniq", &outPack{dataType: dtGrabJobUniq}},
		{"WorkComplete", &outPack{dataType: dtWorkComplete, handle: perfHandle, data: perfSmallPayload}},
		{"WorkCompleteLarge", &outPack{dataType: dtWorkComplete, handle: perfHandle, data: perfLargePayload}},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			a := perfWriteAgent()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				perfSinkErr = a.write(c.pack)
			}
		})
	}
}

// --- the pooling hooks ------------------------------------------------------

// BenchmarkWorkerGetBuffer prices what a sync.Pool behind getBuffer
// (common.go:49) would be replacing, at the sizes the worker actually asks for:
// 12 is an empty packet, 27 a small WORK_COMPLETE body, 4096 and 32 KiB the
// payload cases.
func BenchmarkWorkerGetBuffer(b *testing.B) {
	for _, n := range []int{12, 27, 4096, 32 << 10} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				perfSinkBytes = getBuffer(n)
			}
		})
	}
}

// BenchmarkWorkerGetOutPack and BenchmarkWorkerGetInPack price the other two
// "TODO pool" hooks (outpack.go:14, inpack.go:19).
func BenchmarkWorkerGetOutPack(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		perfSinkOut = getOutPack()
	}
}

func BenchmarkWorkerGetInPack(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		perfSinkIn = getInPack()
	}
}

// --- dispatch ---------------------------------------------------------------

// perfExecWorker builds a running worker with one registered function and an
// agent that discards its writes. running must be true or exec skips the whole
// result path (worker.go:285) and the benchmark prices a map lookup.
func perfExecWorker(timeout uint32, f JobFunc) (*Worker, *inPack) {
	w := New(Unlimited)
	w.ErrorHandler = func(error) {}
	w.funcs[perfFn] = &jobFunc{f: f, timeout: timeout}
	w.running = true
	a := perfWriteAgent()
	a.worker = w
	return w, &inPack{
		dataType: dtJobAssignUniq,
		handle:   perfHandle,
		fn:       perfFn,
		uniqueId: perfUniq,
		data:     perfSmallPayload,
		a:        a,
	}
}

// BenchmarkWorkerExec prices one dispatch body: the funcs lookup, the job
// function, the result, and the WORK_COMPLETE encode and write. It calls exec
// synchronously, so the goroutine handleInPack spawns per job (worker.go:148)
// is not in the number -- see BenchmarkWorkerHandleInPack.
//
// WithTimeout takes the execTimeout branch (worker.go:320): a channel, a
// goroutine, a second result and time.After.
func BenchmarkWorkerExec(b *testing.B) {
	job := func(job Job) ([]byte, error) { return nil, nil }
	for _, c := range []struct {
		name    string
		timeout uint32
	}{
		{"NoTimeout", 0},
		{"WithTimeout", 30},
	} {
		b.Run(c.name, func(b *testing.B) {
			w, inpack := perfExecWorker(c.timeout, job)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				perfSinkErr = w.exec(inpack)
			}
		})
	}
}

// BenchmarkWorkerHandleInPack prices the dispatcher's two synchronous branches.
// The dtJobAssign branch is deliberately absent: it spawns a goroutine whose
// work lands outside the measured region, which would report a per-op figure
// for a fraction of the job. Use BenchmarkWorkerJobPipeline for that path.
func BenchmarkWorkerHandleInPack(b *testing.B) {
	for _, c := range []struct {
		name string
		dt   uint32
	}{
		{"NoJob", dtNoJob},
		{"Noop", dtNoop},
	} {
		b.Run(c.name, func(b *testing.B) {
			w, inpack := perfExecWorker(0, func(Job) ([]byte, error) { return nil, nil })
			inpack.dataType = c.dt
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w.handleInPack(inpack)
			}
		})
	}
}

// BenchmarkWorkerLimitToken prices the concurrency semaphore per job: one send
// in handleInPack (worker.go:154) and one receive in exec's defer
// (worker.go:264). Capped uses capacity 1 rather than OneByOne's 0 so a single
// goroutine can round-trip a token; the channel operations are the same pair.
func BenchmarkWorkerLimitToken(b *testing.B) {
	b.Run("Unlimited", func(b *testing.B) {
		var limit chan bool
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if limit != nil {
				limit <- true
			}
			if limit != nil {
				<-limit
			}
		}
	})
	b.Run("Capped", func(b *testing.B) {
		limit := make(chan bool, 1)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			limit <- true
			<-limit
		}
	})
}

// --- the job-side writers ---------------------------------------------------

// BenchmarkWorkerInPackWriters prices the three progress reporters. Each goes
// through the locked a.Write, uncontended here: the cost of the lock itself is
// part of what these three now pay.
func BenchmarkWorkerInPackWriters(b *testing.B) {
	_, inpack := perfExecWorker(0, func(Job) ([]byte, error) { return nil, nil })
	b.Run("SendData", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			inpack.SendData(perfSmallPayload)
		}
	})
	// Two UpdateStatus cases because strconv.Itoa returns a string from a
	// preallocated table for 0-99 and allocates above it (strconv/itoa.go's
	// small()). One case alone would either overstate or understate the path
	// depending on which side of 100 it happened to pick.
	b.Run("UpdateStatusSmall", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			inpack.UpdateStatus(7, 99)
		}
	})
	b.Run("UpdateStatusLarge", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			inpack.UpdateStatus(7000, 100000)
		}
	})
}

// BenchmarkWorkerPrepFuncOutpack prices registration, which is not a hot path
// but is the one place a NUL separator is written explicitly (worker.go:112).
func BenchmarkWorkerPrepFuncOutpack(b *testing.B) {
	b.Run("CanDo", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			perfSinkOut = prepFuncOutpack(perfFn, 0)
		}
	})
	b.Run("CanDoTimeout", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			perfSinkOut = prepFuncOutpack(perfFn, 30)
		}
	})
}

// --- fixtures under guard ---------------------------------------------------

// TestWorkerPerfFixtures runs in the default suite and asserts decoded *values*,
// not err == nil. A JOB_ASSIGN_UNIQ body with three fields instead of four
// decodes cleanly and leaves fn, handle and uniqueId empty, which would leave
// BenchmarkWorkerDecodeInPack measuring a cheaper path in silence -- the trap
// CLAUDE.md records for worker/fakeserver_test.go and client/microbench_test.go
// records for STATUS_RES.
func TestWorkerPerfFixtures(t *testing.T) {
	t.Run("DecodeJobAssignUniq", func(t *testing.T) {
		pkt := perfResPacket(dtJobAssignUniq, perfAssignBody(true, perfSmallPayload))
		inpack, l, err := decodeInPack(pkt)
		if err != nil {
			t.Fatal(err)
		}
		if l != len(pkt) {
			t.Errorf("consumed %d bytes, want %d", l, len(pkt))
		}
		if inpack.handle != perfHandle || inpack.fn != perfFn || inpack.uniqueId != perfUniq {
			t.Errorf("decoded (%q, %q, %q), want (%q, %q, %q)",
				inpack.handle, inpack.fn, inpack.uniqueId, perfHandle, perfFn, perfUniq)
		}
		if !bytes.Equal(inpack.data, perfSmallPayload) {
			t.Errorf("data = %q, want %q", inpack.data, perfSmallPayload)
		}
	})

	t.Run("DecodeJobAssign", func(t *testing.T) {
		pkt := perfResPacket(dtJobAssign, perfAssignBody(false, perfSmallPayload))
		inpack, _, err := decodeInPack(pkt)
		if err != nil {
			t.Fatal(err)
		}
		if inpack.handle != perfHandle || inpack.fn != perfFn {
			t.Errorf("decoded (%q, %q), want (%q, %q)", inpack.handle, inpack.fn, perfHandle, perfFn)
		}
		if !bytes.Equal(inpack.data, perfSmallPayload) {
			t.Errorf("data = %q, want %q", inpack.data, perfSmallPayload)
		}
	})

	// The separator between handle and body comes from Encode itself
	// (outpack.go:39), not from getBuffer's zeroing. That is what makes the
	// encode side safe to pool; the three inpack.go writers are not, and
	// EncodeFillsEveryByte is the guard on the half that is.
	t.Run("EncodeFillsEveryByte", func(t *testing.T) {
		pack := &outPack{dataType: dtWorkComplete, handle: perfHandle, data: perfSmallPayload}
		got := pack.Encode()
		want := append([]byte(perfHandle+"\x00"), perfSmallPayload...)
		if !bytes.Equal(got[minPacketLength:], want) {
			t.Errorf("body = %q, want %q", got[minPacketLength:], want)
		}
		if binary.BigEndian.Uint32(got[:4]) != req {
			t.Errorf("magic = %q, want %q", got[:4], reqStr)
		}
		if n := binary.BigEndian.Uint32(got[8:minPacketLength]); int(n) != len(want) {
			t.Errorf("declared length = %d, want %d", n, len(want))
		}
	})

	// What pooling in write actually depends on: a reused buffer is not
	// re-zeroed, so any byte encodeInto leaves untouched reaches the wire as
	// whatever the previous packet put there.
	t.Run("EncodeIntoLeavesNoResidue", func(t *testing.T) {
		cases := []struct {
			name string
			pack *outPack
		}{
			{"Handle", &outPack{dataType: dtWorkComplete, handle: perfHandle, data: perfSmallPayload}},
			{"NoHandle", &outPack{dataType: dtPreSleep}},
			{"WorkFail", &outPack{dataType: dtWorkFail, handle: perfHandle}},
			{"EmptyData", &outPack{dataType: dtWorkComplete, handle: perfHandle}},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				dirty := make([]byte, 4096)
				for i := range dirty {
					dirty[i] = 0xff
				}
				got := c.pack.encodeInto(dirty)
				if want := c.pack.Encode(); !bytes.Equal(got, want) {
					t.Errorf("encodeInto over dirty memory = %q, want %q", got, want)
				}
			})
		}
	})

	// read must hand back exactly one whole packet from the replaying reader,
	// or every framing benchmark below is measuring the wrong bytes.
	t.Run("AgentReadReplaysWholePackets", func(t *testing.T) {
		pkt := perfResPacket(dtJobAssignUniq, perfAssignBody(true, perfSmallPayload))
		a := perfReadAgent(pkt)
		for i := 0; i < 3; i++ {
			got, err := a.read(a.rw)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, pkt) {
				t.Fatalf("read %d returned %d bytes, want the whole %d-byte packet", i, len(got), len(pkt))
			}
		}
	})
}
