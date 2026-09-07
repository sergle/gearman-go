package worker

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

// Regression tests for agent.read's framing contract. They were the
// -knownbugs reproducer until it went green; see TestAgentReadReturnsWholePackets
// for the defect itself.
//
// A regression here does not fail cleanly -- a wrong declared length makes read
// wait for bytes that never arrive -- hence readWithin rather than calling
// a.read() on the test goroutine.
//
// net.Pipe, not a loopback socket: it delivers exactly one Write per Read, so
// each case's chunking is what the reader actually sees rather than whatever
// the kernel decides to coalesce.

func pipeAgent(t *testing.T) (a *agent, remote net.Conn) {
	t.Helper()
	local, remote := net.Pipe()
	t.Cleanup(func() {
		local.Close()
		remote.Close()
	})
	return &agent{
		conn:   local,
		rw:     bufio.NewReadWriter(bufio.NewReader(local), bufio.NewWriter(local)),
		worker: New(Unlimited),
	}, remote
}

type readResult struct {
	data []byte
	err  error
}

// readWithin keeps a blocking regression from wedging the whole test binary
// until the global timeout: a body length taken from the wrong offset waits
// for bytes that never arrive.
func readWithin(t *testing.T, a *agent) readResult {
	t.Helper()
	done := make(chan readResult, 1)
	go func() {
		data, err := a.read()
		done <- readResult{data, err}
	}()
	select {
	case got := <-done:
		return got
	case <-time.After(2 * time.Second):
		t.Fatal("read did not return within 2s")
		return readResult{}
	}
}

func assertPacket(t *testing.T, got readResult, want []byte) {
	t.Helper()
	if got.err != nil {
		t.Fatalf("read: %v", got.err)
	}
	if !bytes.Equal(got.data, want) {
		t.Fatalf("read returned %d bytes, want the whole %d-byte packet\n got %q\nwant %q",
			len(got.data), len(want), got.data, want)
	}
	if dl := int(binary.BigEndian.Uint32(got.data[8:12])); dl != len(want)-minPacketLength {
		t.Errorf("framed body length = %d, want %d", dl, len(want)-minPacketLength)
	}
}

// The original reproducer, kept unedited apart from its gate so that it stays
// evidence rather than something reshaped to fit the fix. Full defect writeup
// in docs/todo.md section 10.
//
// The old read took the body length from its first Read. A first read shorter
// than a header left that length reading as zero, so a fragment came back as a
// packet and every later read framed from the wrong offset -- the worker
// stopped grabbing with no error anywhere.
func TestAgentReadReturnsWholePackets(t *testing.T) {
	a, remote := pipeAgent(t)

	pkt := resPacket(dtJobAssignUniq,
		jobAssignBody(fakeJobHandle, "bench", fakeJobUniq, []byte("xxxxxxxxxxxxxxxx")))

	// The first write must be shorter than a header; that is what zeroed the
	// declared length. See SetChunkWrites for why a uniform split does not
	// reproduce this.
	go func() {
		remote.Write(pkt[:8])
		remote.Write(pkt[8:18])
		remote.Write(pkt[18:])
	}()

	got := readWithin(t, a)
	if got.err != nil {
		t.Fatalf("read: %v", got.err)
	}
	if len(got.data) < len(pkt) {
		t.Fatalf("read returned %d bytes of a %d-byte packet: dl was taken "+
			"from a buffer with no header in it", len(got.data), len(pkt))
	}
	if dl := int(binary.BigEndian.Uint32(got.data[8:12])); dl != len(pkt)-minPacketLength {
		t.Errorf("framed body length = %d, want %d", dl, len(pkt)-minPacketLength)
	}
}

// Worst case: nothing arrives together, so anything that frames off a single
// Read's return value fails here.
func TestAgentReadFramesByteAtATime(t *testing.T) {
	a, remote := pipeAgent(t)

	pkt := resPacket(dtJobAssignUniq,
		jobAssignBody(fakeJobHandle, "bench", fakeJobUniq, []byte("split me up")))

	go func() {
		for i := range pkt {
			remote.Write(pkt[i : i+1])
		}
	}()

	assertPacket(t, readWithin(t, a), pkt)
}

// The other direction from splitting: coalescing. Returning both at once would
// hand decodeInPack a tail it silently ignores.
func TestAgentReadSplitsCoalescedPackets(t *testing.T) {
	a, remote := pipeAgent(t)

	first := resPacket(dtJobAssignUniq,
		jobAssignBody(fakeJobHandle, "bench", fakeJobUniq, []byte("first")))
	second := resPacket(dtJobAssignUniq,
		jobAssignBody(fakeJobHandle, "bench", fakeJobUniq, []byte("second-and-longer")))

	go func() {
		remote.Write(append(append([]byte(nil), first...), second...))
	}()

	assertPacket(t, readWithin(t, a), first)
	assertPacket(t, readWithin(t, a), second)
}

// Not an edge case but the most-travelled packet on this path: NOOP is how a
// sleeping worker is woken. If an empty body broke, everything would.
func TestAgentReadEmptyBody(t *testing.T) {
	a, remote := pipeAgent(t)

	noop := resPacket(dtNoop, nil)
	next := resPacket(dtNoJob, nil)

	go func() {
		remote.Write(noop)
		remote.Write(next)
	}()

	assertPacket(t, readWithin(t, a), noop)
	assertPacket(t, readWithin(t, a), next)
}

// Sizes chosen to cross the old 1024-byte read buffer and bufio.Reader's
// 4096-byte default, so the body spans several underlying reads however the
// writer chunks it.
func TestAgentReadLargeBodies(t *testing.T) {
	for _, size := range []int{2000, 5000, 70000} {
		size := size
		t.Run(itoa(size), func(t *testing.T) {
			a, remote := pipeAgent(t)

			// Non-zero bytes: an all-zero payload mis-frames to a length of
			// 0, which returns immediately and hides the defect.
			payload := bytes.Repeat([]byte("x"), size)
			pkt := resPacket(dtJobAssignUniq,
				jobAssignBody(fakeJobHandle, "bench", fakeJobUniq, payload))

			go func() {
				// An awkward size so the chunks land out of step with both
				// the header and bufio's buffer.
				for i := 0; i < len(pkt); i += 777 {
					end := i + 777
					if end > len(pkt) {
						end = len(pkt)
					}
					remote.Write(pkt[i:end])
				}
			}()

			assertPacket(t, readWithin(t, a), pkt)
		})
	}
}

// Nothing may escape with the error: work() redials, and a fragment of the
// dead socket carried into the fresh stream desyncs it exactly as the original
// defect did.
func TestAgentReadEOFMidHeader(t *testing.T) {
	a, remote := pipeAgent(t)

	go func() {
		remote.Write([]byte("\x00RES\x00\x00"))
		remote.Close()
	}()

	got := readWithin(t, a)
	if got.err == nil {
		t.Fatalf("read returned %q, want an error for a truncated header", got.data)
	}
	if got.data != nil {
		t.Errorf("read returned %d bytes with an error; data must be nil", len(got.data))
	}
}

// The two EOFs mean different things to work(), so read must not blur them.
// TestAgentWorkTruncatedPacketDisconnects covers what work() does with this one.
func TestAgentReadEOFMidBody(t *testing.T) {
	a, remote := pipeAgent(t)

	pkt := resPacket(dtJobAssignUniq,
		jobAssignBody(fakeJobHandle, "bench", fakeJobUniq, []byte("truncated")))

	go func() {
		remote.Write(pkt[:len(pkt)-4])
		remote.Close()
	}()

	got := readWithin(t, a)
	if got.err != io.ErrUnexpectedEOF {
		t.Errorf("read error = %v, want io.ErrUnexpectedEOF", got.err)
	}
	if got.data != nil {
		t.Errorf("read returned %d bytes with an error; data must be nil", len(got.data))
	}
}

// The ordinary shutdown case, kept next to the truncated one so a change that
// collapses them fails here.
func TestAgentReadEOFAtBoundary(t *testing.T) {
	a, remote := pipeAgent(t)

	go remote.Close()

	got := readWithin(t, a)
	if got.err != io.EOF {
		t.Errorf("read error = %v, want io.EOF", got.err)
	}
	if got.data != nil {
		t.Errorf("read returned %d bytes with an error; data must be nil", len(got.data))
	}
}

// read pre-allocates the body, so a garbage length is an allocation the caller
// does not control. It has to be rejected from the header alone -- before the
// make, and without waiting for a body that is not coming.
func TestAgentReadRejectsOverlongBody(t *testing.T) {
	a, remote := pipeAgent(t)

	hdr := make([]byte, minPacketLength)
	copy(hdr[:4], resStr)
	binary.BigEndian.PutUint32(hdr[4:8], dtJobAssignUniq)
	binary.BigEndian.PutUint32(hdr[8:12], maxPacketLength+1)

	go remote.Write(hdr)

	got := readWithin(t, a)
	if got.err == nil {
		t.Fatalf("read accepted a %d-byte declared body", maxPacketLength+1)
	}
	if got.data != nil {
		t.Errorf("read returned %d bytes with an error; data must be nil", len(got.data))
	}
}

// The same guard against the value the old code actually produced: "xxxx"
// read as a length, 0x78787878, which is what made the agent wait forever.
func TestAgentReadRejectsLengthFromPayloadBytes(t *testing.T) {
	a, remote := pipeAgent(t)

	hdr := make([]byte, minPacketLength)
	copy(hdr[:4], resStr)
	binary.BigEndian.PutUint32(hdr[4:8], dtJobAssignUniq)
	copy(hdr[8:12], "xxxx")

	go remote.Write(hdr)

	got := readWithin(t, a)
	if got.err == nil {
		t.Fatal("read accepted a body length of 0x78787878 and would have waited for it")
	}
}

// Pins a deliberate behaviour change: a \x00REQ-magic packet from a job server
// used to be decoded and is now rejected. Worth the break because decodeInPack
// ignores data[0:4], leaving the magic as the only cheap desync detector.
func TestAgentReadRejectsBadMagic(t *testing.T) {
	a, remote := pipeAgent(t)

	hdr := make([]byte, minPacketLength)
	copy(hdr[:4], reqStr)
	binary.BigEndian.PutUint32(hdr[4:8], dtJobAssignUniq)
	binary.BigEndian.PutUint32(hdr[8:12], 0)

	go remote.Write(hdr)

	got := readWithin(t, a)
	if got.err == nil {
		t.Fatal("read accepted a \\x00REQ-magic packet")
	}
	if got.data != nil {
		t.Errorf("read returned %d bytes with an error; data must be nil", len(got.data))
	}
}

// The tests above pin what read() returns; this pins where work() sends it,
// which is a separate question and the one that bit. Framing by declared length
// renames a mid-packet death from io.EOF to io.ErrUnexpectedEOF, and routing
// that to the redial branch instead of the disconnect path turns a recoverable
// drop into a worker that is connected, idle and silent forever.
func TestAgentWorkTruncatedPacketDisconnects(t *testing.T) {
	local, remote := net.Pipe()
	defer local.Close()

	w := New(Unlimited)
	errs := make(chan error, 8)
	w.ErrorHandler = func(e error) { errs <- e }

	a := &agent{
		conn:   local,
		rw:     bufio.NewReadWriter(bufio.NewReader(local), bufio.NewWriter(local)),
		worker: w,
		net:    Network,
		// Nothing listens here, so taking the redial branch reports a dial
		// error and names itself in the failure rather than hanging.
		addr: "127.0.0.1:1",
	}
	go a.work()

	pkt := resPacket(dtJobAssignUniq,
		jobAssignBody(fakeJobHandle, "bench", fakeJobUniq, []byte("cut short")))
	go func() {
		remote.Write(pkt[:len(pkt)-4])
		remote.Close()
	}()

	select {
	case err := <-errs:
		disconnect, ok := err.(*WorkerDisconnectError)
		if !ok {
			t.Fatalf("work reported %T (%v); a connection that died mid-packet "+
				"must reach ErrorHandler as *WorkerDisconnectError, or the "+
				"caller never gets to Reconnect()", err, err)
		}
		if disconnect.err != io.ErrUnexpectedEOF {
			t.Errorf("wrapped error = %v, want io.ErrUnexpectedEOF", disconnect.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("work reported nothing for a truncated packet")
	}
}

// The unit tests prove read() frames correctly in isolation. This proves the
// framing survives everything downstream and that the worker keeps grabbing --
// five jobs in a row, which is what the defect actually took away.
//
// 8/10/rest is the split that used to kill the connection; SetChunkWrites
// explains why a uniform one does not.
func TestWorkerPipelineWithChunkedWrites(t *testing.T) {
	s := newFakeWorkerServer(t)
	s.SetChunkWrites(8, 10)
	// Non-zero bytes: an all-zero body mis-frames to a length of 0, which
	// returns immediately and hides the defect.
	payload := bytes.Repeat([]byte("x"), 64)
	s.SetJob("bench", payload)

	newTestWorker(t, s, "bench", func(job Job) ([]byte, error) {
		return job.Data(), nil
	})

	const n = 5
	done := s.Serve(n)
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		assigned, completed := s.Counts()
		t.Fatalf("pipeline stalled on chunked writes: %d/%d done, %d assigned",
			completed, n, assigned)
	}

	results := s.Results()
	if len(results) != n {
		t.Fatalf("results = %d, want %d", len(results), n)
	}
	for i, r := range results {
		if r.DataType != dtWorkComplete {
			t.Errorf("result %d opcode = %d, want %d (WORK_COMPLETE)", i, r.DataType, dtWorkComplete)
		}
		if !bytes.Equal(r.Data, payload) {
			t.Errorf("result %d carried %d bytes, want the %d-byte payload back",
				i, len(r.Data), len(payload))
		}
	}
}

// Avoids pulling strconv in for one subtest name.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
