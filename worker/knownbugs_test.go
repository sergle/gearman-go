package worker

import (
	"bufio"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

// Tests for defects that are known and not yet fixed, mirroring
// client/knownbugs_test.go. They describe the behaviour the worker *should*
// have, so they fail against the code as it stands -- that is the point.
//
//	go test ./worker              # green; these are skipped
//	go test ./worker -knownbugs   # the outstanding defects, red
//
// The flag must come *after* the package list, exactly as for -integration:
// `go test -knownbugs ./worker` silently tests the current directory and exits
// 0.

func requireWorkerKnownBugs(t *testing.T) {
	t.Helper()
	if !runKnownBugTests {
		t.Skip("known unfixed defect; run with: go test ./worker -knownbugs")
	}
}

// agent.read assumes every read starts on a packet boundary and that the first
// read delivers at least a 12-byte header. Neither holds on a stream socket.
//
//	tmp := getBuffer(bufferSize)
//	n, err = a.rw.Read(tmp)
//	dl := int(binary.BigEndian.Uint32(tmp[8:12]))
//	...
//	for buf.Len() < dl+minPacketLength { ... }
//
// When that first read returns fewer than 12 bytes, bytes 8:12 are still the
// zeros getBuffer handed out, so dl is 0 and the loop stops as soon as 12
// bytes have accumulated -- returning a fragment of a packet. work() then
// fails to decode it and parks the remainder in leftdata, so the *next*
// a.read() starts in the middle of a body and takes dl from four payload
// bytes. With a text payload that is a huge number, and the read loop waits
// for bytes that will never arrive: the agent stops framing, the worker stops
// grabbing, and the pipeline is dead with no error anywhere.
//
// This test drives the first half deterministically over net.Pipe, which
// delivers exactly one Write per Read: an 8-byte chunk, then a 10-byte chunk,
// then the rest. read() must return whole packets, whatever the chunking.
//
// Found while writing worker/bench_test.go: a job pipeline benchmark stalls
// once assignments stop fitting in one read, and whether it stalls or merely
// crawls depends on whether the *payload bytes* are zeros -- which is what
// pointed at dl being read out of a body.
func TestAgentReadReturnsWholePackets(t *testing.T) {
	requireWorkerKnownBugs(t)

	local, remote := net.Pipe()
	defer local.Close()
	defer remote.Close()

	a := &agent{
		conn:   local,
		rw:     bufio.NewReadWriter(bufio.NewReader(local), bufio.NewWriter(local)),
		worker: New(Unlimited),
	}

	pkt := resPacket(dtJobAssignUniq,
		jobAssignBody(fakeJobHandle, "bench", fakeJobUniq, []byte("xxxxxxxxxxxxxxxx")))

	// Three writes, so the reader sees an 8-byte fragment first: shorter than
	// the header, which is what makes dl come out of an untouched buffer.
	go func() {
		remote.Write(pkt[:8])
		remote.Write(pkt[8:18])
		remote.Write(pkt[18:])
	}()

	type readResult struct {
		data []byte
		err  error
	}
	done := make(chan readResult, 1)
	go func() {
		data, err := a.read()
		done <- readResult{data, err}
	}()

	select {
	case got := <-done:
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
	case <-time.After(2 * time.Second):
		// The other half of the defect: a dl taken from payload bytes is
		// enormous, and the read loop waits for it forever.
		t.Fatal("read did not return within 2s")
	}
}
