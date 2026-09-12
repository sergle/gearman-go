package client

import (
	"bufio"
	"encoding/binary"
	"io"
	"testing"
)

// Socket-free microbenchmarks for the encode, decode and read paths, the
// counterpart to ./worker's BenchmarkDecode and BenchmarkEncode.
//
// Read allocs/op and B/op, not ns/op: those are exact Mallocs deltas, right
// even at make bench's default 2000x, while the socket benchmarks in
// bench_test.go are syscall-dominated and blind to any of this.

// --- fixtures ---------------------------------------------------------------

// microLongFuncName is past the 32-byte stack temporary a []byte(string)
// conversion uses. See BenchmarkEncodeJob.
const (
	microFuncName     = "f"
	microLongFuncName = "com.example.service.reticulate_splines_v2"
	microId           = "1234567890"
	microHandle       = "H:localhost:42"
)

var (
	microSmallPayload = []byte("payload")
	microLargePayload = make([]byte, 32*1024)

	// Framed as the job server frames them, built at init so no benchmark
	// measures its own fixture construction.
	microJobCreated      = frameResponse(dtJobCreated, []byte(microHandle))
	microWorkComplete    = frameResponse(dtWorkComplete, microBody(microSmallPayload))
	microWorkCompleteBig = frameResponse(dtWorkComplete, microBody(microLargePayload))
	microEchoRes         = frameResponse(dtEchoRes, microSmallPayload)
	microStatusRes       = frameResponse(dtStatusRes, []byte(microHandle+"\x001\x001\x0050\x00100"))
	microStatusResponse  = &Response{DataType: dtStatusRes, Handle: microHandle, Data: []byte("1\x001\x0050\x00100")}
)

func microBody(data []byte) []byte {
	body := make([]byte, 0, len(microHandle)+1+len(data))
	body = append(body, microHandle...)
	body = append(body, 0)
	return append(body, data...)
}

// Sinks, so the compiler cannot delete the work being measured.
var (
	sinkResp   *Response
	sinkBytes  []byte
	sinkStatus *Status
	sinkString string
)

// --- self-test --------------------------------------------------------------

// TestMicrobenchFixtures runs in the default suite, per the convention
// fakeserver_test.go:281 sets out. It asserts decoded values rather than a nil
// error: a STATUS_RES body with too few NUL-separated fields decodes fine and
// only fails _status, leaving BenchmarkStatusParse on the error path at a
// fraction of the real cost.
func TestMicrobenchFixtures(t *testing.T) {
	cases := []struct {
		name       string
		packet     []byte
		dataType   uint32
		wantHandle string
		wantData   []byte
	}{
		{"JobCreated", microJobCreated, dtJobCreated, microHandle, nil},
		{"WorkComplete", microWorkComplete, dtWorkComplete, microHandle, microSmallPayload},
		{"WorkCompleteLarge", microWorkCompleteBig, dtWorkComplete, microHandle, microLargePayload},
		{"EchoRes", microEchoRes, dtEchoRes, "", microSmallPayload},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp, l, err := decodeResponse(c.packet)
			if err != nil {
				t.Fatalf("decodeResponse: %v", err)
			}
			if l != len(c.packet) {
				t.Errorf("consumed %d bytes, packet is %d", l, len(c.packet))
			}
			if resp.DataType != c.dataType {
				t.Errorf("DataType: want %d, got %d", c.dataType, resp.DataType)
			}
			if resp.Handle != c.wantHandle {
				t.Errorf("Handle: want %q, got %q", c.wantHandle, resp.Handle)
			}
			if len(resp.Data) != len(c.wantData) {
				t.Fatalf("Data: want %d bytes, got %d", len(c.wantData), len(resp.Data))
			}
			for i := range c.wantData {
				if resp.Data[i] != c.wantData[i] {
					t.Fatalf("Data differs at byte %d", i)
				}
			}
		})
	}

	t.Run("StatusRes", func(t *testing.T) {
		resp, l, err := decodeResponse(microStatusRes)
		if err != nil {
			t.Fatalf("decodeResponse: %v", err)
		}
		if l != len(microStatusRes) {
			t.Errorf("consumed %d bytes, packet is %d", l, len(microStatusRes))
		}
		if resp.Handle != microHandle {
			t.Errorf("Handle: want %q, got %q", microHandle, resp.Handle)
		}
		st, err := resp._status()
		if err != nil {
			t.Fatalf("_status: %v (fixture is not four NUL-separated fields)", err)
		}
		if !st.Known || !st.Running {
			t.Errorf("Known/Running: want true/true, got %v/%v", st.Known, st.Running)
		}
		if st.Numerator != 50 || st.Denominator != 100 {
			t.Errorf("progress: want 50/100, got %d/%d", st.Numerator, st.Denominator)
		}
	})

	// The parse benchmark's hand-built Response must agree with the decoded one,
	// or the two benchmarks measure different inputs.
	t.Run("StandaloneStatusResponse", func(t *testing.T) {
		st, err := microStatusResponse._status()
		if err != nil {
			t.Fatalf("_status: %v", err)
		}
		if !st.Known || st.Numerator != 50 || st.Denominator != 100 {
			t.Errorf("want known 50/100, got %v %d/%d", st.Known, st.Numerator, st.Denominator)
		}
	})

	// Nothing else asserts the two NUL separators, and getBuffer's zeroing would
	// hide a missing one until it became a pool.
	t.Run("EncodeSeparators", func(t *testing.T) {
		buf := encodeJob(dtSubmitJobBg, microFuncName, microId, microSmallPayload)

		wantLen := minPacketLength + len(microFuncName) + 1 + len(microId) + 1 + len(microSmallPayload)
		if len(buf) != wantLen {
			t.Fatalf("encoded %d bytes, want %d", len(buf), wantLen)
		}
		if string(buf[:4]) != reqStr {
			t.Errorf("magic: want %q, got %q", reqStr, buf[:4])
		}
		if dt := binary.BigEndian.Uint32(buf[4:8]); dt != dtSubmitJobBg {
			t.Errorf("DataType: want %d, got %d", dtSubmitJobBg, dt)
		}
		if l := binary.BigEndian.Uint32(buf[8:12]); int(l) != wantLen-minPacketLength {
			t.Errorf("length prefix: want %d, got %d", wantLen-minPacketLength, l)
		}
		body := buf[minPacketLength:]
		if body[len(microFuncName)] != 0 {
			t.Errorf("no NUL after funcname at body index %d", len(microFuncName))
		}
		if body[len(microFuncName)+1+len(microId)] != 0 {
			t.Errorf("no NUL after id at body index %d", len(microFuncName)+1+len(microId))
		}
	})
}

// --- decode -----------------------------------------------------------------

// BenchmarkDecodeResponse prices getResponse and the string(dt) behind Handle.
//
// WorkCompleteLarge is flat against WorkComplete because resp.Data aliases the
// packet. It should stop being flat: framing reads by declared length forces a
// copy at decode, which lands here.
func BenchmarkDecodeResponse(b *testing.B) {
	cases := []struct {
		name   string
		packet []byte
	}{
		{"JobCreated", microJobCreated},
		{"WorkComplete", microWorkComplete},
		{"StatusRes", microStatusRes},
		{"EchoRes", microEchoRes},
		{"WorkCompleteLarge", microWorkCompleteBig},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resp, _, err := decodeResponse(c.packet)
				if err != nil {
					b.Fatal(err)
				}
				sinkResp = resp
			}
		})
	}
}

// BenchmarkStatusParse measures _status alone: a bytes.SplitN and two ParseUint.
// The four string(...) conversions do not allocate -- they never escape.
func BenchmarkStatusParse(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st, err := microStatusResponse._status()
		if err != nil {
			b.Fatal(err)
		}
		sinkStatus = st
	}
}

// --- encode -----------------------------------------------------------------

// BenchmarkEncodeJob measures the whole submit encode.
//
// LongFuncName guards the funcname past the 32-byte stack temporary: encodeJob
// copies from the string, so there is no allocation, and a signature taking
// []byte would reintroduce one here while Small stayed flat.
func BenchmarkEncodeJob(b *testing.B) {
	cases := []struct {
		name     string
		funcname string
		payload  []byte
	}{
		{"Small", microFuncName, microSmallPayload},
		{"Large", microFuncName, microLargePayload},
		{"LongFuncName", microLongFuncName, microSmallPayload},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sinkBytes = encodeJob(dtSubmitJobBg, c.funcname, microId, c.payload)
			}
		})
	}
}

// BenchmarkIdGen prices the default IdGenerator: a strconv.FormatInt string
// that encodeJob then just copies. A local generator, not the package-level
// IdGen, whose counter other tests read.
func BenchmarkIdGen(b *testing.B) {
	gen := NewAutoIncId()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkString = gen.Id()
	}
}

// --- read -------------------------------------------------------------------

// packetReader replays one packet endlessly, so readPacket never runs out of
// input and the benchmark measures framing rather than a socket.
type packetReader struct {
	packet []byte
	off    int
}

func (r *packetReader) Read(p []byte) (n int, err error) {
	for n < len(p) {
		c := copy(p[n:], r.packet[r.off:])
		n += c
		if r.off += c; r.off == len(r.packet) {
			r.off = 0
		}
	}
	return n, nil
}

// BenchmarkClientRead measures (*Client).readPacket with no socket under it:
// one exact-size allocation per packet, header and body in the same slice.
//
// nil conn with a live rw is deliberate -- readPacket only reaches for getRW.
func BenchmarkClientRead(b *testing.B) {
	cases := []struct {
		name   string
		packet []byte
	}{
		{"SmallPacket", microWorkComplete},
		{"LargePacket", microWorkCompleteBig},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			cl := &Client{}
			cl.setConn(nil, bufio.NewReadWriter(
				bufio.NewReader(&packetReader{packet: c.packet}),
				bufio.NewWriter(io.Discard),
			))

			var hdr [minPacketLength]byte
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				packet, err := cl.readPacket(hdr[:])
				if err != nil {
					b.Fatal(err)
				}
				sinkBytes = packet
			}
		})
	}
}
