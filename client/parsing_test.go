package client

import (
	"testing"
)

// Tests for the response parser: the handle/body split in decodeResponse and
// the four-field STATUS_RES scan in _status. Both are reachable from the
// network and run on processLoop's goroutine, so a malformed packet has to
// produce an error, never a panic.

// TestStatusParseMalformed covers the bodies a job server should never send.
//
// An empty flag field used to panic: the length check passed and the first byte
// was read from a zero-length field, on a goroutine the caller cannot recover.
func TestStatusParseMalformed(t *testing.T) {
	for _, c := range []struct {
		name string
		body string
	}{
		{"empty known", "\x00\x0050\x00100"},
		{"empty running", "1\x00\x0050\x00100"},
		{"both flags empty", "\x00\x0050\x00100"},
		{"all fields empty", "\x00\x00\x00"},
		{"three fields", "1\x001\x0050"},
		{"two fields", "1\x001"},
		{"one field", "1"},
		{"no fields", ""},
		{"numerator not a number", "1\x001\x00xx\x00100"},
		{"denominator not a number", "1\x001\x0050\x00yy"},
		{"fifth field", "1\x001\x0050\x00100\x00extra"},
	} {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panicked on %q: %v", c.body, r)
				}
			}()
			resp := &Response{DataType: dtStatusRes, Handle: "H:localhost:1", Data: []byte(c.body)}
			if _, err := resp._status(); err == nil {
				t.Errorf("_status(%q) returned nil error, want one", c.body)
			}
		})
	}
}

// TestStatusParseValid pins what the scan accepts, including the first-byte
// reading of the two flags.
func TestStatusParseValid(t *testing.T) {
	for _, c := range []struct {
		name        string
		body        string
		known       bool
		running     bool
		numerator   uint64
		denominator uint64
	}{
		{"known and running", "1\x001\x0050\x00100", true, true, 50, 100},
		{"neither", "0\x000\x000\x000", false, false, 0, 0},
		{"known only", "1\x000\x003\x009", true, false, 3, 9},
		{"flags read by first byte", "10\x0010\x005\x006", true, true, 5, 6},
		{"non-flag byte is false", "x\x00y\x001\x002", false, false, 1, 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			resp := &Response{DataType: dtStatusRes, Handle: "H:localhost:1", Data: []byte(c.body)}
			st, err := resp._status()
			if err != nil {
				t.Fatalf("_status(%q): %v", c.body, err)
			}
			if st.Handle != "H:localhost:1" {
				t.Errorf("Handle = %q, want %q", st.Handle, "H:localhost:1")
			}
			if st.Known != c.known {
				t.Errorf("Known = %v, want %v", st.Known, c.known)
			}
			if st.Running != c.running {
				t.Errorf("Running = %v, want %v", st.Running, c.running)
			}
			if st.Numerator != c.numerator || st.Denominator != c.denominator {
				t.Errorf("progress = %d/%d, want %d/%d",
					st.Numerator, st.Denominator, c.numerator, c.denominator)
			}
		})
	}
}

// TestDecodeHandleSplit pins the handle/body split for every packet type that
// carries a handle.
func TestDecodeHandleSplit(t *testing.T) {
	for _, c := range []struct {
		name       string
		dataType   uint32
		body       string
		wantHandle string
		wantData   string
		wantErr    bool
	}{
		{"work complete", dtWorkComplete, "H:1\x00result", "H:1", "result", false},
		{"empty handle", dtWorkComplete, "\x00result", "", "result", false},
		{"empty payload", dtWorkComplete, "H:1\x00", "H:1", "", false},
		{"payload holds separators", dtWorkComplete, "H:1\x00a\x00b", "H:1", "a\x00b", false},
		{"no separator", dtWorkComplete, "H:1", "", "", true},
		{"empty body", dtWorkComplete, "", "", "", true},
		{"status res", dtStatusRes, "H:1\x001\x001\x000\x000", "H:1", "1\x001\x000\x000", false},
		{"work data", dtWorkData, "H:1\x00chunk", "H:1", "chunk", false},
		// WORK_FAIL carries the handle alone and has never required a separator.
		{"work fail", dtWorkFail, "H:1", "H:1", "", false},
		{"work fail with separator", dtWorkFail, "H:1\x00tail", "H:1", "", false},
		{"work fail empty", dtWorkFail, "", "", "", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			packet := frameResponse(c.dataType, []byte(c.body))
			resp, l, err := decodeResponse(packet)
			if c.wantErr {
				if err == nil {
					t.Fatalf("decodeResponse(%q) returned nil error, want one", c.body)
				}
				return
			}
			if err != nil {
				t.Fatalf("decodeResponse(%q): %v", c.body, err)
			}
			if l != len(packet) {
				t.Errorf("consumed %d bytes, packet is %d", l, len(packet))
			}
			if resp.Handle != c.wantHandle {
				t.Errorf("Handle = %q, want %q", resp.Handle, c.wantHandle)
			}
			if string(resp.Data) != c.wantData {
				t.Errorf("Data = %q, want %q", resp.Data, c.wantData)
			}
		})
	}
}
