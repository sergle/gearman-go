package client

import (
	"bytes"
	"testing"
	"time"
)

// Characterisation tests: they pin the client's observable behaviour as it is
// today, so that the race fixes can be told apart from regressions. They assert
// what the code *does*, not what it arguably should do — where the two differ,
// the comment says so.
//
// They run against the in-process fake job server (fakeserver_test.go), so no
// gearmand is needed and they are not gated behind -integration.
//
// Some overlap with the TestFakeServer* self-tests is deliberate: those exist
// to prove the harness speaks the protocol, these to pin the client's contract.

func newTestClient(t *testing.T, s *fakeJobServer) *Client {
	t.Helper()
	c, err := New(Network, s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// Do returns the handle the server assigned and routes the completion to the
// caller's handler.
func TestDoReturnsHandleAndDeliversCompletion(t *testing.T) {
	s := newFakeJobServer(t)
	c := newTestClient(t, s)

	type completion struct {
		handle string
		data   []byte
	}
	done := make(chan completion, 1)

	handle, err := c.Do("ToUpper", []byte("hello"), JobNormal, func(r *Response) {
		done <- completion{r.Handle, r.Data}
	})
	if err != nil {
		t.Fatal(err)
	}
	if handle != "H:fake:1" {
		t.Errorf("Do handle = %q, want H:fake:1", handle)
	}

	select {
	case got := <-done:
		if got.handle != handle {
			t.Errorf("completion handle = %q, want %q", got.handle, handle)
		}
		if string(got.data) != "hello" {
			t.Errorf("completion data = %q, want %q", got.data, "hello")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WORK_COMPLETE was never delivered to the handler")
	}
}

// DoBg returns a handle without waiting for a result.
func TestDoBgReturnsHandle(t *testing.T) {
	s := newFakeJobServer(t)
	c := newTestClient(t, s)

	handle, err := c.DoBg("ToUpper", []byte("hello"), JobNormal)
	if err != nil {
		t.Fatal(err)
	}
	if handle != "H:fake:1" {
		t.Errorf("DoBg handle = %q, want H:fake:1", handle)
	}
}

// The opcode is chosen by priority and by foreground/background, and the body
// is always funcname\x00id\x00payload.
func TestSubmitWireFormat(t *testing.T) {
	cases := []struct {
		name     string
		bg       bool
		flag     byte
		wantType uint32
	}{
		{"normal", false, JobNormal, dtSubmitJob},
		{"low", false, JobLow, dtSubmitJobLow},
		{"high", false, JobHigh, dtSubmitJobHigh},
		{"normal bg", true, JobNormal, dtSubmitJobBg},
		{"low bg", true, JobLow, dtSubmitJobLowBg},
		{"high bg", true, JobHigh, dtSubmitJobHighBg},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newFakeJobServer(t)
			c := newTestClient(t, s)

			var err error
			if tc.bg {
				_, err = c.DoBgWithId("ToUpper", []byte("payload"), tc.flag, "my-id")
			} else {
				_, err = c.DoWithId("ToUpper", []byte("payload"), tc.flag, nil, "my-id")
			}
			if err != nil {
				t.Fatal(err)
			}

			req := s.WaitRequests(t, 1, 2*time.Second)[0]
			if req.DataType != tc.wantType {
				t.Errorf("opcode = %d, want %d", req.DataType, tc.wantType)
			}
			want := []byte("ToUpper\x00my-id\x00payload")
			if !bytes.Equal(req.Data, want) {
				t.Errorf("body = %q, want %q", req.Data, want)
			}
		})
	}
}

// Echo round-trips bytes verbatim, including an embedded NUL — the byte the
// protocol itself uses as a field separator.
func TestEchoRoundTripsNulBytes(t *testing.T) {
	s := newFakeJobServer(t)
	c := newTestClient(t, s)

	payload := []byte("Hello\x00 world")
	echo, err := c.Echo(payload)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(echo, payload) {
		t.Errorf("Echo = %q, want %q", echo, payload)
	}
}

// Status parses every field of STATUS_RES.
func TestStatusParsesResponse(t *testing.T) {
	s := newFakeJobServer(t)
	s.SetStatus("H:fake:9", fakeStatus{
		Known: true, Running: false, Numerator: 3, Denominator: 9,
	})
	c := newTestClient(t, s)

	st, err := c.Status("H:fake:9")
	if err != nil {
		t.Fatal(err)
	}
	if st.Handle != "H:fake:9" {
		t.Errorf("Handle = %q", st.Handle)
	}
	if !st.Known {
		t.Error("Known = false, want true")
	}
	if st.Running {
		t.Error("Running = true, want false")
	}
	if st.Numerator != 3 || st.Denominator != 9 {
		t.Errorf("progress = %d/%d, want 3/9", st.Numerator, st.Denominator)
	}
}

// An empty unique id is rejected before anything is written to the server.
func TestDoWithIdRejectsEmptyId(t *testing.T) {
	s := newFakeJobServer(t)
	c := newTestClient(t, s)

	if _, err := c.DoWithId("ToUpper", []byte("x"), JobNormal, nil, ""); err != ErrInvalidId {
		t.Errorf("DoWithId err = %v, want ErrInvalidId", err)
	}
	if _, err := c.DoBgWithId("ToUpper", []byte("x"), JobNormal, ""); err != ErrInvalidId {
		t.Errorf("DoBgWithId err = %v, want ErrInvalidId", err)
	}
	if got := s.Requests(); len(got) != 0 {
		t.Errorf("server received %d requests, want 0 — the id is validated before writing", len(got))
	}
}

// Resubmitting the same unique id coalesces onto the same handle.
func TestSameIdYieldsSameHandle(t *testing.T) {
	s := newFakeJobServer(t)
	c := newTestClient(t, s)

	first, err := c.DoBgWithId("ToUpper", []byte("x"), JobNormal, "same-id")
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.DoBgWithId("ToUpper", []byte("y"), JobNormal, "same-id")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Errorf("handles = %q and %q, want the same handle for one id", first, second)
	}

	other, err := c.DoBgWithId("ToUpper", []byte("z"), JobNormal, "other-id")
	if err != nil {
		t.Fatal(err)
	}
	if other == first {
		t.Errorf("a different id reused handle %q", other)
	}
}

// Close can be called repeatedly, and afterwards every request-sending method
// reports the connection is gone rather than panicking or hanging.
func TestCloseIsIdempotentAndSubsequentCallsFail(t *testing.T) {
	s := newFakeJobServer(t)
	c := newTestClient(t, s)

	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	// Each of these must return promptly. Run them with a deadline so a
	// regression that reintroduces blocking fails the test instead of hanging
	// the whole package.
	checks := []struct {
		name string
		call func() error
	}{
		{"Do", func() error { _, err := c.Do("f", []byte("x"), JobNormal, nil); return err }},
		{"DoBg", func() error { _, err := c.DoBg("f", []byte("x"), JobNormal); return err }},
		{"Status", func() error { _, err := c.Status("H:fake:1"); return err }},
		{"Echo", func() error { err := error(nil); _, err = c.Echo([]byte("x")); return err }},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			errc := make(chan error, 1)
			go func() { errc <- check.call() }()
			select {
			case err := <-errc:
				if err != ErrLostConn {
					t.Errorf("%s after Close = %v, want ErrLostConn", check.name, err)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("%s blocked after Close", check.name)
			}
		})
	}
}
