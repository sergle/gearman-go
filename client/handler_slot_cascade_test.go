package client

import (
	"testing"
	"time"
)

// An abandoned entry with no expiry stayed queued forever when its reply never
// came, so it ate the next caller's reply, and that caller's entry ate the one
// after: every later call starved. Expiry and reset bound that.
//
// Not a full fix: nothing on the wire tells a late reply apart from the next
// request's own, and TestEchoLateReplyDoesNotSatisfyNextCaller requires the
// late one to still be absorbed. A call issued inside the expiry window can
// still lose its reply; it can no longer repeat forever.

// TestEchoRecoversOnceTheAbandonedEntryExpires is the gap case: a call issued
// well after the expiry window must not be starved by an old abandoned entry.
func TestEchoRecoversOnceTheAbandonedEntryExpires(t *testing.T) {
	s := newFakeJobServer(t)
	c := newTestClient(t, s)
	c.SetErrorHandler(func(error) {})
	c.ResponseTimeout = 50 * time.Millisecond

	s.SetSilent(true)
	if _, err := c.Echo([]byte("first")); err != ErrLostConn {
		t.Fatalf("first Echo err = %v, want ErrLostConn", err)
	}
	s.SetSilent(false)

	// Longer than the expiry (ResponseTimeout past the timeout above), so the
	// abandoned entry is provably gone by the time this call's own reply
	// arrives -- not a race against it.
	time.Sleep(3 * c.ResponseTimeout)

	got, err := c.Echo([]byte("second"))
	if err != nil {
		t.Fatalf("Echo well after the abandoned entry expired: %v", err)
	}
	if string(got) != "second" {
		t.Errorf("Echo = %q, want %q", got, "second")
	}
}

// The case neither mitigation above covers: retrying with no gap for the
// abandoned entry to expire and nothing to drop the connection. Each retry's
// reply used to be eaten by the previous entry, which left one of its own, so
// the debt moved but never drained. Echo closes the connection now.
func TestEchoRecoversFromARetryWithNoGap(t *testing.T) {
	s := newFakeJobServer(t)
	c := newTestClient(t, s)
	c.SetErrorHandler(func(error) {})
	c.ResponseTimeout = 150 * time.Millisecond

	s.SetSilent(true)
	if _, err := c.Echo([]byte("first")); err != ErrLostConn {
		t.Fatalf("first Echo err = %v, want ErrLostConn", err)
	}
	s.SetSilent(false)

	// Sleep between attempts: a retry during the redial fails instantly, and
	// a tight loop starves readLoop's goroutine rather than measuring it.
	deadline := time.Now().Add(3 * time.Second)
	var got []byte
	var err error
	for time.Now().Before(deadline) {
		got, err = c.Echo([]byte("retry"))
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil {
		t.Fatalf("retry with no gap never recovered within 3s: %v", err)
	}
	if string(got) != "retry" {
		t.Errorf("Echo = %q, want %q", got, "retry")
	}
}

// TestEchoRecoversAfterATimedOutRequestAndADisconnect is the disconnect
// variant: dropping the connection clears both handler queues outright
// (closeConn), so a call issued after redial is never at risk from an entry
// left behind on the old connection -- no timing race at all.
func TestEchoRecoversAfterATimedOutRequestAndADisconnect(t *testing.T) {
	s := newFakeJobServer(t)
	s.SetSilent(true)
	c := newTestClient(t, s)
	c.ResponseTimeout = 200 * time.Millisecond

	if _, err := c.Echo([]byte("first")); err != ErrLostConn {
		t.Fatalf("first Echo err = %v, want ErrLostConn", err)
	}
	s.SetSilent(false)
	s.DropConnections() // readLoop redials; closeConn clears the queues

	// Wait for the redial's OPTION_REQ before issuing the next call, matching
	// how TestRedialNotBlockedByInFlightDo synchronizes on it elsewhere.
	deadline := time.Now().Add(2 * time.Second)
	for s.OptionReqs() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("no re-dial within 2s")
		}
		time.Sleep(time.Millisecond)
	}

	got, err := c.Echo([]byte("second"))
	if err != nil {
		t.Fatalf("Echo after a timed-out request and a disconnect: %v", err)
	}
	if string(got) != "second" {
		t.Errorf("Echo = %q, want %q", got, "second")
	}
}
