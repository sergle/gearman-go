package client

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// jobCreatedServer acknowledges the option and answers every SUBMIT_JOB* with a
// JOB_CREATED, which is all Pool.Do and Pool.DoBg need to return. It also
// answers ECHO_REQ and GET_STATUS, for Pool.Echo and Pool.Status.
//
// The STATUS_RES body carries the four NUL-separated fields _status() wants
// once decodeResponse has taken the handle off the front. A malformed one would
// fail the tests below on a parse error rather than on the deadlock.
func jobCreatedServer(conn net.Conn, dataType uint32, data []byte) bool {
	switch dataType {
	case dtOptionReq:
		conn.Write(resPacket(dtOptionRes, data))
	case dtSubmitJob, dtSubmitJobBg, dtSubmitJobHigh, dtSubmitJobHighBg,
		dtSubmitJobLow, dtSubmitJobLowBg:
		conn.Write(resPacket(dtJobCreated, []byte("H:localhost:1")))
	case dtEchoReq:
		conn.Write(resPacket(dtEchoRes, data))
	case dtGetStatus:
		handle := string(data)
		conn.Write(resPacket(dtStatusRes, []byte(handle+"\x001\x001\x001\x002")))
	}
	return true
}

// TestPoolDoDoesNotDeadlock guards the locking in Pool.Do and Pool.DoBg.
//
// Both used to take the client's embedded mutex and then call through to
// (*Client).do, which takes that same mutex. sync.Mutex is not reentrant, so
// the call never returned. The tests that would have caught it are behind
// -integration and would have hung rather than failed.
//
// Each call runs in its own goroutine so a regression fails this test instead
// of blocking the whole suite.
func TestPoolDoDoesNotDeadlock(t *testing.T) {
	s := newFakeServer(t)
	defer s.close()
	s.handle = jobCreatedServer

	p := NewPool()
	if err := p.Add(Network, s.addr(), 1); err != nil {
		t.Fatalf("Pool.Add: %v", err)
	}

	for _, tc := range []struct {
		name string
		call func() (string, string, error)
	}{
		{"Do", func() (string, string, error) {
			return p.Do("ToUpper", []byte("x"), JobNormal, func(*Response) {})
		}},
		{"DoBg", func() (string, string, error) {
			return p.DoBg("ToUpper", []byte("x"), JobNormal)
		}},
	} {
		type result struct {
			handle string
			err    error
		}
		done := make(chan result, 1)
		go func() {
			_, handle, err := tc.call()
			done <- result{handle, err}
		}()

		select {
		case r := <-done:
			if r.err != nil {
				t.Errorf("Pool.%s: %v", tc.name, r.err)
			}
			if r.handle != "H:localhost:1" {
				t.Errorf("Pool.%s handle = %q, want %q", tc.name, r.handle, "H:localhost:1")
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("Pool.%s did not return: the client mutex is taken twice", tc.name)
		}
	}
	p.Close()
}

// TestPoolStatusAndEchoDoNotDeadlock is the same guard for Pool.Status and
// Pool.Echo.
//
// Those two took the client's mutex deliberately: (*Client).Status and
// (*Client).Echo wrote to the shared bufio.Writer without holding it, so the
// pool's lock was the only thing serialising them. Now that the client locks
// itself, that outer lock is the Pool.Do deadlock again.
//
// Each call runs in its own goroutine so a regression fails this test instead
// of blocking the suite. Close is safe either way: it takes connMu, not the
// mutex a stuck caller would hold.
func TestPoolStatusAndEchoDoNotDeadlock(t *testing.T) {
	s := newFakeServer(t)
	defer s.close()
	s.handle = jobCreatedServer

	p := NewPool()
	if err := p.Add(Network, s.addr(), 1); err != nil {
		t.Fatalf("Pool.Add: %v", err)
	}
	defer p.Close()

	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"Status", func() error {
			st, err := p.Status(s.addr(), "H:localhost:1")
			if err == nil && st.Handle != "H:localhost:1" {
				t.Errorf("Pool.Status handle = %q, want %q", st.Handle, "H:localhost:1")
			}
			return err
		}},
		{"Echo", func() error {
			echo, err := p.Echo(s.addr(), []byte("ping"))
			if err == nil && string(echo) != "ping" {
				t.Errorf("Pool.Echo = %q, want %q", echo, "ping")
			}
			return err
		}},
	} {
		done := make(chan error, 1)
		go func() { done <- tc.call() }()

		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Pool.%s: %v", tc.name, err)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("Pool.%s did not return: the client mutex is taken twice", tc.name)
		}
	}
}

// TestPoolConcurrentStatusAndEcho is the other half of dropping that lock. It
// did two jobs: double-locking the client (above), and serialising concurrent
// pool callers onto one connection. Dropping it is safe only because the client
// now takes its own mutex for the whole round trip.
//
// So: several goroutines through Pool.Status and Pool.Echo at once on one
// address, each asserting its own answer comes back intact. Interleaved writes
// would corrupt the framing and the bodies would come back wrong.
func TestPoolConcurrentStatusAndEcho(t *testing.T) {
	s := newFakeServer(t)
	defer s.close()
	s.handle = jobCreatedServer

	p := NewPool()
	if err := p.Add(Network, s.addr(), 1); err != nil {
		t.Fatalf("Pool.Add: %v", err)
	}
	defer p.Close()

	const n = 20
	errc := make(chan error, 4*n)
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		wg.Add(4)
		for w := 0; w < 2; w++ {
			go func() {
				defer wg.Done()
				for i := 0; i < n; i++ {
					st, err := p.Status(s.addr(), "H:localhost:1")
					if err != nil {
						errc <- fmt.Errorf("Pool.Status: %w", err)
						return
					}
					if st.Handle != "H:localhost:1" {
						errc <- fmt.Errorf("Pool.Status handle = %q", st.Handle)
						return
					}
				}
			}()
			go func() {
				defer wg.Done()
				for i := 0; i < n; i++ {
					echo, err := p.Echo(s.addr(), []byte("ping"))
					if err != nil {
						errc <- fmt.Errorf("Pool.Echo: %w", err)
						return
					}
					if string(echo) != "ping" {
						errc <- fmt.Errorf("Pool.Echo = %q, want %q", echo, "ping")
					}
				}
			}()
		}
		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent Pool.Status/Pool.Echo wedged the connection")
	}
	close(errc)
	for err := range errc {
		t.Error(err)
	}
}
