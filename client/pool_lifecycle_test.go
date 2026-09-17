package client

import (
	"sync/atomic"
	"testing"
	"time"
)

// Regression tests for two Pool defects: Remove dropping a client without
// closing it (leaking a connection and two goroutines), and Pool.ErrorHandler
// being an exported field nothing ever read.

// TestPoolRemoveClosesTheClient guards the leak: Remove used to be a bare
// delete(pool.Clients, addr), leaving the dropped *Client's socket, readLoop
// and processLoop with nothing to close them.
func TestPoolRemoveClosesTheClient(t *testing.T) {
	s := newFakeServer(t)
	defer s.close()
	s.handle = jobCreatedServer

	p := NewPool()
	if err := p.Add(Network, s.addr(), 1); err != nil {
		t.Fatalf("Pool.Add: %v", err)
	}
	pc := p.Clients[s.addr()] // keep a reference: Remove drops the map entry

	p.Remove(s.addr())

	if _, ok := p.Clients[s.addr()]; ok {
		t.Fatal("Remove did not delete the pool entry")
	}
	if !pc.isClosed() {
		t.Error("Remove did not close the removed client")
	}
	// A closed client refuses further I/O rather than reaching the network.
	if _, err := pc.Echo([]byte("ping")); err != ErrLostConn {
		t.Errorf("Echo on a removed client = %v, want ErrLostConn", err)
	}
}

// TestPoolSetErrorHandlerInstallsOnExistingAndFutureClients guards the fix for
// the unused Pool.ErrorHandler field: SetErrorHandler must reach a client
// already in the pool and be remembered for one Add installs later, and the
// installed handler must actually fire.
func TestPoolSetErrorHandlerInstallsOnExistingAndFutureClients(t *testing.T) {
	s1 := newFakeServer(t)
	defer s1.close()
	s1.handle = jobCreatedServer

	p := NewPool()
	if err := p.Add(Network, s1.addr(), 1); err != nil {
		t.Fatalf("Pool.Add: %v", err)
	}
	defer p.Close()

	var calls int32
	p.SetErrorHandler(func(error) { atomic.AddInt32(&calls, 1) })

	if h := p.Clients[s1.addr()].errorHandler.Load(); h == nil {
		t.Fatal("SetErrorHandler did not reach the client already in the pool")
	}

	s2 := newFakeServer(t)
	defer s2.close()
	s2.handle = jobCreatedServer
	if err := p.Add(Network, s2.addr(), 1); err != nil {
		t.Fatalf("Pool.Add (second server): %v", err)
	}
	if h := p.Clients[s2.addr()].errorHandler.Load(); h == nil {
		t.Fatal("Add did not install the pool's error handler on a client added later")
	}

	// And it actually runs: drop the first server so its client's redial
	// fails, which is a real connection error reaching client.err().
	s1.close()
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&calls) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("installed handler never fired for a real connection error")
		}
		time.Sleep(time.Millisecond)
	}
}
