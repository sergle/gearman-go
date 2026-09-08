package client

import (
	"testing"
	"time"
)

// Regression tests for defect 6a: Pool.selectServer spun forever on an empty
// pool, and both bundled SelectionHandlers called rand.Intn with a
// non-positive argument on preconditions the exported API can produce.
//
// TestPoolOnEmptyPoolReturnsNotFound came from knownbugs_test.go unchanged
// apart from dropping its gate; the rest are new. They stay behind
// mustReturnWithin because a regression here does not fail, it spins: the
// goroutine it leaks is runnable, not blocked, so it burns a core for the rest
// of the binary's life.

// TestPoolOnEmptyPoolReturnsNotFound covers the call the defect was found
// through. Reached in practice by `make integration` with no gearmand: every
// Pool.Add fails, so the pool is empty by the time anything is submitted.
func TestPoolOnEmptyPoolReturnsNotFound(t *testing.T) {
	p := NewPool()

	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"Echo", func() error {
			_, err := p.Echo("", []byte("ping"))
			return err
		}},
		{"Do", func() error {
			_, _, err := p.Do("ToUpper", []byte("x"), JobNormal, func(*Response) {})
			return err
		}},
		{"DoBg", func() error {
			_, _, err := p.DoBg("ToUpper", []byte("x"), JobNormal)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := mustReturnWithin(t, 2*time.Second, "Pool."+tc.name+" on an empty pool", tc.call)
			if err != ErrNotFound {
				t.Errorf("Pool.%s on an empty pool = %v, want ErrNotFound", tc.name, err)
			}
		})
	}
}

// TestPoolWithSelectRandomOnEmptyPoolReturnsNotFound is the same case with the
// other bundled handler, which did not spin but panicked: rand.Intn(0).
func TestPoolWithSelectRandomOnEmptyPoolReturnsNotFound(t *testing.T) {
	p := NewPool()
	p.SelectionHandler = SelectRandom

	err := mustReturnWithin(t, 2*time.Second, "Pool.Echo with SelectRandom", func() error {
		_, err := p.Echo("", []byte("ping"))
		return err
	})
	if err != ErrNotFound {
		t.Errorf("Pool.Echo on an empty pool with SelectRandom = %v, want ErrNotFound", err)
	}
}

// TestSelectRandomOnEmptyPool calls the handler directly, since the pool no
// longer hands it an empty map: a caller using it as a SelectionHandler
// elsewhere must not panic either.
func TestSelectRandomOnEmptyPool(t *testing.T) {
	if addr := SelectRandom(map[string]*PoolClient{}, "last"); addr != "last" {
		t.Errorf("SelectRandom on an empty pool = %q, want %q", addr, "last")
	}
}

// TestSelectWithRateAllZeroRates covers Add(net, addr, 0), which is reachable
// through the exported API. total stays 0, so the old rand.Intn(total) panicked
// before any rate was compared.
func TestSelectWithRateAllZeroRates(t *testing.T) {
	p := map[string]*PoolClient{
		"a": {Rate: 0},
		"b": {Rate: 0},
	}
	if addr := SelectWithRate(p, "last"); addr != "last" {
		t.Errorf("SelectWithRate with all-zero rates = %q, want %q", addr, "last")
	}
}

// TestPoolSelectionHandlerReturningUnknownAddr covers the other way selection
// used to spin: a handler that never names a server actually in the pool. One
// attempt, then ErrNotFound -- retrying cannot change a handler's answer on an
// unchanged pool.
func TestPoolSelectionHandlerReturningUnknownAddr(t *testing.T) {
	s := newFakeServer(t)
	defer s.close()
	s.handle = jobCreatedServer

	p := NewPool()
	if err := p.Add(Network, s.addr(), 1); err != nil {
		t.Fatalf("Pool.Add: %v", err)
	}
	defer p.Close()

	calls := 0
	p.SelectionHandler = func(map[string]*PoolClient, string) string {
		calls++
		return "127.0.0.1:1"
	}

	err := mustReturnWithin(t, 2*time.Second, "Pool.Echo with a bad SelectionHandler", func() error {
		_, err := p.Echo("", []byte("ping"))
		return err
	})
	if err != ErrNotFound {
		t.Errorf("Pool.Echo with a handler naming an unknown server = %v, want ErrNotFound", err)
	}
	if calls != 1 {
		t.Errorf("SelectionHandler called %d times, want 1", calls)
	}
}

// TestPoolSelectsAServerWhenThereIsOne is the positive half: the empty-pool
// guard must not swallow the ordinary path, and the pool must go back to
// ErrNotFound once its only server is removed.
func TestPoolSelectsAServerWhenThereIsOne(t *testing.T) {
	s := newFakeServer(t)
	defer s.close()
	s.handle = jobCreatedServer

	p := NewPool()
	if err := p.Add(Network, s.addr(), 1); err != nil {
		t.Fatalf("Pool.Add: %v", err)
	}
	defer p.Close()

	var addr string
	err := mustReturnWithin(t, 2*time.Second, "Pool.DoBg", func() error {
		a, _, err := p.DoBg("ToUpper", []byte("x"), JobNormal)
		addr = a
		return err
	})
	if err != nil {
		t.Fatalf("Pool.DoBg on a pool with one server: %v", err)
	}
	if addr != s.addr() {
		t.Errorf("Pool.DoBg selected %q, want %q", addr, s.addr())
	}

	p.Remove(s.addr())

	err = mustReturnWithin(t, 2*time.Second, "Pool.DoBg after Remove", func() error {
		_, _, err := p.DoBg("ToUpper", []byte("x"), JobNormal)
		return err
	})
	if err != ErrNotFound {
		t.Errorf("Pool.DoBg after removing the only server = %v, want ErrNotFound", err)
	}
}
