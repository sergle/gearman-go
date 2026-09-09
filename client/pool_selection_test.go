package client

import (
	"context"
	"os"
	"os/exec"
	"strings"
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
//
// TestPoolConcurrentMapAccessDoesNotThrow at the bottom is for 6b, the locking
// pass on the same file. It is the odd one out: mustReturnWithin could not have
// contained it, so it drives the pool in a child process.

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

// --- 6b: Pool read Clients, last and Rate with no lock ----------------------
//
// Status, Echo, Close and selectServer read pool.Clients unlocked while Add and
// Remove wrote it; selectServer wrote pool.last, and Add wrote
// PoolClient.Rate, against unlocked readers in SelectWithRate.
//
// The map half was not a -race finding. A map read concurrent with a map write
// trips the runtime's hashWriting check and *throws* `fatal error: concurrent
// map read and map write` -- no -race needed, no recover possible, whole
// process down. Hence the child process: an in-process reproducer would kill
// the test binary, and this test is in the default suite.
//
// The child inherits the parent's build, so one test carries both oracles: the
// throw (exit 2) with no -race, the last/Rate races (exit 66, DATA RACE) under
// it. Which means `go test ./client` alone does *not* guard the last/Rate
// half; `go test -race ./client` does.

const poolRaceChildEnv = "GEARMAN_POOL_RACE_CHILD"

// poolRaceHammerFor was 3s while the defect was open. As a regression guard it
// needs margin over the ~10ms the unfixed code took to throw, not volume.
const poolRaceHammerFor = time.Second

func TestPoolConcurrentMapAccessDoesNotThrow(t *testing.T) {
	if os.Getenv(poolRaceChildEnv) == "1" {
		poolRaceHammer(t)
		return // not os.Exit(0): a throw must be the only non-zero exit
	}

	// -test.timeout only arms once the child's testing framework is running,
	// so the parent's wait is bounded too: without this a child that wedged
	// early would block until `go test`'s own timeout killed the whole binary,
	// which is the failure the fork exists to prevent.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, os.Args[0],
		"-test.run=^TestPoolConcurrentMapAccessDoesNotThrow$",
		"-test.timeout=60s")
	cmd.Env = append(os.Environ(), poolRaceChildEnv+"=1")
	out, err := cmd.CombinedOutput()

	s := string(out)
	switch {
	case strings.Contains(s, "fatal error") || strings.Contains(s, "DATA RACE"):
		t.Errorf("concurrent Pool map/last/Rate access reported by the runtime:\n%s", s)
	case ctx.Err() != nil:
		t.Errorf("pool race child did not finish within 30s:\n%s", s)
	case err != nil:
		t.Errorf("pool race child exited with %v:\n%s", err, s)
	}
}

// poolRaceHammer asserts nothing: the runtime is the oracle and the child's
// exit status is the result.
//
// The map must be non-empty -- mapaccess and mapdelete short-circuit on
// count == 0 *before* the hashWriting check, so an empty pool cannot throw
// however hard it is driven. Hence two servers that are never removed.
func poolRaceHammer(t *testing.T) {
	permanent := newFakeServer(t)
	defer permanent.close()
	permanent.handle = jobCreatedServer

	second := newFakeServer(t)
	defer second.close()
	second.handle = jobCreatedServer

	p := NewPool()
	for _, s := range []*fakeServer{permanent, second} {
		if err := p.Add(Network, s.addr(), 1); err != nil {
			t.Fatalf("Pool.Add: %v", err)
		}
	}

	const absent = "127.0.0.1:1" // never in the pool: every lookup misses
	deadline := time.Now().Add(poolRaceHammerFor)
	done := make(chan struct{})
	go func() {
		time.Sleep(time.Until(deadline))
		close(done)
	}()

	loop := func(fn func()) {
		go func() {
			for {
				select {
				case <-done:
					return
				default:
					fn()
				}
			}
		}()
	}

	// Readers. The address is never in the pool, so each is a map lookup that
	// misses and returns ErrNotFound -- no round trip.
	for i := 0; i < 8; i++ {
		loop(func() { p.Status(absent, "H:absent:1") })
		loop(func() { p.Echo(absent, nil) })
	}

	// Writers. mapdelete sets hashWriting after hashing whether or not the key
	// is present, so an absent key is a full-rate map write with no dial.
	for i := 0; i < 8; i++ {
		loop(func() { p.Remove(absent) })
	}

	// selectServer callers: a round trip each, but the only paths reaching the
	// pool.last write and the handler's reads of last and Rate. Volume is not
	// needed -- the detector reports one.
	loop(func() { p.Echo("", []byte("ping")) })
	loop(func() { p.Do("ToUpper", []byte("x"), JobNormal, func(*Response) {}) })
	loop(func() { p.DoBg("ToUpper", []byte("x"), JobNormal) })

	// Add on an address already present takes the other branch and writes
	// PoolClient.Rate. The churn loop below always takes the insert branch, so
	// this is the only thing that reaches that race.
	loop(func() { p.Add(Network, permanent.addr(), 2) })

	// Close is the only reader that ranges the whole map. Once, partway in, so
	// the range overlaps the writers; the clients it closes are not needed
	// again.
	go func() {
		select {
		case <-done:
		case <-time.After(poolRaceHammerFor / 2):
			p.Close()
		}
	}()

	// Real Add/Remove cycles, for the insert side of the map write. Bounded
	// because Remove is a bare delete and does not Close the client, so each
	// cycle leaks a socket and two goroutines. They die with the child.
	go func() {
		churn := newFakeServer(t)
		defer churn.close()
		churn.handle = jobCreatedServer
		for i := 0; i < 50; i++ {
			select {
			case <-done:
				return
			default:
			}
			p.Add(Network, churn.addr(), 1)
			p.Remove(churn.addr())
		}
	}()

	<-done
}
