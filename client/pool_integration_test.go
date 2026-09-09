package client

import (
	"os"
	"strings"
	"testing"
	"time"
)

// Multi-server Pool tests. `make integration` starts GEARMAND_COUNT job servers
// (default 3) and passes the list in GEARMAND_POOL_ADDRS.
//
// An env var, not a test flag: a flag must be defined in the TestMain of every
// package `make integration` passes it to, so worker/worker_test.go would have
// to define it too or the run dies with "flag provided but not defined".
//
// Nothing here asserts anything about Rate. SelectWithRate returns on the first
// item it iterates whenever that rate is positive, so later rates are never
// consulted (docs/todo.md §6a). The spread is Go's map-iteration order, and it
// is not uniform: a one-bucket map starts at a random slot in 0..7 and wraps, so
// one server of three is picked ~6/8 of the time and the others ~1/8 each
// (measured 177/12/11 over 200 submits).

const (
	poolAddrsEnv = "GEARMAND_POOL_ADDRS"

	// No worker in this repo registers it. ./client and ./worker run in
	// parallel against the same 127.0.0.1:4730, so a job submitted as "ToUpper"
	// can be grabbed and completed mid-test, flipping the Status assertions.
	poolProbeFunc = "PoolSelectionProbe"

	// P(a 1/8 server is never picked) = (7/8)^200 ≈ 3e-12.
	poolSelectionTrials = 200

	// Bounds the whole submit loop. With ResponseTimeout at 1s, 200 calls to a
	// port held by something that is not a gearmand would take 200s and blow the
	// 120s package timeout, discarding every other result in the binary.
	poolSelectionBudget = 45 * time.Second
)

// poolIntegrationAddrs returns the job servers to pool, or skips. Fewer than
// two is a skip and not a failure: `make integration` only starts the servers
// it has to, so a machine with no docker still runs the single-server suite.
func poolIntegrationAddrs(t *testing.T) []string {
	t.Helper()
	if !runIntegrationTests {
		t.Skip("To run this test, use: go test ./client -integration")
	}
	raw := os.Getenv(poolAddrsEnv)
	var addrs []string
	for _, a := range strings.Split(raw, ",") {
		if a = strings.TrimSpace(a); a != "" {
			addrs = append(addrs, a)
		}
	}
	if len(addrs) < 2 {
		t.Skipf("%s=%q gives %d job server(s); these tests need at least 2. "+
			"`make integration` starts GEARMAND_COUNT (default 3) of them in docker.",
			poolAddrsEnv, raw, len(addrs))
	}
	t.Logf("job servers: %s", strings.Join(addrs, " "))
	return addrs
}

// newIntegrationPool returns a local pool -- never the package-level one in
// pool_test.go, which later tests depend on the state of.
func newIntegrationPool(t *testing.T, addrs []string) *Pool {
	t.Helper()
	p := NewPool()
	for _, addr := range addrs {
		// Add dials, so a failure here is a dead job server, not a pool defect.
		if err := p.Add(Network, addr, 1); err != nil {
			p.Close()
			t.Fatalf("Pool.Add(%s): %v", addr, err)
		}
	}
	if len(p.Clients) != len(addrs) {
		p.Close()
		t.Fatalf("pool has %d clients, want %d", len(p.Clients), len(addrs))
	}
	return p
}

// TestPoolMultiServerReachable is the precondition for everything below. Echo
// names the server, so it does not go through the SelectionHandler.
func TestPoolMultiServerReachable(t *testing.T) {
	addrs := poolIntegrationAddrs(t)
	p := newIntegrationPool(t, addrs)
	defer p.Close() // returns map[string]error, always non-nil: never `if err != nil`

	for _, addr := range addrs {
		var echo []byte
		err := mustReturnWithin(t, 5*time.Second, "Pool.Echo("+addr+")", func() error {
			var e error
			echo, e = p.Echo(addr, []byte(TestStr))
			return e
		})
		if err != nil {
			t.Errorf("Pool.Echo(%s): %v", addr, err)
			continue
		}
		if string(echo) != TestStr {
			t.Errorf("Pool.Echo(%s) = %q, want %q", addr, echo, TestStr)
		}
	}
}

// TestPoolMultiServerSelectionCoversEveryServer would catch a pool that dialed
// N servers and then sent everything to one of them.
func TestPoolMultiServerSelectionCoversEveryServer(t *testing.T) {
	addrs := poolIntegrationAddrs(t)
	p := newIntegrationPool(t, addrs)
	defer p.Close()

	counts := make(map[string]int, len(addrs))
	// One deadline for the loop, not per call. Nothing inside may call t.Fatal:
	// it runs on another goroutine.
	err := mustReturnWithin(t, poolSelectionBudget, "the Pool.DoBg submit loop", func() error {
		for i := 0; i < poolSelectionTrials; i++ {
			addr, handle, err := p.DoBg(poolProbeFunc, []byte("x"), JobNormal)
			if err != nil {
				return err
			}
			if handle == "" {
				t.Errorf("Pool.DoBg via %s returned an empty handle", addr)
			}
			counts[addr]++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Pool.DoBg: %v", err)
	}

	for _, addr := range addrs {
		if counts[addr] == 0 {
			t.Errorf("%s was never selected in %d submits (counts: %v)",
				addr, poolSelectionTrials, counts)
		}
	}
	for addr, n := range counts {
		if _, ok := p.Clients[addr]; !ok {
			t.Errorf("Pool.DoBg reported %s (%d times), which is not in the pool", addr, n)
		}
	}
	t.Logf("selection counts over %d submits: %v", poolSelectionTrials, counts)
}

// TestPoolMultiServerSelectionReportsTheRealServer checks the reported address
// against the job servers rather than the pool's own bookkeeping: the server
// that minted the handle is the only one that knows it.
//
// Pinned to the last address, never the first: a worker from ./worker could
// grab a job on 127.0.0.1:4730 between the submit and the Status.
func TestPoolMultiServerSelectionReportsTheRealServer(t *testing.T) {
	addrs := poolIntegrationAddrs(t)
	p := newIntegrationPool(t, addrs)
	defer p.Close()

	want := addrs[len(addrs)-1]
	p.SelectionHandler = func(map[string]*PoolClient, string) string { return want }

	var addr, handle string
	err := mustReturnWithin(t, 5*time.Second, "Pool.DoBg with a pinned handler", func() error {
		var e error
		addr, handle, e = p.DoBg(poolProbeFunc, []byte("x"), JobNormal)
		return e
	})
	if err != nil {
		t.Fatalf("Pool.DoBg: %v", err)
	}
	if addr != want {
		t.Fatalf("Pool.DoBg selected %q, want the pinned %q", addr, want)
	}

	for _, srv := range addrs {
		var status *Status
		err := mustReturnWithin(t, 5*time.Second, "Pool.Status("+srv+")", func() error {
			var e error
			status, e = p.Status(srv, handle)
			return e
		})
		if err != nil {
			t.Errorf("Pool.Status(%s, %s): %v", srv, handle, err)
			continue
		}
		switch {
		case srv == want && !status.Known:
			t.Errorf("%s does not know handle %s, but Pool.DoBg said it holds the job",
				srv, handle)
		case srv != want && status.Known:
			t.Errorf("%s knows handle %s, which Pool.DoBg said went to %s",
				srv, handle, want)
		}
	}
}

// TestPoolMultiServerRemovedServerIsNotSelected: after Remove, selection falls
// back to what is left and the removed address stops being addressable.
func TestPoolMultiServerRemovedServerIsNotSelected(t *testing.T) {
	addrs := poolIntegrationAddrs(t)
	p := newIntegrationPool(t, addrs)
	defer p.Close()

	gone := addrs[len(addrs)-1] // not 4730, see the note above
	p.Remove(gone)

	seen := make(map[string]int, len(addrs))
	err := mustReturnWithin(t, poolSelectionBudget, "the Pool.DoBg submit loop after Remove", func() error {
		for i := 0; i < poolSelectionTrials; i++ {
			addr, _, err := p.DoBg(poolProbeFunc, []byte("x"), JobNormal)
			if err != nil {
				return err
			}
			seen[addr]++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Pool.DoBg after Remove: %v", err)
	}
	if n := seen[gone]; n != 0 {
		t.Errorf("removed server %s was still selected %d times", gone, n)
	}
	for _, addr := range addrs[:len(addrs)-1] {
		if seen[addr] == 0 {
			t.Errorf("%s was never selected in %d submits after removing %s (counts: %v)",
				addr, poolSelectionTrials, gone, seen)
		}
	}

	err = mustReturnWithin(t, 5*time.Second, "Pool.Echo on the removed server", func() error {
		_, e := p.Echo(gone, []byte(TestStr))
		return e
	})
	if err != ErrNotFound {
		t.Errorf("Pool.Echo(%s) after Remove = %v, want ErrNotFound", gone, err)
	}
}
